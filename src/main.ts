// perry-hub: Public-facing build server
// Receives build uploads from CLI, dispatches to workers via WebSocket

import Fastify from 'fastify';
import * as fs from 'fs';
import * as crypto from 'crypto';
import { WebSocketServer, sendToClient, closeClient } from 'ws';
import mysql from 'mysql2/promise';

// --- Multipart parser (pure TypeScript) ---

interface MultipartPart {
  name: string;
  filename?: string;
  content_type?: string;
  data: string;
  size: number;
}

function parseMultipart(body: string, contentType: string): MultipartPart[] {
  // Extract boundary from Content-Type header
  const boundaryMatch = contentType.match(/boundary=([^\s;]+)/);
  if (!boundaryMatch) return [];
  const boundary = boundaryMatch[1].replace(/^"(.*)"$/, '$1');

  const delimiter = '--' + boundary;
  const endDelimiter = delimiter + '--';

  const parts: MultipartPart[] = [];
  const segments = body.split(delimiter);

  for (let i = 1; i < segments.length; i++) {
    const segment = segments[i];
    if (segment.startsWith('--')) break; // end delimiter

    // Split headers from body at first \r\n\r\n
    const headerEnd = segment.indexOf('\r\n\r\n');
    if (headerEnd === -1) continue;

    const headerSection = segment.substring(0, headerEnd);
    let data = segment.substring(headerEnd + 4);

    // Strip trailing \r\n
    if (data.endsWith('\r\n')) {
      data = data.substring(0, data.length - 2);
    }

    // Parse headers
    let name: string | null = null;
    let filename: string | undefined;
    let partContentType: string | undefined;

    const lines = headerSection.split('\r\n');
    for (let li = 0; li < lines.length; li++) {
      const line = lines[li];
      const trimmed = line.trim();
      if (!trimmed) continue;

      const lower = trimmed.toLowerCase();
      if (lower.startsWith('content-disposition:')) {
        const dispValue = trimmed.substring('content-disposition:'.length).trim();
        const nameMatch = dispValue.match(/name="([^"]+)"/);
        if (nameMatch) name = nameMatch[1];
        const fileMatch = dispValue.match(/filename="([^"]+)"/);
        if (fileMatch) filename = fileMatch[1];
      } else if (lower.startsWith('content-type:')) {
        partContentType = trimmed.substring('content-type:'.length).trim();
      }
    }

    if (name) {
      parts.push({
        name,
        filename,
        content_type: partContentType,
        data,
        size: data.length,
      });
    }
  }

  return parts;
}

// --- Configuration ---

const HTTP_PORT = parseInt(process.env.PERRY_HUB_HTTP_PORT || '3456', 10);
const WS_PORT = parseInt(process.env.PERRY_HUB_WS_PORT || '3457', 10);
const ARTIFACT_TTL_SECS = parseInt(process.env.PERRY_HUB_ARTIFACT_TTL_SECS || '600', 10);

function isSelfHosted(): boolean {
  const v = process.env.PERRY_HUB_SELF_HOSTED || '';
  return v === 'true' || v === '1';
}
const TARBALL_DIR = '/tmp/perry-hub-tarballs';
const ARTIFACT_DIR = '/tmp/perry-artifacts';

// --- Database pool ---
// createPool must be called inside a function (not inline at module level)
// for perry to properly pass the object literal to the native mysql2 FFI.
// The returned pool must be assigned to a named const so perry's HIR
// transform can track it as a mysql2 Pool for method dispatch.

function createDbPool(): Pool {
  return mysql.createPool({
    host: 'localhost',
    port: 3306,
    user: 'perry',
    password: 'perry',
    database: 'perry_hub',
  });
}

const db = createDbPool();

// --- Types ---

interface License {
  key: string;
  tier: 'free' | 'pro';
  github_username: string;
  platforms: string[];
}

interface RateLimitState {
  active_builds: number;
  recent_builds: number[]; // timestamps in ms
}

interface Job {
  id: string;
  license_key: string;
  tier: 'free' | 'pro';
  manifest: any;
  credentials: any;
  status: 'queued' | 'running' | 'completed' | 'failed' | 'cancelled';
  created_at: number;
  priority: number;
}

interface WorkerInfo {
  clientHandle: any; // WS client handle from server-level event
  capStr: string; // comma-separated capabilities (avoids perry array bugs)
  name: string;
  busy: boolean;
  current_job_id: string | null;
}

interface ArtifactEntry {
  path: string;
  name: string;
  size: number;
  sha256: string;
  expires: number; // timestamp ms
  downloaded: boolean;
}

// --- In-memory stores ---

const licenses = new Map<string, License>();
const rateLimits = new Map<string, RateLimitState>();
const jobs = new Map<string, Job>();
const jobQueue: string[] = []; // job IDs in priority order
const workerList: WorkerInfo[] = [];
const counters = { workers: 0, queueLen: 0 };
const artifacts = new Map<string, ArtifactEntry>();
// Use Maps for worker property lookup (perry workaround: reading properties from
// objects stored in module-level arrays is unreliable)
const workerCapMap = new Map<string, string>(); // "w0" -> ",macos,ios,android,"
const workerNameMap = new Map<string, string>(); // "w0" -> "macbook-intel"
const workerHandleMap = new Map<string, number>(); // "w0" -> clientHandle
const workerBusyMap = new Map<string, boolean>(); // "w0" -> false
const workerJobMap = new Map<string, string>(); // "w0" -> current_job_id or ""
// Precomputed JSON string of supported targets (rebuilt on worker connect/disconnect)
const cached = { targetsJson: '[]' };

// Map job_id -> CLI client handle (stored as string to avoid NaN issues with arrays)
const jobCliHandles = new Map<string, string>(); // job_id -> "handle1,handle2,..."

// --- Client state tracking (server-level events use raw handles) ---
// Use Maps keyed by string(handle) to avoid perry array/object push bugs
const clientIdentified = new Map<string, boolean>();
const clientRole = new Map<string, string>();       // 'worker' | 'cli'
const clientWorkerIdx = new Map<string, number>();   // index into workerList

function handleKey(handle: any): string {
  return 'h' + String(handle);
}

function registerClient(handle: any): void {
  const key = handleKey(handle);
  clientIdentified.set(key, false);
}

function isClientRegistered(handle: any): boolean {
  return clientIdentified.has(handleKey(handle));
}

function isClientIdentified(handle: any): boolean {
  return clientIdentified.get(handleKey(handle)) || false;
}

function setClientIdentified(handle: any): void {
  clientIdentified.set(handleKey(handle), true);
}

function setClientRole(handle: any, role: string): void {
  clientRole.set(handleKey(handle), role);
}

function getClientRole(handle: any): string | null {
  return clientRole.get(handleKey(handle)) || null;
}

function setClientWorkerIdx(handle: any, idx: number): void {
  clientWorkerIdx.set(handleKey(handle), idx);
}

function getClientWorkerIdx(handle: any): number {
  const v = clientWorkerIdx.get(handleKey(handle));
  if (v === undefined) return -1;
  return v;
}

function removeClient(handle: any): void {
  const key = handleKey(handle);
  clientIdentified.delete(key);
  clientRole.delete(key);
  clientWorkerIdx.delete(key);
}

// --- DB init and helpers ---

async function initDb(): Promise<void> {
  try {
    await db.query(`CREATE TABLE IF NOT EXISTS licenses (
      licenseKey VARCHAR(64) PRIMARY KEY,
      tier VARCHAR(8) NOT NULL DEFAULT 'free',
      githubUsername VARCHAR(255) NOT NULL DEFAULT '',
      platforms TEXT NOT NULL,
      createdAt BIGINT NOT NULL,
      notes TEXT
    )`);

    await db.query(`CREATE TABLE IF NOT EXISTS builds (
      id CHAR(36) PRIMARY KEY,
      licenseKey VARCHAR(64) NOT NULL,
      tier VARCHAR(8) NOT NULL,
      targets TEXT NOT NULL,
      status VARCHAR(20) NOT NULL DEFAULT 'queued',
      queuedAt BIGINT NOT NULL,
      startedAt BIGINT,
      completedAt BIGINT,
      durationSecs INT,
      workerName VARCHAR(255),
      errorMessage TEXT,
      INDEX idxLicenseKey (licenseKey)
    )`);

    const result = await db.query('SELECT licenseKey, tier, githubUsername, platforms FROM licenses');
    const rows: any = result[0];
    for (let i = 0; i < rows.length; i++) {
      const row = rows[i];
      const license: License = {
        key: row.licenseKey,
        tier: row.tier,
        github_username: row.githubUsername,
        platforms: JSON.parse(row.platforms),
      };
      licenses.set(license.key, license);
    }
    console.log('DB ready, loaded ' + String(licenses.size) + ' licenses');
  } catch (e: any) {
    console.error('DB init error:', e.message || e);
  }
}

async function dbSaveLicense(license: License): Promise<void> {
  try {
    await db.execute(
      'INSERT INTO licenses (licenseKey, tier, githubUsername, platforms, createdAt) VALUES (?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE tier=VALUES(tier), platforms=VALUES(platforms)',
      [license.key, license.tier, license.github_username, JSON.stringify(license.platforms), Date.now()]
    );
  } catch (e: any) {
    console.error('dbSaveLicense error:', e.message || e);
  }
}

function dbInsertBuild(job: Job): void {
  const targets = job.manifest && job.manifest.targets ? JSON.stringify(job.manifest.targets) : '[]';
  db.execute(
    'INSERT INTO builds (id, licenseKey, tier, targets, status, queuedAt) VALUES (?, ?, ?, ?, ?, ?)',
    [job.id, job.license_key, job.tier, targets, 'queued', job.created_at]
  );
}

function dbBuildStarted(jobId: string, workerName: string): void {
  db.execute(
    'UPDATE builds SET status=?, startedAt=?, workerName=? WHERE id=?',
    ['running', Date.now(), workerName, jobId]
  );
}

function dbBuildFinished(jobId: string, success: boolean, durationSecs: number, errorMsg: string): void {
  const status = success ? 'completed' : 'failed';
  db.execute(
    'UPDATE builds SET status=?, completedAt=?, durationSecs=?, errorMessage=? WHERE id=?',
    [status, Date.now(), durationSecs, errorMsg, jobId]
  );
}

// --- License store ---

function generateLicenseKey(tier: 'free' | 'pro'): string {
  const prefix = tier === 'pro' ? 'PRO' : 'FREE';
  // Use randomUUID and extract hex segments (crypto.randomBytes toString('hex') not reliable)
  const uuid = crypto.randomUUID().replace(/-/g, '');
  const seg1 = uuid.substring(0, 4).toUpperCase();
  const seg2 = uuid.substring(4, 8).toUpperCase();
  const seg3 = uuid.substring(8, 12).toUpperCase();
  return prefix + '-' + seg1 + '-' + seg2 + '-' + seg3;
}

function registerLicense(username: string, tier: 'free' | 'pro'): License {
  const key = generateLicenseKey(tier);
  const platforms = tier === 'pro'
    ? ['macos', 'ios', 'android', 'windows', 'linux']
    : ['macos'];
  const license: License = { key, tier, github_username: username, platforms };
  licenses.set(key, license);
  return license;
}

function verifyLicense(key: string): License | null {
  return licenses.get(key) || null;
}

// --- Rate limiter ---

function checkRateLimit(licenseKey: string, tier: 'free' | 'pro', uploadSize: number): string | null {
  const maxConcurrent = tier === 'pro' ? 3 : 1;
  const maxPerHour = tier === 'pro' ? 30 : 5;
  const maxUpload = tier === 'pro' ? 200 * 1024 * 1024 : 50 * 1024 * 1024;

  if (uploadSize > maxUpload) {
    return `Upload size ${uploadSize} exceeds tier limit of ${maxUpload} bytes`;
  }

  let state = rateLimits.get(licenseKey);
  if (!state) {
    state = { active_builds: 0, recent_builds: [] };
    rateLimits.set(licenseKey, state);
  }

  if (state.active_builds >= maxConcurrent) {
    return `Concurrent build limit reached (${maxConcurrent})`;
  }

  const oneHourAgo = Date.now() - 3600_000;
  state.recent_builds = state.recent_builds.filter(t => t > oneHourAgo);

  if (state.recent_builds.length >= maxPerHour) {
    return `Hourly build limit reached (${maxPerHour} per hour)`;
  }

  state.active_builds++;
  state.recent_builds.push(Date.now());
  return null;
}

function releaseRateLimit(licenseKey: string): void {
  const state = rateLimits.get(licenseKey);
  if (state) {
    state.active_builds = Math.max(0, state.active_builds - 1);
  }
}

// --- Job queue ---

function enqueueJob(job: Job): number {
  jobs.set(job.id, job);

  // Insert in priority order (higher priority first, then FIFO)
  let insertIdx = counters.queueLen;
  for (let i = 0; i < counters.queueLen; i++) {
    const existing = jobs.get(jobQueue[i]);
    if (existing && existing.priority < job.priority) {
      insertIdx = i;
      break;
    }
  }
  jobQueue.splice(insertIdx, 0, job.id);
  counters.queueLen++;
  return insertIdx + 1; // 1-based position
}

function dequeueJob(): Job | null {
  while (counters.queueLen > 0) {
    const jobId = jobQueue.shift()!;
    counters.queueLen--;
    const job = jobs.get(jobId);
    if (job && job.status === 'queued') {
      return job;
    }
  }
  return null;
}

// --- Worker pool ---

function getWorkerCapStr(wi: number): string {
  return workerCapMap.get('w' + String(wi)) || ',';
}

function rebuildTargetsJson(): void {
  // Check each known target against all worker capStr entries
  // Avoid split/length comparisons entirely (broken in perry)
  const known = [',macos,', ',ios,', ',android,', ',linux,', ',windows,'];
  const labels = ['"macos"', '"ios"', '"android"', '"linux"', '"windows"'];
  let json = '[';
  let first = true;
  for (let ki = 0; ki < 5; ki++) {
    let found = false;
    for (let wi = 0; wi < counters.workers; wi++) {
      const cs = workerCapMap.get('w' + String(wi)) || ',';
      if (cs.indexOf(known[ki]) >= 0) {
        found = true;
        break;
      }
    }
    if (found) {
      if (!first) json = json + ',';
      json = json + labels[ki];
      first = false;
    }
  }
  json = json + ']';
  cached.targetsJson = json;
  console.log('rebuildTargetsJson: ' + json);
}

function workerIdxHasCapability(wi: number, cap: string): boolean {
  const cs = getWorkerCapStr(wi);
  const needle = ',' + cap + ',';
  const idx = cs.indexOf(needle);
  // Perry bug: < 0 comparison is unreliable with indexOf results.
  // Use >= 0 (positive check) which works correctly in rebuildTargetsJson.
  if (idx >= 0) return true;
  return false;
}

// Returns worker INDEX (not the worker object, which has corrupted properties in perry)
function getAvailableWorkerIdx(requiredCaps: string[], reqLen: number): number {
  console.log('getAvailableWorkerIdx: checking ' + String(counters.workers) + ' workers for ' + String(reqLen) + ' caps');
  for (let wi = 0; wi < counters.workers; wi++) {
    const wKey = 'w' + String(wi);
    const wName = workerNameMap.get(wKey) || '?';
    const wBusy = workerBusyMap.get(wKey) || false;
    console.log('getAvailableWorkerIdx: ' + wKey + ' name=' + wName + ' busy=' + String(wBusy));
    if (!wBusy) {
      let hasAll = true;
      for (let ri = 0; ri < reqLen; ri++) {
        const hasCap = workerIdxHasCapability(wi, requiredCaps[ri]);
        console.log('getAvailableWorkerIdx: ' + wKey + ' cap=' + requiredCaps[ri] + ' has=' + String(hasCap));
        if (!hasCap) {
          hasAll = false;
          break;
        }
      }
      if (hasAll) {
        console.log('getAvailableWorkerIdx: selected ' + wKey + ' name=' + wName);
        return wi;
      }
    }
  }
  return -1;
}

function getSupportedTargets(): string[] {
  // Collect unique capabilities from all workers using string dedup
  let allCaps = ',';
  for (let wi = 0; wi < counters.workers; wi++) {
    const cs = getWorkerCapStr(wi);
    const parts = cs.split(',');
    for (let pi = 0; pi < parts.length; pi++) {
      const p = parts[pi];
      if (p.length > 0 && allCaps.indexOf(',' + p + ',') < 0) {
        allCaps = allCaps + p + ',';
      }
    }
  }
  // Convert to array for JSON
  const result: string[] = [];
  let rLen = 0;
  const final = allCaps.split(',');
  for (let fi = 0; fi < final.length; fi++) {
    if (final[fi].length > 0) {
      result[rLen] = final[fi];
      rLen++;
    }
  }
  return result;
}

function dispatchJob(job: Job): boolean {
  // Determine required capabilities from manifest targets
  const requiredCaps: string[] = [];
  let reqCapLen = 0;
  console.log('dispatchJob: manifest=' + JSON.stringify(job.manifest));
  if (job.manifest.targets) {
    const manifestTargets = job.manifest.targets;
    for (let ti = 0; ti < manifestTargets.length; ti++) {
      requiredCaps[reqCapLen] = manifestTargets[ti].toLowerCase();
      reqCapLen++;
    }
  }
  if (reqCapLen === 0) {
    requiredCaps[0] = 'macos'; // default
    reqCapLen = 1;
  }
  console.log('dispatchJob: reqCapLen=' + String(reqCapLen) + ' caps=' + requiredCaps[0]);

  const wi = getAvailableWorkerIdx(requiredCaps, reqCapLen);
  // Perry bug: < 0 unreliable, use === -1
  if (wi === -1) {
    console.log('dispatchJob: no available worker');
    return false;
  }

  const wKey = 'w' + String(wi);
  const wName = workerNameMap.get(wKey) || '?';
  const wHandle = workerHandleMap.get(wKey) || 0;
  console.log('dispatchJob: dispatching to ' + wName + ' (handle=' + String(wHandle) + ')');

  workerBusyMap.set(wKey, true);
  workerJobMap.set(wKey, job.id);
  job.status = 'running';
  dbBuildStarted(job.id, wName);

  try {
    sendToClient(wHandle, JSON.stringify({
      type: 'job_assign',
      job_id: job.id,
      manifest: job.manifest,
      credentials: job.credentials,
      tarball_url: getPublicUrl() + '/api/v1/tarball/' + job.id,
      artifact_upload_url: getPublicUrl() + '/api/v1/artifact/upload/' + job.id,
    }));
  } catch (e) {
    workerBusyMap.set(wKey, false);
    workerJobMap.set(wKey, '');
    job.status = 'queued';
    return false;
  }

  return true;
}

function tryDispatchNext(): void {
  const job = dequeueJob();
  if (job) {
    if (!dispatchJob(job)) {
      // Re-enqueue if no worker available
      job.status = 'queued';
      jobQueue.unshift(job.id);
      counters.queueLen++;
    }
  }
}

// --- Artifact store ---

function registerArtifact(path: string, name: string, sha256: string, size: number): string {
  const token = crypto.randomUUID();
  const now = Date.now();
  const expires = now + ARTIFACT_TTL_SECS * 1000;
  const entry: ArtifactEntry = {
    path,
    name,
    size,
    sha256,
    expires,
    downloaded: false,
  };
  artifacts.set(token, entry);
  return token;
}

function takeArtifact(token: string): ArtifactEntry | null {
  const entry = artifacts.get(token);
  if (!entry) return null;
  if (entry.downloaded) return null;
  if (Date.now() > entry.expires) return null;
  entry.downloaded = true;
  return entry;
}

// Cleanup expired artifacts periodically
function startArtifactCleanup(): void {
  const cleanup = () => {
    const now = Date.now();
    const expired: string[] = [];
    artifacts.forEach((entry, token) => {
      if (now > entry.expires) {
        expired.push(token);
      }
    });
    for (let ei = 0; ei < expired.length; ei++) {
      const token = expired[ei];
      const entry = artifacts.get(token);
      if (entry) {
        try { fs.unlinkSync(entry.path); } catch (e) { /* ignore */ }
        artifacts.delete(token);
      }
    }
    setTimeout(cleanup, 60_000);
  };
  setTimeout(cleanup, 60_000);
}

// --- Relay messages to CLI clients ---

function addCliClient(jobId: string, handle: any): void {
  const hStr = String(handle);
  const existing = jobCliHandles.get(jobId) || '';
  if (existing) {
    jobCliHandles.set(jobId, existing + ',' + hStr);
  } else {
    jobCliHandles.set(jobId, hStr);
  }
}

function sendToAllCliClients(jobId: string, json: string): void {
  const handleStr = jobCliHandles.get(jobId) || '';
  if (!handleStr) return;
  const parts = handleStr.split(',');
  for (let pi = 0; pi < parts.length; pi++) {
    const hStr = parts[pi];
    if (!hStr) continue;
    const h = Number(hStr);
    sendToClient(h, json);
  }
}

function relayToCliClients(jobId: string, message: any): void {
  const json = JSON.stringify(message);
  sendToAllCliClients(jobId, json);
}

function relayToCliClientsJson(jobId: string, json: string): void {
  sendToAllCliClients(jobId, json);
}

// --- GitHub verification ---

async function verifyGitHubToken(token: string): Promise<string> {
  const resp = await fetch('https://api.github.com/user', {
    headers: {
      'Authorization': `Bearer ${token}`,
      'User-Agent': 'perry-hub/0.1.0',
      'Accept': 'application/vnd.github+json',
    },
  });

  if (!resp.ok) {
    throw new Error(`GitHub returned status ${resp.status}`);
  }

  const user = await resp.json() as { login: string };
  return user.login;
}

// --- Ensure directories ---

try { fs.mkdirSync(TARBALL_DIR, { recursive: true }); } catch (e) { /* exists */ }
try { fs.mkdirSync(ARTIFACT_DIR, { recursive: true }); } catch (e) { /* exists */ }
initDb();

// --- Fastify HTTP server ---

// 400MB body limit to accommodate base64-encoded tarballs (~33% overhead over binary)
const app = Fastify({ bodyLimit: 400 * 1024 * 1024 });

function getPublicUrl(): string {
  const v = process.env.PERRY_HUB_PUBLIC_URL || '';
  return v || 'https://hub.perryts.com';
}

// GET /api/v1/status
app.get('/api/v1/status', async (request: any, reply: any) => {
  reply.header('Content-Type', 'application/json');
  return '{"status":"ok","queue_length":' + String(counters.queueLen) + ',"perry_version":"0.1.0","supported_targets":' + cached.targetsJson + ',"connected_workers":' + String(counters.workers) + '}';
});

// POST /api/v1/license/register
app.post('/api/v1/license/register', async (request: any, reply: any) => {
  reply.header('Content-Type', 'application/json');
  let body: any = request.body || {};

  const username = body.github_username || '';
  const license = registerLicense(username, 'free');
  await dbSaveLicense(license);
  return JSON.stringify({
    license_key: license.key,
    tier: license.tier,
    platforms: license.platforms,
  });
});

// POST /api/v1/license/verify
app.post('/api/v1/license/verify', async (request: any, reply: any) => {
  reply.header('Content-Type', 'application/json');
  const body: any = request.body;
  if (!body) {
    reply.status(400);
    return JSON.stringify({ error: { code: 'BAD_REQUEST', message: 'Invalid JSON body' } });
  }

  const license = verifyLicense(body.license_key);
  if (license) {
    return JSON.stringify({ valid: true, tier: license.tier, platforms: license.platforms });
  } else {
    return JSON.stringify({ valid: false, tier: null, platforms: null });
  }
});

// POST /api/v1/build
app.post('/api/v1/build', async (request: any, reply: any) => {
  reply.header('Content-Type', 'application/json');
  const hdrs = request.headers;
  const contentType = hdrs['content-type'] || '';
  if (!contentType.includes('multipart/form-data')) {
    reply.status(400);
    return JSON.stringify({ error: { code: 'BAD_REQUEST', message: 'Expected multipart/form-data' } });
  }

  // Parse multipart body (request.text = raw body string from perry runtime)
  const rawBody = request.text;
  let parts: MultipartPart[];
  try {
    parts = parseMultipart(rawBody, contentType);
  } catch (e: any) {
    reply.status(400);
    return JSON.stringify({ error: { code: 'BAD_REQUEST', message: 'Failed to parse multipart body: ' + (e.message || e) } });
  }

  // Extract fields - use index loop (not .find()) to avoid perry closure issues
  // Use truthiness checks (not !== null) since perry's null comparison is broken
  let licensePart: MultipartPart | null = null;
  let manifestPart: MultipartPart | null = null;
  let credentialsPart: MultipartPart | null = null;
  let tarballB64Part: MultipartPart | null = null;
  for (let pi = 0; pi < parts.length; pi++) {
    const p = parts[pi];
    if (p.name === 'license_key') licensePart = p;
    else if (p.name === 'manifest') manifestPart = p;
    else if (p.name === 'credentials') credentialsPart = p;
    else if (p.name === 'tarball_b64') tarballB64Part = p;
  }

  if (!licensePart) {
    reply.status(400);
    return JSON.stringify({ error: { code: 'BAD_REQUEST', message: "Missing 'license_key' field" } });
  }
  if (!manifestPart) {
    reply.status(400);
    return JSON.stringify({ error: { code: 'BAD_REQUEST', message: "Missing 'manifest' field" } });
  }
  if (!tarballB64Part) {
    reply.status(400);
    return JSON.stringify({ error: { code: 'BAD_REQUEST', message: "Missing 'tarball_b64' field" } });
  }

  const licenseKey = licensePart.data;
  const manifest = JSON.parse(manifestPart.data);

  let credentials: any = {};
  if (credentialsPart) {
    credentials = JSON.parse(credentialsPart.data);
  }

  // Validate license
  const license = verifyLicense(licenseKey);
  if (!license) {
    reply.status(401);
    return JSON.stringify({ error: { code: 'LICENSE_INVALID', message: 'Invalid license key' } });
  }

  // Check rate limits (skip for self-hosted)
  if (!isSelfHosted()) {
    const rateLimitErr = checkRateLimit(license.key, license.tier, 0);
    if (rateLimitErr) {
      reply.status(429);
      return JSON.stringify({ error: { code: 'RATE_LIMITED', message: rateLimitErr } });
    }
  }

  // Save tarball base64 to disk so workers can download it
  const jobId = crypto.randomUUID();
  const tarballB64Path = TARBALL_DIR + '/' + jobId + '.b64';
  fs.writeFileSync(tarballB64Path, tarballB64Part.data);

  // Create and enqueue job
  const job: Job = {
    id: jobId,
    license_key: license.key,
    tier: license.tier,
    manifest,
    credentials,
    status: 'queued',
    created_at: Date.now(),
    priority: license.tier === 'pro' ? 10 : 1,
  };

  const position = enqueueJob(job);
  dbInsertBuild(job);

  // Try to dispatch immediately
  tryDispatchNext();

  return JSON.stringify({
    job_id: jobId,
    ws_url: '/ws',
    position,
  });
});

// GET /api/v1/dl/:token
app.get('/api/v1/dl/:token', async (request: any, reply: any) => {
  const token = request.params.token;
  const entry = takeArtifact(token);

  if (!entry) {
    reply.status(404);
    reply.header('Content-Type', 'application/json');
    return JSON.stringify({ error: { code: 'ARTIFACT_NOT_FOUND', message: 'Artifact not found or expired' } });
  }

  try {
    const data = fs.readFileSync(entry.path);
    reply.header('Content-Type', 'application/octet-stream');
    reply.header('Content-Disposition', `attachment; filename="${entry.name}"`);
    reply.header('Content-Length', String(entry.size));
    return data;
  } catch (e: any) {
    reply.status(500);
    reply.header('Content-Type', 'application/json');
    return JSON.stringify({ error: { code: 'INTERNAL_ERROR', message: 'Failed to read artifact: ' + (e.message || e) } });
  }
});

// GET /api/v1/tarball/:jobId — workers download the base64-encoded tarball for a job
app.get('/api/v1/tarball/:jobId', async (request: any, reply: any) => {
  const jobId = request.params.jobId;
  const b64Path = TARBALL_DIR + '/' + jobId + '.b64';
  try {
    const b64Data = fs.readFileSync(b64Path);
    reply.header('Content-Type', 'text/plain');
    return b64Data;
  } catch (e: any) {
    reply.status(404);
    reply.header('Content-Type', 'application/json');
    return JSON.stringify({ error: { code: 'NOT_FOUND', message: 'Tarball not found for job: ' + jobId } });
  }
});

// POST /api/v1/artifact/upload/:jobId — workers upload built artifacts (base64-encoded)
// Headers: x-artifact-name (filename), x-artifact-sha256 (checksum), x-artifact-target (platform)
// Body: base64-encoded artifact data (text/plain)
app.post('/api/v1/artifact/upload/:jobId', async (request: any, reply: any) => {
  reply.header('Content-Type', 'application/json');
  const jobId = request.params.jobId;
  const hdrs = request.headers;
  const artifactName = hdrs['x-artifact-name'] || 'artifact';
  const sha256 = hdrs['x-artifact-sha256'] || '';
  const target = hdrs['x-artifact-target'] || '';

  const job = jobs.get(jobId);
  if (!job) {
    reply.status(404);
    return JSON.stringify({ error: { code: 'JOB_NOT_FOUND', message: 'Job not found' } });
  }
  if (job.status !== 'running') {
    reply.status(409);
    return JSON.stringify({ error: { code: 'JOB_NOT_RUNNING', message: 'Job is not in running state' } });
  }

  const rawBody = request.text || request.body || '';
  if (!rawBody || typeof rawBody !== 'string') {
    reply.status(400);
    return JSON.stringify({ error: { code: 'BAD_REQUEST', message: 'Missing base64 artifact data in body' } });
  }

  const b64Data = rawBody.trim();

  // Decode base64 and write to artifact directory
  const artifactId = crypto.randomUUID();
  const artifactPath = ARTIFACT_DIR + '/' + artifactId;
  const buffer = Buffer.from(b64Data, 'base64');
  fs.writeFileSync(artifactPath, buffer);
  const size = buffer.length;

  // Register artifact with download token
  const token = registerArtifact(artifactPath, artifactName, sha256, size);
  const downloadUrl = getPublicUrl() + '/api/v1/dl/' + token;

  // Notify CLI clients watching this job
  const artJson = '{"type":"artifact_ready","job_id":"' + jobId + '","target":"' + target + '","artifact_name":"' + artifactName + '","artifact_size":' + String(size) + ',"sha256":"' + sha256 + '","download_url":"' + downloadUrl + '","expires_in_secs":' + String(ARTIFACT_TTL_SECS) + '}';
  relayToCliClientsJson(jobId, artJson);

  console.log('Artifact uploaded for job ' + jobId + ': ' + artifactName + ' (' + String(size) + ' bytes)');

  return JSON.stringify({ token: token, download_url: downloadUrl, size: size });
});

// --- WebSocket server ---
// Uses server-level events (wss.on) instead of per-connection ws.on()
// because perry's codegen can't compile ws.on() inside arrow callbacks.
// Server-level events receive (clientHandle, data) where clientHandle
// identifies the specific client connection.

const wss = new WebSocketServer({ port: WS_PORT });

// Connection event: just register the new client
wss.on('connection', (clientHandle: any) => {
  registerClient(clientHandle);
});

// Message event: server-level, receives (clientHandle, data)
wss.on('message', (clientHandle: any, data: any) => {
  let msg: any;
  try {
    msg = JSON.parse(data);
  } catch (e: any) {
    return;
  }

  if (!isClientRegistered(clientHandle)) {
    return;
  }

  // First message identifies the role
  if (!isClientIdentified(clientHandle)) {
    setClientIdentified(clientHandle);

    if (msg.type === 'worker_hello') {
      setClientRole(clientHandle, 'worker');
      // Build comma-delimited capStr from msg.capabilities array
      // Format: ",macos,ios,android," for reliable indexOf matching
      let capStr = ',';
      const rawCaps = msg.capabilities;
      if (rawCaps) {
        for (let ci = 0; ci < rawCaps.length; ci++) {
          capStr = capStr + rawCaps[ci] + ',';
        }
      } else {
        capStr = ',macos,';
      }
      const workerName = msg.name || 'worker-' + String(counters.workers + 1);
      const wKey = 'w' + String(counters.workers);
      const workerInfo: WorkerInfo = {
        clientHandle,
        capStr: capStr,
        name: workerName,
        busy: false,
        current_job_id: null,
      };
      workerList[counters.workers] = workerInfo;  // keep for backwards compat
      workerCapMap.set(wKey, capStr);
      workerNameMap.set(wKey, workerName);
      workerHandleMap.set(wKey, clientHandle);
      workerBusyMap.set(wKey, false);
      workerJobMap.set(wKey, '');
      setClientWorkerIdx(clientHandle, counters.workers);
      counters.workers++;
      rebuildTargetsJson();
      console.log('Worker connected: ' + workerName + ' caps=' + capStr);
      tryDispatchNext();
      return;
    }

    if (msg.type === 'subscribe') {
      setClientRole(clientHandle, 'cli');
      const job = jobs.get(msg.job_id);
      if (job) {
        addCliClient(msg.job_id, clientHandle);
        // Send job_created
        const resp = '{"type":"job_created","job_id":"' + job.id + '","position":0,"estimated_wait_secs":0}';
        sendToClient(clientHandle, resp);

        // If job is already completed, send the completion message immediately
        if (job.status === 'completed' || job.status === 'failed') {
          const success = job.status === 'completed';
          const completeMsg = '{"type":"complete","job_id":"' + job.id + '","success":' + String(success) + ',"duration_secs":0,"artifacts":[]}';
          sendToClient(clientHandle, completeMsg);
        }
      } else {
        const errMsg = '{"type":"error","code":"JOB_NOT_FOUND","message":"Job not found"}';
        sendToClient(clientHandle, errMsg);
        closeClient(clientHandle);
      }
      return;
    }

    // Unknown first message
    closeClient(clientHandle);
    return;
  }

  // Subsequent messages based on role
  const role = getClientRole(clientHandle);
  if (role === 'worker') {
    const wIdx = getClientWorkerIdx(clientHandle);
    if (wIdx >= 0 && wIdx < counters.workers) {
      handleWorkerMessageByIdx(msg, wIdx);
    }
  } else if (role === 'cli') {
    handleCliMessage(msg, clientHandle);
  }
});

// Close event: server-level, receives (clientHandle)
wss.on('close', (clientHandle: any) => {
  const role = getClientRole(clientHandle);
  if (!role) {
    removeClient(clientHandle);
    return;
  }

  if (role === 'worker') {
    const wIdx = getClientWorkerIdx(clientHandle);
    if (wIdx >= 0 && wIdx < counters.workers) {
      const wKey = 'w' + String(wIdx);
      const wName = workerNameMap.get(wKey) || '?';
      const wBusy = workerBusyMap.get(wKey) || false;
      const wJobId = workerJobMap.get(wKey) || '';
      console.log('Worker disconnected: ' + wName);

      if (wBusy && wJobId) {
        const job = jobs.get(wJobId);
        if (job && job.status === 'running') {
          job.status = 'failed';
          releaseRateLimit(job.license_key);
          const errJson = '{"type":"error","code":"INTERNAL_ERROR","message":"Worker disconnected during build"}';
          relayToCliClientsJson(job.id, errJson);
          const dur = (Date.now() - job.created_at) / 1000;
          const completeJson = '{"type":"complete","job_id":"' + job.id + '","success":false,"duration_secs":' + String(dur) + ',"artifacts":[]}';
          relayToCliClientsJson(job.id, completeJson);
        }
      }

      // Remove worker from list — avoid splice() in perry, shift down manually
      counters.workers--;
      for (let wi = wIdx; wi < counters.workers; wi++) {
        const nextKey = 'w' + String(wi + 1);
        const curKey = 'w' + String(wi);
        workerList[wi] = workerList[wi + 1];
        workerCapMap.set(curKey, workerCapMap.get(nextKey) || ',');
        workerNameMap.set(curKey, workerNameMap.get(nextKey) || '?');
        const nextHandle = workerHandleMap.get(nextKey) || 0;
        workerHandleMap.set(curKey, nextHandle);
        workerBusyMap.set(curKey, workerBusyMap.get(nextKey) || false);
        workerJobMap.set(curKey, workerJobMap.get(nextKey) || '');
        setClientWorkerIdx(nextHandle, wi);
      }
      const lastKey = 'w' + String(counters.workers);
      workerCapMap.delete(lastKey);
      workerNameMap.delete(lastKey);
      workerHandleMap.delete(lastKey);
      workerBusyMap.delete(lastKey);
      workerJobMap.delete(lastKey);
      rebuildTargetsJson();
    }
  }

  if (role === 'cli') {
    // Remove this handle from all job CLI handle lists
    // (in practice, a CLI is subscribed to just one job)
    const hStr = String(clientHandle);
    jobs.forEach((job: Job) => {
      const existing = jobCliHandles.get(job.id) || '';
      if (existing.includes(hStr)) {
        const parts = existing.split(',');
        let newHandles = '';
        for (let fi = 0; fi < parts.length; fi++) {
          const p = parts[fi];
          if (p && p !== hStr) {
            if (newHandles) {
              newHandles = newHandles + ',' + p;
            } else {
              newHandles = p;
            }
          }
        }
        jobCliHandles.set(job.id, newHandles);
      }
    });
  }

  removeClient(clientHandle);
});

wss.on('error', (err: any) => {
  console.error('WebSocket server error:', err);
});

function handleWorkerMessageByIdx(msg: any, wIdx: number): void {
  const wKey = 'w' + String(wIdx);
  const msgType = msg.type;
  const jobId = msg.job_id || workerJobMap.get(wKey) || '';

  if (msgType === 'progress' || msgType === 'stage' || msgType === 'log' || msgType === 'queue_update' || msgType === 'published') {
    if (jobId) {
      relayToCliClients(jobId, msg);
    }
  } else if (msgType === 'artifact_ready') {
    if (!jobId) return;
    const job = jobs.get(jobId);
    if (!job) return;

    const token = registerArtifact(
      msg.path,
      msg.artifact_name,
      msg.sha256,
      msg.size,
    );
    const downloadUrl = getPublicUrl() + '/api/v1/dl/' + token;
    const target = msg.target || '';

    const artJson = '{"type":"artifact_ready","job_id":"' + jobId + '","target":"' + target + '","artifact_name":"' + msg.artifact_name + '","artifact_size":' + String(msg.size) + ',"sha256":"' + msg.sha256 + '","download_url":"' + downloadUrl + '","expires_in_secs":' + String(ARTIFACT_TTL_SECS) + '}';
    relayToCliClientsJson(jobId, artJson);
  } else if (msgType === 'complete') {
    if (!jobId) return;

    const job = jobs.get(jobId);
    if (job) {
      if (msg.success) {
        job.status = 'completed';
      } else {
        job.status = 'failed';
      }
      releaseRateLimit(job.license_key);
      const durationSecs = Math.round((Date.now() - job.created_at) / 1000);
      dbBuildFinished(jobId, msg.success, durationSecs, msg.error || '');
      // Clean up tarball b64 file now that the worker is done with it
      try { fs.unlinkSync(TARBALL_DIR + '/' + jobId + '.b64'); } catch (e) { /* ignore */ }
    }

    const completeJson = JSON.stringify(msg);
    relayToCliClientsJson(jobId, completeJson);

    workerBusyMap.set(wKey, false);
    workerJobMap.set(wKey, '');

    tryDispatchNext();
  } else if (msgType === 'error') {
    if (jobId) {
      relayToCliClients(jobId, msg);
    }
  }
}

function handleCliMessage(msg: any, clientHandle: any): void {
  if (msg.type === 'cancel' && msg.job_id) {
    const job = jobs.get(msg.job_id);
    if (job && job.status === 'running') {
      // Find the worker handling this job and send cancel
      for (let wi = 0; wi < counters.workers; wi++) {
        const wKey = 'w' + String(wi);
        const wJobId = workerJobMap.get(wKey) || '';
        if (wJobId === msg.job_id) {
          try {
            const wHandle = workerHandleMap.get(wKey) || 0;
            sendToClient(wHandle, JSON.stringify({ type: 'cancel', job_id: msg.job_id }));
          } catch (e) {
            // Worker may have disconnected
          }
          break;
        }
      }
    }
  }
}

// --- Start servers ---

startArtifactCleanup();

// Register WS event handlers BEFORE app.listen() since it enters the event loop and never returns
wss.on('listening', () => {
  console.log(`Perry Hub WebSocket server listening on port ${WS_PORT}`);
});

app.listen({ port: HTTP_PORT, host: '0.0.0.0' }, (err: any, address: string) => {
  if (err) {
    console.error('Failed to start HTTP server:', err);
    process.exit(1);
  }
  console.log(`Perry Hub HTTP server listening on port ${HTTP_PORT}`);
});
