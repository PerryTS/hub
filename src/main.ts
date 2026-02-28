// perry-hub: Public-facing build server
// Receives build uploads from CLI, dispatches to workers via WebSocket

import Fastify from 'fastify';
import * as fs from 'fs';
import * as crypto from 'crypto';
import { WebSocketServer, sendToClient, closeClient } from 'ws';

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
    for (const line of lines) {
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
const LICENSE_DIR = '/tmp/perry-hub-data';
const LICENSE_FILE = '/tmp/perry-hub-data/licenses.json';

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
  tarball_path: string;
  status: 'queued' | 'running' | 'completed' | 'failed' | 'cancelled';
  created_at: number;
  priority: number;
}

interface WorkerInfo {
  clientHandle: any; // WS client handle from server-level event
  capabilities: string[];
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

function saveLicenses(): void {
  try {
    const arr: License[] = [];
    licenses.forEach((license: License) => {
      arr.push(license);
    });
    fs.writeFileSync(LICENSE_FILE, JSON.stringify(arr));
  } catch (e) {
    console.error('Failed to save licenses:', e);
  }
}

function loadLicenses(): void {
  try {
    if (fs.existsSync(LICENSE_FILE)) {
      const data = fs.readFileSync(LICENSE_FILE, 'utf-8');
      const arr = JSON.parse(data) as License[];
      for (const license of arr) {
        licenses.set(license.key, license);
      }
      console.log('Loaded ' + String(arr.length) + ' licenses from disk');
    }
  } catch (e) {
    console.error('Failed to load licenses:', e);
  }
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

function getAvailableWorker(requiredCapabilities: string[]): WorkerInfo | null {
  for (const worker of workerList) {
    if (!worker.busy) {
      const hasAll = requiredCapabilities.every(cap => worker.capabilities.includes(cap));
      if (hasAll) {
        return worker;
      }
    }
  }
  return null;
}

function getSupportedTargets(): string[] {
  const targets = new Set<string>();
  for (const worker of workerList) {
    for (const cap of worker.capabilities) {
      targets.add(cap);
    }
  }
  return Array.from(targets);
}

function dispatchJob(job: Job): boolean {
  // Determine required capabilities from manifest targets
  const requiredCaps: string[] = [];
  if (job.manifest.targets) {
    for (const t of job.manifest.targets) {
      requiredCaps.push(t.toLowerCase());
    }
  }
  if (requiredCaps.length === 0) {
    requiredCaps.push('macos'); // default
  }

  const worker = getAvailableWorker(requiredCaps);
  if (!worker) {
    return false;
  }

  worker.busy = true;
  worker.current_job_id = job.id;
  job.status = 'running';

  try {
    sendToClient(worker.clientHandle, JSON.stringify({
      type: 'job_assign',
      job_id: job.id,
      manifest: job.manifest,
      credentials: job.credentials,
      tarball_path: job.tarball_path,
    }));
  } catch (e) {
    worker.busy = false;
    worker.current_job_id = null;
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
    for (const token of expired) {
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
  for (const hStr of parts) {
    if (!hStr) continue;
    const h = Number(hStr);
    try {
      sendToClient(h, json);
    } catch (e) { /* ignore */ }
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
try { fs.mkdirSync(LICENSE_DIR, { recursive: true }); } catch (e) { /* exists */ }
loadLicenses();

// --- Fastify HTTP server ---

const app = Fastify({ bodyLimit: 200 * 1024 * 1024 });

// GET /api/v1/status
app.get('/api/v1/status', async (request: any, reply: any) => {
  reply.header('Content-Type', 'application/json');
  const targets = getSupportedTargets();
  const targetsJson = '[' + targets.map((t: string) => '"' + t + '"').join(',') + ']';
  return '{"status":"ok","queue_length":' + String(counters.queueLen) + ',"perry_version":"0.1.0","supported_targets":' + targetsJson + ',"connected_workers":' + String(counters.workers) + '}';
});

// POST /api/v1/license/register
app.post('/api/v1/license/register', async (request: any, reply: any) => {
  reply.header('Content-Type', 'application/json');
  let body: any = {};
  try {
    if (request.body) {
      body = JSON.parse(request.body);
    }
  } catch (e) {
    // Accept empty or invalid body — no fields required
  }

  const username = body.github_username || '';
  const license = registerLicense(username, 'free');
  saveLicenses();
  return JSON.stringify({
    license_key: license.key,
    tier: license.tier,
    platforms: license.platforms,
  });
});

// POST /api/v1/license/verify
app.post('/api/v1/license/verify', async (request: any, reply: any) => {
  reply.header('Content-Type', 'application/json');
  let body: any;
  try {
    body = JSON.parse(request.body);
  } catch (e) {
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
  const hdrs = JSON.parse(request.headers);
  const contentType = hdrs['content-type'] || '';
  if (!contentType.includes('multipart/form-data')) {
    reply.status(400);
    return JSON.stringify({ error: { code: 'BAD_REQUEST', message: 'Expected multipart/form-data' } });
  }

  // Parse multipart body
  let parts: MultipartPart[];
  try {
    parts = parseMultipart(request.body, contentType);
  } catch (e: any) {
    reply.status(400);
    return JSON.stringify({ error: { code: 'BAD_REQUEST', message: 'Failed to parse multipart body: ' + (e.message || e) } });
  }

  // Extract fields
  const licensePart = parts.find((p: any) => p.name === 'license_key');
  const manifestPart = parts.find((p: any) => p.name === 'manifest');
  const credentialsPart = parts.find((p: any) => p.name === 'credentials');
  const projectPathPart = parts.find((p: any) => p.name === 'project_path');

  if (!licensePart) {
    reply.status(400);
    return JSON.stringify({ error: { code: 'BAD_REQUEST', message: "Missing 'license_key' field" } });
  }
  if (!manifestPart) {
    reply.status(400);
    return JSON.stringify({ error: { code: 'BAD_REQUEST', message: "Missing 'manifest' field" } });
  }
  if (!projectPathPart) {
    reply.status(400);
    return JSON.stringify({ error: { code: 'BAD_REQUEST', message: "Missing 'project_path' field" } });
  }

  const licenseKey = licensePart.data;
  let manifest: any;
  try {
    manifest = JSON.parse(manifestPart.data);
  } catch (e) {
    reply.status(400);
    return JSON.stringify({ error: { code: 'BAD_REQUEST', message: 'Invalid manifest JSON' } });
  }

  let credentials: any = {};
  if (credentialsPart) {
    try {
      credentials = JSON.parse(credentialsPart.data);
    } catch (e) {
      reply.status(400);
      return JSON.stringify({ error: { code: 'BAD_REQUEST', message: 'Invalid credentials JSON' } });
    }
  }

  // Use the tarball path directly from the CLI (avoids binary corruption in text-based body parsing)
  const tarballPath = projectPathPart.data.trim();
  if (!fs.existsSync(tarballPath)) {
    reply.status(400);
    return JSON.stringify({ error: { code: 'BAD_REQUEST', message: 'Tarball not found at path: ' + tarballPath } });
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

  // Create and enqueue job
  const jobId = crypto.randomUUID();
  const job: Job = {
    id: jobId,
    license_key: license.key,
    tier: license.tier,
    manifest,
    credentials,
    tarball_path: tarballPath,
    status: 'queued',
    created_at: Date.now(),
    priority: license.tier === 'pro' ? 10 : 1,
  };

  const position = enqueueJob(job);

  // Try to dispatch immediately
  tryDispatchNext();

  return JSON.stringify({
    job_id: jobId,
    ws_url: `ws://localhost:${WS_PORT}`,
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
      const workerInfo: WorkerInfo = {
        clientHandle,
        capabilities: msg.capabilities || ['macos'],
        name: msg.name || 'worker-' + String(counters.workers + 1),
        busy: false,
        current_job_id: null,
      };
      workerList.push(workerInfo);
      setClientWorkerIdx(clientHandle, counters.workers);
      counters.workers++;
      console.log('Worker connected: ' + workerInfo.name);
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
      handleWorkerMessage(msg, workerList[wIdx]);
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
      const workerInfo = workerList[wIdx];
      console.log('Worker disconnected: ' + workerInfo.name);

      if (workerInfo.busy && workerInfo.current_job_id) {
        const job = jobs.get(workerInfo.current_job_id);
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

      // Remove worker from list
      workerList.splice(wIdx, 1);
      counters.workers--;
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
        const filtered = parts.filter((p: string) => p !== hStr);
        jobCliHandles.set(job.id, filtered.join(','));
      }
    });
  }

  removeClient(clientHandle);
});

wss.on('error', (err: any) => {
  console.error('WebSocket server error:', err);
});

function handleWorkerMessage(msg: any, worker: WorkerInfo): void {
  const msgType = msg.type;
  const jobId = msg.job_id || worker.current_job_id;

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
    const downloadUrl = '/api/v1/dl/' + token;

    const artJson = '{"type":"artifact_ready","artifact_name":"' + msg.artifact_name + '","artifact_size":' + String(msg.size) + ',"sha256":"' + msg.sha256 + '","download_url":"' + downloadUrl + '","expires_in_secs":' + String(ARTIFACT_TTL_SECS) + ',"download_path":"' + msg.path + '"}';
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
    }

    const completeJson = JSON.stringify(msg);
    relayToCliClientsJson(jobId, completeJson);

    worker.busy = false;
    worker.current_job_id = null;

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
      for (const worker of workerList) {
        if (worker.current_job_id === msg.job_id) {
          try {
            sendToClient(worker.clientHandle, JSON.stringify({ type: 'cancel', job_id: msg.job_id }));
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
