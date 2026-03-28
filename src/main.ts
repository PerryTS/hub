// perry-hub: Public-facing build server
// Receives build uploads from CLI, dispatches to workers via WebSocket

import Fastify from 'fastify';
import * as fs from 'fs';
import * as crypto from 'crypto';
import * as child_process from 'child_process';
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
const WORKER_SECRET = process.env.PERRY_HUB_WORKER_SECRET || '';
const ADMIN_SECRET = process.env.PERRY_HUB_ADMIN_SECRET || '';

function isSelfHosted(): boolean {
  const v = process.env.PERRY_HUB_SELF_HOSTED || '';
  return v === 'true' || v === '1';
}
const TARBALL_DIR = '/tmp/perry-hub-tarballs';
const ARTIFACT_DIR = '/tmp/perry-artifacts';
const VERIFY_URL = process.env.PERRY_VERIFY_URL || 'http://localhost:7777';
const VERIFY_POLL_INTERVAL_MS = 5000;
const VERIFY_TIMEOUT_MS = 120000; // 2 minutes max

// --- Verification state ---
// Track pending verifications: buildJobId → verifyJobId
const verifyJobIdMap = new Map<string, string>();
// Track which worker index is waiting for verification
const verifyWorkerIdxMap = new Map<string, number>();
// Track artifact path for verification
const verifyArtifactPathMap = new Map<string, string>();
// Track verification start time for timeout
const verifyStartTimeMap = new Map<string, number>();

// --- Database pool ---
// createPool must be called inside a function (not inline at module level)
// for perry to properly pass the object literal to the native mysql2 FFI.
// The returned pool must be assigned to a named const so perry's HIR
// transform can track it as a mysql2 Pool for method dispatch.

function createDbPool(): Pool {
  return mysql.createPool({
    host: process.env.PERRY_DB_HOST || 'localhost',
    port: parseInt(process.env.PERRY_DB_PORT || '3306', 10),
    user: process.env.PERRY_DB_USER || 'perry',
    password: process.env.PERRY_DB_PASSWORD || '',
    database: process.env.PERRY_DB_NAME || 'perry_hub',
  });
}

const db = createDbPool();

// --- Types ---

interface License {
  key: string;
  tier: 'free' | 'pro';
  github_username: string;
  platforms: string[];
  account_id: string;       // '' if no account linked
  device_bound: boolean;
  project_bundle_id: string; // '' if not yet set
}

interface Account {
  id: string;
  github_username: string;
  github_id: string;
  email: string;
  tier: 'free' | 'pro';
  polar_customer_id: string;
  polar_subscription_id: string;
  api_token: string;
  has_payment_method: boolean;
  created_at: number;
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
  retries: number;
}

const MAX_JOB_RETRIES = 3;

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
const accounts = new Map<string, Account>();           // id -> Account
const accountsByToken = new Map<string, string>();     // api_token -> account_id
const rateLimits = new Map<string, RateLimitState>();
const jobs = new Map<string, Job>();
const jobQueue: string[] = []; // job IDs in priority order
const workerList: WorkerInfo[] = [];
const counters = { workers: 0, queueLen: 0 };
const artifacts = new Map<string, ArtifactEntry>();
const jobTokens = new Map<string, string>(); // job_id -> bearer token for worker auth

// Escape a string for safe interpolation into hand-built JSON
function jsonEscape(s: string): string {
  return s.replace(/\\/g, '\\\\').replace(/"/g, '\\"').replace(/\r/g, '\\r').replace(/\n/g, '\\n');
}

// Use Maps for worker property lookup (perry workaround: reading properties from
// objects stored in module-level arrays is unreliable)
const workerCapMap = new Map<string, string>(); // "w0" -> ",macos,ios,android,"
const workerNameMap = new Map<string, string>(); // "w0" -> "macbook-intel"
const workerHandleMap = new Map<string, number>(); // "w0" -> clientHandle
const workerBusyMap = new Map<string, boolean>(); // "w0" -> false (legacy)
const workerActiveJobsMap = new Map<string, number>(); // "w0" -> 0
const workerMaxConcurrentMap = new Map<string, number>(); // "w0" -> 2
const workerJobMap = new Map<string, string>(); // "w0" -> current_job_id or ""
const workerVersionMap = new Map<string, string>(); // "w0" -> "0.2.181"
// Precomputed JSON string of supported targets (rebuilt on worker connect/disconnect)
const cached = { targetsJson: '[]' };
// Expected perry version — set via admin endpoint or env
const perryExpected = { version: process.env.PERRY_EXPECTED_VERSION || '' };

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
      notes TEXT,
      account_id VARCHAR(36) DEFAULT NULL,
      device_bound BOOLEAN NOT NULL DEFAULT FALSE,
      project_bundle_id VARCHAR(255) DEFAULT NULL
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

    await db.query(`CREATE TABLE IF NOT EXISTS accounts (
      id VARCHAR(36) PRIMARY KEY,
      github_username VARCHAR(255) NOT NULL,
      github_id VARCHAR(64) NOT NULL,
      email VARCHAR(255) NOT NULL DEFAULT '',
      tier VARCHAR(8) NOT NULL DEFAULT 'free',
      polar_customer_id VARCHAR(64) DEFAULT NULL,
      polar_subscription_id VARCHAR(64) DEFAULT NULL,
      api_token VARCHAR(64) DEFAULT NULL,
      has_payment_method BOOLEAN NOT NULL DEFAULT FALSE,
      created_at BIGINT NOT NULL,
      UNIQUE INDEX idx_github_id (github_id),
      UNIQUE INDEX idx_api_token (api_token)
    )`);

    await db.query(`CREATE TABLE IF NOT EXISTS usage_counters (
      license_key VARCHAR(64) NOT NULL,
      period VARCHAR(7) NOT NULL,
      publishes INT NOT NULL DEFAULT 0,
      deep_verifies INT NOT NULL DEFAULT 0,
      PRIMARY KEY (license_key, period)
    )`);

    // Add new columns to licenses if they don't exist (migration for existing installs)
    try { await db.query('ALTER TABLE licenses ADD COLUMN account_id VARCHAR(36) DEFAULT NULL'); } catch (e) { /* already exists */ }
    try { await db.query('ALTER TABLE licenses ADD COLUMN device_bound BOOLEAN NOT NULL DEFAULT FALSE'); } catch (e) { /* already exists */ }
    try { await db.query('ALTER TABLE licenses ADD COLUMN project_bundle_id VARCHAR(255) DEFAULT NULL'); } catch (e) { /* already exists */ }

    const result = await db.query('SELECT licenseKey, tier, githubUsername, platforms, account_id, device_bound, project_bundle_id FROM licenses');
    const rows: any = result[0];
    for (let i = 0; i < rows.length; i++) {
      const row = rows[i];
      const license: License = {
        key: row.licenseKey,
        tier: row.tier,
        github_username: row.githubUsername,
        platforms: JSON.parse(row.platforms),
        account_id: row.account_id || '',
        device_bound: row.device_bound ? true : false,
        project_bundle_id: row.project_bundle_id || '',
      };
      licenses.set(license.key, license);
    }
    // Load accounts
    const accResult = await db.query('SELECT id, github_username, github_id, email, tier, polar_customer_id, polar_subscription_id, api_token, has_payment_method, created_at FROM accounts');
    const accRows: any = accResult[0];
    for (let ai = 0; ai < accRows.length; ai++) {
      const ar = accRows[ai];
      const account: Account = {
        id: ar.id,
        github_username: ar.github_username,
        github_id: ar.github_id,
        email: ar.email || '',
        tier: ar.tier,
        polar_customer_id: ar.polar_customer_id || '',
        polar_subscription_id: ar.polar_subscription_id || '',
        api_token: ar.api_token || '',
        has_payment_method: ar.has_payment_method ? true : false,
        created_at: ar.created_at,
      };
      accounts.set(account.id, account);
      if (account.api_token) {
        accountsByToken.set(account.api_token, account.id);
      }
    }

    console.log('DB ready, loaded ' + String(licenses.size) + ' licenses, ' + String(accounts.size) + ' accounts');
  } catch (e: any) {
    console.error('DB init error:', e.message || e);
  }
}

async function dbSaveLicense(license: License): Promise<void> {
  try {
    await db.execute(
      'INSERT INTO licenses (licenseKey, tier, githubUsername, platforms, createdAt, account_id, device_bound, project_bundle_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE tier=VALUES(tier), platforms=VALUES(platforms), account_id=VALUES(account_id), device_bound=VALUES(device_bound), project_bundle_id=VALUES(project_bundle_id)',
      [license.key, license.tier, license.github_username, JSON.stringify(license.platforms), Date.now(), license.account_id || null, license.device_bound ? 1 : 0, license.project_bundle_id || null]
    );
  } catch (e: any) {
    console.error('dbSaveLicense error:', e.message || e);
  }
}

async function dbSaveAccount(account: Account): Promise<void> {
  try {
    await db.execute(
      'INSERT INTO accounts (id, github_username, github_id, email, tier, polar_customer_id, polar_subscription_id, api_token, has_payment_method, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE tier=VALUES(tier), polar_customer_id=VALUES(polar_customer_id), polar_subscription_id=VALUES(polar_subscription_id), api_token=VALUES(api_token), has_payment_method=VALUES(has_payment_method), email=VALUES(email)',
      [account.id, account.github_username, account.github_id, account.email, account.tier, account.polar_customer_id || null, account.polar_subscription_id || null, account.api_token || null, account.has_payment_method ? 1 : 0, account.created_at]
    );
  } catch (e: any) {
    console.error('dbSaveAccount error:', e.message || e);
  }
}

// --- Usage tracking ---

function getCurrentPeriod(): string {
  const d = new Date();
  const y = String(d.getFullYear());
  const m = String(d.getMonth() + 1);
  const mm = m.length === 1 ? '0' + m : m;
  return y + '-' + mm;
}

// In-memory usage cache (keyed by "licenseKey:YYYY-MM")
const usageCache = new Map<string, { publishes: number; deep_verifies: number }>();

async function getUsage(licenseKey: string): Promise<{ publishes: number; deep_verifies: number }> {
  const period = getCurrentPeriod();
  const cacheKey = licenseKey + ':' + period;
  const cached = usageCache.get(cacheKey);
  if (cached) return cached;

  try {
    const result = await db.query('SELECT publishes, deep_verifies FROM usage_counters WHERE license_key = ? AND period = ?', [licenseKey, period]);
    const rows: any = result[0];
    if (rows.length > 0) {
      const usage = { publishes: rows[0].publishes, deep_verifies: rows[0].deep_verifies };
      usageCache.set(cacheKey, usage);
      return usage;
    }
  } catch (e: any) {
    console.error('getUsage error:', e.message || e);
  }
  const fresh = { publishes: 0, deep_verifies: 0 };
  usageCache.set(cacheKey, fresh);
  return fresh;
}

async function incrementUsage(licenseKey: string, field: 'publishes' | 'deep_verifies'): Promise<void> {
  const period = getCurrentPeriod();
  const cacheKey = licenseKey + ':' + period;
  try {
    if (field === 'publishes') {
      await db.execute(
        'INSERT INTO usage_counters (license_key, period, publishes, deep_verifies) VALUES (?, ?, 1, 0) ON DUPLICATE KEY UPDATE publishes = publishes + 1',
        [licenseKey, period]
      );
    } else {
      await db.execute(
        'INSERT INTO usage_counters (license_key, period, publishes, deep_verifies) VALUES (?, ?, 0, 1) ON DUPLICATE KEY UPDATE deep_verifies = deep_verifies + 1',
        [licenseKey, period]
      );
    }
    // Update cache
    const cached = usageCache.get(cacheKey);
    if (cached) {
      if (field === 'publishes') cached.publishes++;
      else cached.deep_verifies++;
    }
  } catch (e: any) {
    console.error('incrementUsage error:', e.message || e);
  }
}

function getPublishLimit(tier: 'free' | 'pro'): number {
  return tier === 'pro' ? 50 : 15;
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

  // Increment usage counter on successful build
  if (success) {
    const job = jobs.get(jobId);
    if (job) {
      incrementUsage(job.license_key, 'publishes');
      // Report overage to Polar if applicable
      reportOverageIfNeeded(job.license_key);
    }
  }
}

function reportOverageIfNeeded(licenseKey: string): void {
  const license = licenses.get(licenseKey);
  if (!license || !license.account_id) return;
  const account = accounts.get(license.account_id);
  if (!account || !account.polar_customer_id) return;

  const polarToken = process.env.POLAR_ACCESS_TOKEN || '';
  if (!polarToken) return;

  const period = getCurrentPeriod();
  const cacheKey = licenseKey + ':' + period;
  const cached = usageCache.get(cacheKey);
  if (!cached) return;

  const limit = getPublishLimit(license.tier);
  if (cached.publishes <= limit) return;

  // Over the limit — report one overage event to Polar
  const meterId = process.env.POLAR_METER_PUBLISH || '';
  if (!meterId) return;

  try {
    child_process.exec(
      'curl -s -X POST "https://api.polar.sh/v1/events" -H "Authorization: Bearer ' + polarToken + '" -H "Content-Type: application/json" -d \'{"customer_id":"' + account.polar_customer_id + '","meter_id":"' + meterId + '","value":1}\'',
      { timeout: 10000 },
      (error: any, stdout: any, stderr: any) => {
        if (error) {
          console.error('Polar overage report failed: ' + (error.message || error));
        }
      }
    );
  } catch (e: any) {
    console.error('Polar overage report error: ' + (e.message || e));
  }
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

function registerLicense(username: string, tier: 'free' | 'pro', deviceBound: boolean): License {
  const key = generateLicenseKey(tier);
  const platforms = tier === 'pro'
    ? ['macos', 'ios', 'android', 'windows', 'linux']
    : ['macos'];
  const license: License = { key, tier, github_username: username, platforms, account_id: '', device_bound: deviceBound, project_bundle_id: '' };
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

  // Don't reject for concurrent builds — just queue them.
  // The worker dispatch handles queueing naturally.

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
    const wActive = workerActiveJobsMap.get(wKey) || 0;
    const wMaxConcurrent = workerMaxConcurrentMap.get(wKey) || 1;
    console.log('getAvailableWorkerIdx: ' + wKey + ' name=' + wName + ' active=' + String(wActive) + '/' + String(wMaxConcurrent));
    if (wActive < wMaxConcurrent) {
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

  workerActiveJobsMap.set(wKey, (workerActiveJobsMap.get(wKey) || 0) + 1);
  workerBusyMap.set(wKey, true);
  workerJobMap.set(wKey, job.id);
  job.status = 'running';
  dbBuildStarted(job.id, wName);

  const jobToken = crypto.randomUUID();
  jobTokens.set(job.id, jobToken);

  try {
    sendToClient(wHandle, JSON.stringify({
      type: 'job_assign',
      job_id: job.id,
      auth_token: jobToken,
      manifest: job.manifest,
      credentials: job.credentials,
      tarball_url: getPublicUrl() + '/api/v1/tarball/' + job.id,
      artifact_upload_url: getPublicUrl() + '/api/v1/artifact/upload/' + job.id,
    }));
  } catch (e) {
    jobTokens.delete(job.id);
    workerBusyMap.set(wKey, false);
    workerActiveJobsMap.set(wKey, 0);
    workerJobMap.set(wKey, '');
    job.status = 'queued';
    return false;
  }

  return true;
}

function tryDispatchNext(): boolean {
  const job = dequeueJob();
  if (job) {
    if (!dispatchJob(job)) {
      // Re-enqueue if no worker available
      job.status = 'queued';
      jobQueue.unshift(job.id);
      counters.queueLen++;
      return false;
    }
    return true;
  }
  return false;
}

/// Start the Azure Windows sign-only VM if configured.
/// Called when a finishing job is queued but no Windows worker is connected.
function startAzureSignVm(): void {
  const tenantId = process.env['AZURE_TENANT_ID'] || '';
  const clientId = process.env['AZURE_CLIENT_ID'] || '';
  const clientSecret = process.env['AZURE_CLIENT_SECRET'] || '';
  const subscriptionId = process.env['AZURE_SUBSCRIPTION_ID'] || '';
  const resourceGroup = process.env['AZURE_VM_RESOURCE_GROUP'] || '';
  const vmName = process.env['AZURE_VM_NAME'] || '';

  if (!tenantId || !clientId || !clientSecret || !subscriptionId || !resourceGroup || !vmName) {
    console.log('Azure VM config not set — cannot auto-start sign VM');
    return;
  }

  console.log('Starting Azure sign VM: ' + vmName);

  // Get OAuth token and start VM in the background via curl
  const tokenUrl = 'https://login.microsoftonline.com/' + tenantId + '/oauth2/v2.0/token';
  try {
    const tokenResult = child_process.execSync(
      'curl -s -X POST "' + tokenUrl + '"'
      + ' -d "grant_type=client_credentials'
      + '&client_id=' + clientId
      + '&client_secret=' + encodeURIComponent(clientSecret)
      + '&scope=https://management.azure.com/.default"',
      { timeout: 15000 }
    ).toString();

    const tokenMatch = tokenResult.match(/"access_token"\s*:\s*"([^"]+)"/);
    if (!tokenMatch) {
      console.error('Failed to get Azure token for VM start');
      return;
    }
    const token = tokenMatch[1];

    const startUrl = 'https://management.azure.com/subscriptions/' + subscriptionId
      + '/resourceGroups/' + resourceGroup
      + '/providers/Microsoft.Compute/virtualMachines/' + vmName
      + '/start?api-version=2024-07-01';

    child_process.exec(
      'curl -s -X POST "' + startUrl + '" -H "Authorization: Bearer ' + token + '" -H "Content-Length: 0"',
      { timeout: 15000 },
      (error: any, stdout: any, stderr: any) => {
        if (error) {
          console.error('Azure VM start failed: ' + (error.message || error));
        } else {
          console.log('Azure VM start request sent for ' + vmName);
        }
      }
    );
  } catch (e: any) {
    console.error('Azure VM start error: ' + (e.message || e));
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
  return '{"status":"ok","queue_length":' + String(counters.queueLen) + ',"perry_version":"0.1.0","expected_perry_version":"' + jsonEscape(perryExpected.version) + '","supported_targets":' + cached.targetsJson + ',"connected_workers":' + String(counters.workers) + '}';
});

// POST /api/v1/admin/update-perry — trigger perry compiler update on all workers
app.post('/api/v1/admin/update-perry', async (request: any, reply: any) => {
  reply.header('Content-Type', 'application/json');

  if (ADMIN_SECRET) {
    const auth = request.headers['authorization'] || '';
    if (!auth.endsWith(ADMIN_SECRET) || !auth.startsWith('Bearer ')) {
      reply.status(403);
      return JSON.stringify({ error: { code: 'FORBIDDEN', message: 'Admin authentication required' } });
    }
  }

  let body: any = request.body || {};
  // Optionally set the expected version so future workers auto-update
  if (body.expected_version) {
    perryExpected.version = body.expected_version;
    console.log('Updated expected perry version to ' + perryExpected.version);
  }

  let sent = 0;
  for (let wi = 0; wi < counters.workers; wi++) {
    const wKey = 'w' + String(wi);
    const wHandle = workerHandleMap.get(wKey) || 0;
    const wName = workerNameMap.get(wKey) || '?';
    const wBusy = workerBusyMap.get(wKey) || false;
    if (wBusy) {
      console.log('Skipping busy worker ' + wName + ' for perry update');
      continue;
    }
    try {
      sendToClient(wHandle, '{"type":"update_perry"}');
      console.log('Sent update_perry to worker ' + wName);
      sent++;
    } catch (e) {
      console.error('Failed to send update_perry to ' + wName + ': ' + String(e));
    }
  }

  // Build worker info for response
  let workersJson = '[';
  for (let wi = 0; wi < counters.workers; wi++) {
    const wKey = 'w' + String(wi);
    if (wi > 0) workersJson = workersJson + ',';
    workersJson = workersJson + '{"name":"' + jsonEscape(workerNameMap.get(wKey) || '?')
      + '","perry_version":"' + jsonEscape(workerVersionMap.get(wKey) || 'unknown')
      + '","busy":' + String(workerBusyMap.get(wKey) || false) + '}';
  }
  workersJson = workersJson + ']';

  return '{"ok":true,"workers_notified":' + String(sent) + ',"expected_version":"' + jsonEscape(perryExpected.version) + '","workers":' + workersJson + '}';
});

// GET /api/v1/admin/workers — read-only per-worker info (versions, capabilities, busy state)
app.get('/api/v1/admin/workers', async (request: any, reply: any) => {
  reply.header('Content-Type', 'application/json');

  if (ADMIN_SECRET) {
    const auth = request.headers['authorization'] || '';
    if (!auth.endsWith(ADMIN_SECRET) || !auth.startsWith('Bearer ')) {
      reply.status(403);
      return JSON.stringify({ error: { code: 'FORBIDDEN', message: 'Admin authentication required' } });
    }
  }

  let workersJson = '[';
  for (let wi = 0; wi < counters.workers; wi++) {
    const wKey = 'w' + String(wi);
    if (wi > 0) workersJson = workersJson + ',';
    workersJson = workersJson + '{"name":"' + jsonEscape(workerNameMap.get(wKey) || '?')
      + '","perry_version":"' + jsonEscape(workerVersionMap.get(wKey) || 'unknown')
      + '","capabilities":"' + jsonEscape(workerCapMap.get(wKey) || ',')
      + '","busy":' + String(workerBusyMap.get(wKey) || false) + '}';
  }
  workersJson = workersJson + ']';

  return '{"expected_version":"' + jsonEscape(perryExpected.version) + '","workers":' + workersJson + '}';
});

// POST /api/v1/license/register
// Public endpoint — free tier, device-bound licenses. No auth required.
app.post('/api/v1/license/register', async (request: any, reply: any) => {
  reply.header('Content-Type', 'application/json');

  let body: any = request.body || {};

  const username = body.github_username || '';
  const license = registerLicense(username, 'free', true);
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

// GET /api/v1/account — get account info and usage (requires Bearer token)
app.get('/api/v1/account', async (request: any, reply: any) => {
  reply.header('Content-Type', 'application/json');
  const auth = request.headers['authorization'] || '';
  if (!auth.startsWith('Bearer ')) {
    reply.status(401);
    return JSON.stringify({ error: { code: 'AUTH_REQUIRED', message: 'Authorization header required' } });
  }
  const token = auth.substring(7);
  const accountId = accountsByToken.get(token);
  if (!accountId) {
    reply.status(401);
    return JSON.stringify({ error: { code: 'AUTH_INVALID', message: 'Invalid API token' } });
  }
  const account = accounts.get(accountId);
  if (!account) {
    reply.status(401);
    return JSON.stringify({ error: { code: 'AUTH_INVALID', message: 'Account not found' } });
  }

  // Find licenses linked to this account
  let licenseKey = '';
  licenses.forEach((lic: License, key: string) => {
    if (lic.account_id === accountId && !licenseKey) {
      licenseKey = key;
    }
  });

  let usage = { publishes: 0, deep_verifies: 0 };
  if (licenseKey) {
    usage = await getUsage(licenseKey);
  }

  const limit = getPublishLimit(account.tier);
  const verifyLimit = account.tier === 'pro' ? 20 : 2;

  return JSON.stringify({
    id: account.id,
    github_username: account.github_username,
    email: account.email,
    tier: account.tier,
    has_payment_method: account.has_payment_method,
    usage: {
      publishes: usage.publishes,
      publish_limit: limit,
      deep_verifies: usage.deep_verifies,
      verify_limit: verifyLimit,
      period: getCurrentPeriod(),
    },
  });
});

// POST /api/v1/account/update — dashboard updates account fields (tier, polar IDs)
// Protected by ADMIN_SECRET (called by dashboard server, not directly by users)
app.post('/api/v1/account/update', async (request: any, reply: any) => {
  reply.header('Content-Type', 'application/json');
  if (ADMIN_SECRET) {
    const auth = request.headers['authorization'] || '';
    if (!auth.endsWith(ADMIN_SECRET) || !auth.startsWith('Bearer ')) {
      reply.status(403);
      return JSON.stringify({ error: { code: 'FORBIDDEN', message: 'Admin authentication required' } });
    }
  }
  const body: any = request.body;
  if (!body || !body.account_id) {
    reply.status(400);
    return JSON.stringify({ error: { code: 'BAD_REQUEST', message: 'account_id required' } });
  }
  const account = accounts.get(body.account_id);
  if (!account) {
    reply.status(404);
    return JSON.stringify({ error: { code: 'NOT_FOUND', message: 'Account not found' } });
  }
  if (body.tier) account.tier = body.tier;
  if (body.polar_customer_id) account.polar_customer_id = body.polar_customer_id;
  if (body.polar_subscription_id) account.polar_subscription_id = body.polar_subscription_id;
  if (body.has_payment_method !== undefined) account.has_payment_method = body.has_payment_method ? true : false;
  await dbSaveAccount(account);

  // Also update the tier on linked licenses
  licenses.forEach((lic: License, key: string) => {
    if (lic.account_id === body.account_id) {
      lic.tier = account.tier;
      if (account.tier === 'pro') {
        lic.platforms = ['macos', 'ios', 'android', 'windows', 'linux'];
      }
      dbSaveLicense(lic);
    }
  });

  return JSON.stringify({ ok: true });
});

// POST /api/v1/account/create — dashboard creates account after GitHub OAuth
// Protected by ADMIN_SECRET
app.post('/api/v1/account/create', async (request: any, reply: any) => {
  reply.header('Content-Type', 'application/json');
  if (ADMIN_SECRET) {
    const auth = request.headers['authorization'] || '';
    const expected = 'Bearer ' + ADMIN_SECRET;
    console.log('account/create auth check: auth_len=' + String(auth.length) + ' expected_len=' + String(expected.length) + ' match=' + String(auth === expected));
    console.log('account/create auth="' + auth.substring(0, 20) + '..." expected="' + expected.substring(0, 20) + '..."');
    if (auth !== expected) {
      reply.status(403);
      return JSON.stringify({ error: { code: 'FORBIDDEN', message: 'Admin authentication required' } });
    }
  }
  const body: any = request.body;
  if (!body || !body.github_id || !body.github_username) {
    reply.status(400);
    return JSON.stringify({ error: { code: 'BAD_REQUEST', message: 'github_id and github_username required' } });
  }

  // Check for existing account by github_id
  let existingAccount: Account | null = null;
  accounts.forEach((acc: Account) => {
    if (acc.github_id === body.github_id && !existingAccount) {
      existingAccount = acc;
    }
  });

  if (existingAccount) {
    // Return existing account
    return JSON.stringify({
      id: (existingAccount as Account).id,
      api_token: (existingAccount as Account).api_token,
      github_username: (existingAccount as Account).github_username,
      tier: (existingAccount as Account).tier,
      created: false,
    });
  }

  const id = crypto.randomUUID();
  const apiToken = crypto.randomUUID();
  const account: Account = {
    id,
    github_username: body.github_username,
    github_id: body.github_id,
    email: body.email || '',
    tier: 'free',
    polar_customer_id: '',
    polar_subscription_id: '',
    api_token: apiToken,
    has_payment_method: false,
    created_at: Date.now(),
  };
  accounts.set(id, account);
  accountsByToken.set(apiToken, id);
  await dbSaveAccount(account);

  return JSON.stringify({
    id: account.id,
    api_token: account.api_token,
    github_username: account.github_username,
    tier: account.tier,
    created: true,
  });
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

  // Parse multipart body (request.rawBody = raw body string from perry runtime)
  const rawBody = request.rawBody;
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

  if (!manifestPart) {
    reply.status(400);
    return JSON.stringify({ error: { code: 'BAD_REQUEST', message: "Missing 'manifest' field" } });
  }
  if (!tarballB64Part) {
    reply.status(400);
    return JSON.stringify({ error: { code: 'BAD_REQUEST', message: "Missing 'tarball_b64' field" } });
  }

  let manifest: any;
  try {
    manifest = JSON.parse(manifestPart.data);
  } catch (e: any) {
    reply.status(400);
    return JSON.stringify({ error: { code: 'BAD_REQUEST', message: 'Invalid manifest JSON' } });
  }

  let credentials: any = {};
  if (credentialsPart) {
    try {
      credentials = JSON.parse(credentialsPart.data);
    } catch (e: any) {
      reply.status(400);
      return JSON.stringify({ error: { code: 'BAD_REQUEST', message: 'Invalid credentials JSON' } });
    }
  }

  // --- Authenticate: API token (Bearer header) or license key (multipart field) ---
  let license: License | null = null;
  const authHeader = request.headers['authorization'] || '';

  if (authHeader.startsWith('Bearer ')) {
    // API token auth — look up account, find or create license
    const token = authHeader.substring(7);
    const accountId = accountsByToken.get(token);
    if (!accountId) {
      reply.status(401);
      return JSON.stringify({ error: { code: 'AUTH_INVALID', message: 'Invalid API token' } });
    }
    const account = accounts.get(accountId);
    if (!account) {
      reply.status(401);
      return JSON.stringify({ error: { code: 'AUTH_INVALID', message: 'Account not found' } });
    }

    // Find existing license linked to this account, or create one
    let foundKey = '';
    licenses.forEach((lic: License, key: string) => {
      if (lic.account_id === accountId && !foundKey) {
        foundKey = key;
      }
    });
    if (foundKey) {
      license = licenses.get(foundKey) || null;
    }
    if (!license) {
      // Create a license linked to this account
      const newLic = registerLicense(account.github_username, account.tier, false);
      newLic.account_id = accountId;
      license = newLic;
      licenses.set(newLic.key, newLic);
      dbSaveLicense(newLic);
    }
    // Sync tier from account (account tier is authoritative for logged-in users)
    if (license.tier !== account.tier) {
      license.tier = account.tier;
      dbSaveLicense(license);
    }
  } else {
    // License key auth (legacy / device-bound flow)
    if (!licensePart) {
      reply.status(400);
      return JSON.stringify({ error: { code: 'BAD_REQUEST', message: "Missing 'license_key' field or Authorization header" } });
    }
    const licenseKey = licensePart.data;
    license = verifyLicense(licenseKey);
    if (!license) {
      reply.status(401);
      return JSON.stringify({ error: { code: 'LICENSE_INVALID', message: 'Invalid license key' } });
    }
  }

  // --- Device-bound project enforcement ---
  if (license.device_bound && !license.account_id) {
    const bundleId = manifest.bundle_id || manifest.name || '';
    if (bundleId) {
      if (!license.project_bundle_id) {
        // First build — bind to this project
        license.project_bundle_id = bundleId;
        dbSaveLicense(license);
      } else if (license.project_bundle_id !== bundleId) {
        // Different project — require account
        reply.status(403);
        return JSON.stringify({
          error: {
            code: 'ACCOUNT_REQUIRED',
            message: 'Free device licenses support one project. Run \'perry login\' to create an account and publish multiple projects.',
            login_url: 'https://app.perryts.com/cli/authorize',
          },
        });
      }
    }
  }

  // --- Usage enforcement (skip for self-hosted) ---
  if (!isSelfHosted()) {
    const usage = await getUsage(license.key);
    const limit = getPublishLimit(license.tier);

    if (usage.publishes >= limit) {
      // Pro users with Polar billing can exceed (overage billed)
      if (license.tier === 'pro' && license.account_id) {
        // Allow — overage will be reported on build completion
      } else {
        // Hard block for free tier
        reply.status(429);
        return JSON.stringify({
          error: {
            code: 'PUBLISH_LIMIT_REACHED',
            message: 'You\'ve used ' + String(usage.publishes) + '/' + String(limit) + ' free publishes this month. Resets on the 1st. Run \'perry login\' to upgrade to Pro ($19/mo, 50 publishes).',
            usage: { publishes: usage.publishes, limit: limit, period: getCurrentPeriod() },
          },
        });
      }
    }
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
    retries: 0,
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
    // Serve base64 version — Perry runtime can't reliably send raw binary
    const b64Path = entry.path + '.b64';
    let responseData: any;
    if (fs.existsSync(b64Path)) {
      responseData = fs.readFileSync(b64Path);
      reply.header('Content-Type', 'text/plain');
    } else {
      responseData = fs.readFileSync(entry.path);
      reply.header('Content-Type', 'application/octet-stream');
    }
    const safeName = entry.name.replace(/[^a-zA-Z0-9._-]/g, '_');
    reply.header('Content-Disposition', `attachment; filename="${safeName}"`);
    return responseData;
  } catch (e: any) {
    reply.status(500);
    reply.header('Content-Type', 'application/json');
    return JSON.stringify({ error: { code: 'INTERNAL_ERROR', message: 'Failed to read artifact: ' + (e.message || e) } });
  }
});

// GET /api/v1/tarball/:jobId — workers download the base64-encoded tarball for a job
app.get('/api/v1/tarball/:jobId', async (request: any, reply: any) => {
  const jobId = request.params.jobId;
  const expectedToken = jobTokens.get(jobId) || '';
  const auth = request.headers['authorization'] || '';
  if (!expectedToken || !auth.endsWith(expectedToken) || !auth.startsWith('Bearer ')) {
    reply.status(403);
    reply.header('Content-Type', 'application/json');
    return JSON.stringify({ error: { code: 'FORBIDDEN', message: 'Invalid job token' } });
  }
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
  const expectedToken = jobTokens.get(jobId) || '';
  const authHdr = request.headers['authorization'] || '';
  if (!expectedToken || !authHdr.endsWith(expectedToken) || !authHdr.startsWith('Bearer ')) {
    reply.status(403);
    return JSON.stringify({ error: { code: 'FORBIDDEN', message: 'Invalid job token' } });
  }
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

  const rawBody = request.rawBody;
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
  // Save base64 for verification submission
  fs.writeFileSync(artifactPath + '.b64', b64Data);
  const size = buffer.length;

  // Register artifact with download token
  const token = registerArtifact(artifactPath, artifactName, sha256, size);
  const downloadUrl = getPublicUrl() + '/api/v1/dl/' + token;

  // For windows-precompiled artifacts, don't notify CLI — this is an intermediate
  // artifact that will be sent to the Windows sign-only worker for finishing.
  if (target !== 'windows-precompiled') {
    const artJson = '{"type":"artifact_ready","job_id":"' + jsonEscape(jobId) + '","target":"' + jsonEscape(target) + '","artifact_name":"' + jsonEscape(artifactName) + '","artifact_size":' + String(size) + ',"sha256":"' + jsonEscape(sha256) + '","download_url":"' + jsonEscape(downloadUrl) + '","expires_in_secs":' + String(ARTIFACT_TTL_SECS) + '}';
    relayToCliClientsJson(jobId, artJson);
  } else {
    console.log('Intermediate windows-precompiled artifact for job ' + jobId + ' — holding for sign-only worker');
  }

  // Track artifact path for verification
  verifyArtifactPathMap.set(jobId, artifactPath);

  console.log('Artifact uploaded for job ' + jobId + ': ' + artifactName + ' (' + String(size) + ' bytes)');

  return JSON.stringify({ token: token, download_url: downloadUrl, size: size });
});

// --- Worker secret check ---
// Extracted as a top-level function because calling sendToClient/closeClient
// inside nested conditionals within wss.on('message') triggers a perry
// codegen bug that prevents the WebSocket server from binding its port.
function rejectWorkerAuth(handle: any): void {
  sendToClient(handle, JSON.stringify({ type: 'error', code: 'AUTH_FAILED', message: 'Invalid worker secret' }));
  closeClient(handle);
}

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
  try {
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
      if (WORKER_SECRET && msg.secret !== WORKER_SECRET) {
        rejectWorkerAuth(clientHandle);
        return;
      }
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
      workerActiveJobsMap.set(wKey, 0);
    workerMaxConcurrentMap.set(wKey, msg.max_concurrent || 1);
      workerBusyMap.set(wKey, (workerActiveJobsMap.get(wKey) || 0) > 0);
      workerJobMap.set(wKey, '');
      const workerVersion = msg.perry_version || '';
      workerVersionMap.set(wKey, workerVersion);
      setClientWorkerIdx(clientHandle, counters.workers);
      counters.workers++;
      rebuildTargetsJson();
      console.log('Worker connected: ' + workerName + ' caps=' + capStr + ' perry=' + workerVersion);

      // Auto-update if worker is behind expected version
      if (perryExpected.version && workerVersion && workerVersion !== perryExpected.version) {
        console.log('Worker ' + workerName + ' has perry ' + workerVersion + ', expected ' + perryExpected.version + ' — sending update_perry');
        try {
          sendToClient(clientHandle, '{"type":"update_perry"}');
        } catch (e) {
          // ignore send failure
        }
      }

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
  } catch (e: any) {
    console.error('WebSocket message handler error:', e);
  }
});

// Close event: server-level, receives (clientHandle)
wss.on('close', (clientHandle: any) => {
  try {
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
          jobTokens.delete(wJobId);
          const retries = job.retries || 0;
          if (retries < MAX_JOB_RETRIES) {
            // Re-queue — another worker can pick it up
            job.status = 'queued';
            job.retries = retries + 1;
            jobQueue.unshift(job.id);
            counters.queueLen++;
            console.log('Re-queued job ' + job.id + ' (worker ' + wName + ' disconnected, retry ' + String(job.retries) + '/' + String(MAX_JOB_RETRIES) + ')');
            const requeueJson = '{"type":"queue_update","job_id":"' + job.id + '","position":1,"message":"Builder disconnected, re-queued (attempt ' + String(job.retries + 1) + ')"}';
            relayToCliClientsJson(job.id, requeueJson);
          } else {
            // Exhausted retries — fail permanently
            job.status = 'failed';
            releaseRateLimit(job.license_key);
            console.log('Job ' + job.id + ' failed after ' + String(MAX_JOB_RETRIES) + ' retries');
            const errJson = '{"type":"error","code":"INTERNAL_ERROR","message":"Build failed after ' + String(MAX_JOB_RETRIES) + ' attempts (workers kept disconnecting)"}';
            relayToCliClientsJson(job.id, errJson);
            const dur = (Date.now() - job.created_at) / 1000;
            const completeJson = '{"type":"complete","job_id":"' + job.id + '","success":false,"duration_secs":' + String(dur) + ',"artifacts":[]}';
            relayToCliClientsJson(job.id, completeJson);
          }
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
        workerVersionMap.set(curKey, workerVersionMap.get(nextKey) || '');
        setClientWorkerIdx(nextHandle, wi);
      }
      const lastKey = 'w' + String(counters.workers);
      workerCapMap.delete(lastKey);
      workerNameMap.delete(lastKey);
      workerHandleMap.delete(lastKey);
      workerBusyMap.delete(lastKey);
      workerJobMap.delete(lastKey);
      workerVersionMap.delete(lastKey);
      rebuildTargetsJson();
      tryDispatchNext();
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
  } catch (e: any) {
    console.error('WebSocket close handler error:', e);
  }
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

    // Validate path is within ARTIFACT_DIR to prevent path traversal
    const artPath = String(msg.path || '');
    if (!artPath.startsWith(ARTIFACT_DIR + '/')) {
      console.error('artifact_ready: rejected path traversal attempt: ' + artPath);
      return;
    }

    const token = registerArtifact(
      artPath,
      msg.artifact_name,
      msg.sha256,
      msg.size,
    );
    const downloadUrl = getPublicUrl() + '/api/v1/dl/' + token;
    const target = msg.target || '';

    const artJson = '{"type":"artifact_ready","job_id":"' + jsonEscape(jobId) + '","target":"' + jsonEscape(target) + '","artifact_name":"' + jsonEscape(msg.artifact_name) + '","artifact_size":' + String(msg.size) + ',"sha256":"' + jsonEscape(msg.sha256) + '","download_url":"' + jsonEscape(downloadUrl) + '","expires_in_secs":' + String(ARTIFACT_TTL_SECS) + '}';
    relayToCliClientsJson(jobId, artJson);
  } else if (msgType === 'build_complete') {
    // Worker finished building but has a distribution step waiting for verification
    if (!jobId) return;
    const hasDistribution = msg.has_distribution === true;

    if (hasDistribution) {
      // Start verification before allowing distribution
      verifyWorkerIdxMap.set(jobId, wIdx);
      verifyStartTimeMap.set(jobId, Date.now());
      console.log('Build complete for job ' + jobId + ', starting verification before distribution');

      // Notify CLI that verification is starting
      const verifyStageJson = '{"type":"stage","job_id":"' + jsonEscape(jobId) + '","stage":"verifying","message":"Verifying binary before distribution..."}';
      relayToCliClientsJson(jobId, verifyStageJson);

      // Submit to perry-verify
      startVerification(jobId, wIdx);
    } else {
      // No distribution — treat as complete (shouldn't normally happen, but handle gracefully)
      const job = jobs.get(jobId);
      if (job) {
        job.status = 'completed';
        releaseRateLimit(job.license_key);
        const durationSecs = Math.round((Date.now() - job.created_at) / 1000);
        dbBuildFinished(jobId, true, durationSecs, '');
        jobTokens.delete(jobId);
        try { fs.unlinkSync(TARBALL_DIR + '/' + jobId + '.b64'); } catch (e) { /* ignore */ }
      }
      const completeJson = '{"type":"complete","job_id":"' + jsonEscape(jobId) + '","success":true,"duration_secs":' + String(msg.duration_secs || 0) + ',"artifacts":[]}';
      relayToCliClientsJson(jobId, completeJson);
      workerActiveJobsMap.set(wKey, Math.max(0, (workerActiveJobsMap.get(wKey) || 1) - 1));
      workerBusyMap.set(wKey, (workerActiveJobsMap.get(wKey) || 0) > 0);
      workerJobMap.set(wKey, '');
      tryDispatchNext();
    }
  } else if (msgType === 'complete') {
    if (!jobId) return;

    const job = jobs.get(jobId);

    // Check if this job needs finishing by another worker (e.g. Linux cross-compiled
    // a Windows exe and now the Windows sign-only worker needs to embed resources,
    // sign, and package it).
    if (msg.success && msg.needs_finishing && job && job.status === 'running') {
      const finishTarget = msg.needs_finishing; // e.g. "windows"
      console.log('Job ' + jobId + ' needs finishing by ' + finishTarget + ' worker');

      // Free the current worker (Linux) so it can take new jobs
      workerActiveJobsMap.set(wKey, Math.max(0, (workerActiveJobsMap.get(wKey) || 1) - 1));
      workerBusyMap.set(wKey, (workerActiveJobsMap.get(wKey) || 0) > 0);
      workerJobMap.set(wKey, '');

      // Replace the original tarball with the precompiled artifact bundle
      // so the finishing worker downloads the precompiled bundle, not the source
      const precompiledPath = verifyArtifactPathMap.get(jobId);
      if (precompiledPath) {
        const precompiledB64Path = precompiledPath + '.b64';
        const tarballB64Path = TARBALL_DIR + '/' + jobId + '.b64';
        try {
          // Read the precompiled artifact's base64 and write it as the job tarball
          if (fs.existsSync(precompiledB64Path)) {
            const b64Data = fs.readFileSync(precompiledB64Path);
            fs.writeFileSync(tarballB64Path, b64Data);
          } else if (fs.existsSync(precompiledPath)) {
            // Base64 encode the binary artifact
            const binData = fs.readFileSync(precompiledPath);
            fs.writeFileSync(tarballB64Path, binData.toString('base64'));
          }
        } catch (e: any) {
          console.error('Failed to prepare precompiled bundle for finishing: ' + (e.message || e));
        }
      }

      // Clean up the old job token — a new one will be generated on re-dispatch
      jobTokens.delete(jobId);

      // Notify CLI that signing is starting
      const stageJson = '{"type":"stage","job_id":"' + jsonEscape(jobId) + '","stage":"signing","message":"Routing to platform worker for signing and packaging..."}';
      relayToCliClientsJson(jobId, stageJson);

      // Re-queue the job for the finishing worker. Override targets to route to
      // the correct worker (e.g. a worker advertising "windows" capability).
      job.status = 'queued';
      job.manifest.targets = [finishTarget + '-sign'];
      jobQueue.unshift(job.id);
      counters.queueLen++;

      // Clean up verification state from this stage
      verifyJobIdMap.delete(jobId);
      verifyWorkerIdxMap.delete(jobId);
      verifyArtifactPathMap.delete(jobId);
      verifyStartTimeMap.delete(jobId);

      // If no worker with the finishing capability is connected, start the
      // Azure VM so it boots and connects. The job stays queued until then.
      if (!tryDispatchNext()) {
        startAzureSignVm();
      }
      return;
    }

    if (job) {
      if (msg.success) {
        job.status = 'completed';
      } else {
        job.status = 'failed';
      }
      releaseRateLimit(job.license_key);
      const durationSecs = Math.round((Date.now() - job.created_at) / 1000);
      dbBuildFinished(jobId, msg.success, durationSecs, msg.error || '');
      // Clean up tarball and job token now that the worker is done
      jobTokens.delete(jobId);
      try { fs.unlinkSync(TARBALL_DIR + '/' + jobId + '.b64'); } catch (e) { /* ignore */ }
    }

    // Clean up verification state
    verifyJobIdMap.delete(jobId);
    verifyWorkerIdxMap.delete(jobId);
    verifyArtifactPathMap.delete(jobId);
    verifyStartTimeMap.delete(jobId);

    const completeJson = JSON.stringify(msg);
    relayToCliClientsJson(jobId, completeJson);

    workerBusyMap.set(wKey, false);
    workerJobMap.set(wKey, '');

    tryDispatchNext();
  } else if (msgType === 'update_result') {
    const wName = workerNameMap.get(wKey) || '?';
    if (msg.success) {
      workerVersionMap.set(wKey, msg.new_version || '');
      console.log('Worker ' + wName + ' updated perry: ' + (msg.old_version || '?') + ' -> ' + (msg.new_version || '?'));
    } else {
      console.error('Worker ' + wName + ' perry update failed: ' + (msg.error || 'unknown error'));
    }
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

// --- Verification flow ---
// Submit artifact to perry-verify and poll for results.
// Uses execSync('curl ...') for localhost requests (fast, <100ms each).
// Polling via setTimeout to avoid blocking the event loop continuously.

function startVerification(buildJobId: string, wIdx: number): void {
  const artifactPath = verifyArtifactPathMap.get(buildJobId) || '';
  if (!artifactPath) {
    console.error('No artifact path for verification of job ' + buildJobId);
    sendVerifyResult(buildJobId, wIdx, false, 'No artifact available for verification');
    return;
  }

  const b64Path = artifactPath + '.b64';
  try {
    fs.statSync(b64Path);
  } catch (e) {
    console.error('No .b64 file for verification of job ' + buildJobId);
    sendVerifyResult(buildJobId, wIdx, false, 'No base64 artifact for verification');
    return;
  }

  // Determine verify target from job manifest
  const job = jobs.get(buildJobId);
  let verifyTarget = 'macos-arm64';
  if (job) {
    const targets = job.manifest.targets || [];
    for (let ti = 0; ti < targets.length; ti++) {
      const t = targets[ti].toLowerCase();
      if (t === 'ios') {
        verifyTarget = 'ios-simulator';
        break;
      } else if (t === 'android') {
        verifyTarget = 'android-emulator';
        break;
      } else if (t === 'macos') {
        verifyTarget = 'macos-arm64';
        break;
      }
    }
  }

  // Submit to perry-verify using curl (localhost, fast)
  try {
    const curlCmd = 'curl -s -X POST ' + VERIFY_URL + '/verify'
      + ' -F "binary_b64=@' + b64Path + '"'
      + ' -F "target=' + verifyTarget + '"'
      + " -F 'config={\"auth\":{\"strategy\":\"none\"}}'"
      + " -F 'manifest={\"appType\":\"gui\",\"hasAuthGate\":false,\"entryScreen\":\"main\"}'";
    const result = child_process.execSync(curlCmd, { timeout: 30000 }).toString().trim();

    // Parse verify job ID from response
    // Response format: {"id":"<verifyJobId>","status":"queued",...}
    let verifyId = '';
    const idMatch = result.match(/"id"\s*:\s*"([^"]+)"/);
    if (idMatch) {
      verifyId = idMatch[1];
    }

    if (!verifyId) {
      console.error('Failed to parse verify job ID from response: ' + result);
      sendVerifyResult(buildJobId, wIdx, false, 'Failed to submit for verification');
      return;
    }

    verifyJobIdMap.set(buildJobId, verifyId);
    console.log('Verification submitted for job ' + buildJobId + ' -> verify job ' + verifyId);

    // Clean up .b64 file now that it's been submitted
    try { fs.unlinkSync(b64Path); } catch (e) { /* ignore */ }

    // Start polling
    setTimeout(function pollTick() { pollVerification(buildJobId); }, VERIFY_POLL_INTERVAL_MS);
  } catch (e: any) {
    console.error('Verification submission failed for job ' + buildJobId + ': ' + String(e));
    // Clean up .b64 file
    try { fs.unlinkSync(b64Path); } catch (e2) { /* ignore */ }
    sendVerifyResult(buildJobId, wIdx, false, 'Verification service unavailable');
  }
}

function pollVerification(buildJobId: string): void {
  const verifyId = verifyJobIdMap.get(buildJobId) || '';
  const wIdx = verifyWorkerIdxMap.get(buildJobId);
  if (!verifyId || wIdx === undefined) return; // cleaned up, nothing to do

  // Check timeout
  const startTime = verifyStartTimeMap.get(buildJobId) || 0;
  if (Date.now() - startTime > VERIFY_TIMEOUT_MS) {
    console.error('Verification timed out for job ' + buildJobId);
    sendVerifyResult(buildJobId, wIdx, false, 'Verification timed out');
    return;
  }

  try {
    const curlCmd = 'curl -s ' + VERIFY_URL + '/verify/' + verifyId;
    const result = child_process.execSync(curlCmd, { timeout: 10000 }).toString().trim();

    // Parse status from response
    // Response format: {"id":"...","status":"completed","result":"passed",...}
    let status = '';
    let verifyResult = '';
    const statusMatch = result.match(/"status"\s*:\s*"([^"]+)"/);
    if (statusMatch) status = statusMatch[1];
    const resultMatch = result.match(/"result"\s*:\s*"([^"]+)"/);
    if (resultMatch) verifyResult = resultMatch[1];

    if (status === 'completed' || status === 'failed') {
      const passed = verifyResult === 'passed';
      console.log('Verification ' + (passed ? 'passed' : 'failed') + ' for job ' + buildJobId);

      // Forward verification progress to CLI
      const pctJson = '{"type":"progress","job_id":"' + jsonEscape(buildJobId) + '","stage":"verifying","percent":100,"message":"Verification ' + (passed ? 'passed' : 'failed') + '"}';
      relayToCliClientsJson(buildJobId, pctJson);

      if (passed) {
        sendVerifyResult(buildJobId, wIdx, true, '');
      } else {
        // Extract failure reason
        let failReason = 'Verification failed';
        const msgMatch = result.match(/"message"\s*:\s*"([^"]+)"/);
        if (msgMatch) failReason = msgMatch[1];
        sendVerifyResult(buildJobId, wIdx, false, failReason);
      }
    } else {
      // Still running — forward progress and poll again
      const pctJson = '{"type":"progress","job_id":"' + jsonEscape(buildJobId) + '","stage":"verifying","percent":50,"message":"Verifying binary..."}';
      relayToCliClientsJson(buildJobId, pctJson);

      setTimeout(function pollAgain() { pollVerification(buildJobId); }, VERIFY_POLL_INTERVAL_MS);
    }
  } catch (e: any) {
    console.error('Verification poll failed for job ' + buildJobId + ': ' + String(e));
    // Don't give up on transient errors — retry
    setTimeout(function pollRetry() { pollVerification(buildJobId); }, VERIFY_POLL_INTERVAL_MS);
  }
}

function sendVerifyResult(buildJobId: string, wIdx: number, passed: boolean, reason: string): void {
  const wKey = 'w' + String(wIdx);
  const wHandle = workerHandleMap.get(wKey);
  if (!wHandle) {
    console.error('Worker ' + wKey + ' not found for verify result');
    return;
  }

  if (passed) {
    const msg = '{"type":"proceed_distribute","job_id":"' + jsonEscape(buildJobId) + '"}';
    try {
      sendToClient(wHandle, msg);
      console.log('Sent proceed_distribute to worker for job ' + buildJobId);
    } catch (e) {
      console.error('Failed to send proceed_distribute: ' + String(e));
    }
  } else {
    const msg = '{"type":"skip_distribute","job_id":"' + jsonEscape(buildJobId) + '","reason":"' + jsonEscape(reason) + '"}';
    try {
      sendToClient(wHandle, msg);
      console.log('Sent skip_distribute to worker for job ' + buildJobId + ': ' + reason);
    } catch (e) {
      console.error('Failed to send skip_distribute: ' + String(e));
    }

    // Notify CLI about verification failure
    const errJson = '{"type":"error","job_id":"' + jsonEscape(buildJobId) + '","code":"VERIFY_FAILED","message":"' + jsonEscape(reason) + '"}';
    relayToCliClientsJson(buildJobId, errJson);
  }

  // Clean up verification state
  verifyJobIdMap.delete(buildJobId);
  verifyWorkerIdxMap.delete(buildJobId);
  verifyStartTimeMap.delete(buildJobId);
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
