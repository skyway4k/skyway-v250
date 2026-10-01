#!/usr/bin/env node
'use strict';
const http = require('http');
const fs = require('fs');
const path = require('path');
const url = require('url');
const { WebSocketServer } = require('ws');
const { Pool } = require('pg');

// ============================================================================================
// SKYWAY v250 — rebuilt from the v249 baseline after a $580 FlightAware bill.
//
// What changed vs v249, in one paragraph: FlightAware is gone entirely (it's what ran up the
// bill — no daily cap, a hardcoded fallback API key baked into the source). Free sources
// (FAA SWIM SCDS + OpenSky) now do the real work; AeroDataBox is optional, off unless
// ADB_ENABLED=1, and hard-capped monthly. Every secret v249 had a hardcoded fallback for
// (OSKY_SECRET, SWIM_PASS, and the old FA_KEY) is now env-var-only — the server refuses to
// boot rather than silently running on a bundled credential. pax/spot/flags/tow-notes — which
// in v249 either lived only in this browser's localStorage (spot) or didn't exist server-side
// at all (pax was a dead input; flags/tow-notes weren't editable) — are now real rows in
// Postgres, shared across /dispatch, /line-room, and /arrivals.
// ============================================================================================

function log(m, l = 'INFO') {
  const t = new Date().toLocaleTimeString('en-US', { hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: false });
  const c = { INFO: '\x1b[37m', OK: '\x1b[32m', WARN: '\x1b[33m', ERR: '\x1b[31m', MSG: '\x1b[36m' };
  console.log(`${c[l] || c.INFO}[${t}] [${l}]\x1b[0m ${m}`);
}

// ------------------------------------------------------------------------------------------
// BOOT SAFEGUARD #1 — refuse to boot without env vars set. No hardcoded fallback credentials
// anywhere in this file. This is the direct fix for the failure mode that caused the $580
// FlightAware bill: v249 had `const FA_KEY = process.env.FA_KEY || '<hardcoded-key>'`
// (and the same pattern for OSKY_SECRET and SWIM_PASS) — a bundled secret that worked even
// when nobody configured anything, so a leaked/scraped copy of the source could run up charges
// on YOUR account with no one noticing. If a required var is missing, we print exactly which
// one(s) and exit(1) instead of limping along on a default.
// ------------------------------------------------------------------------------------------
// SWIM is optional until the FAA SWIFT account is restored. Set SWIM_ENABLED=1 plus
// SWIM_USER/SWIM_PASS/SWIM_QUEUE to turn the Solace feed on. Boards still run on OpenSky
// /flights/*; the map uses adsb.lol (with OpenSky fallback).
const SWIM_ENABLED = process.env.SWIM_ENABLED === '1';
const REQUIRED_ENV = ['OSKY_ID', 'OSKY_SECRET', 'DATABASE_URL'];
if (SWIM_ENABLED) REQUIRED_ENV.push('SWIM_USER', 'SWIM_PASS', 'SWIM_QUEUE');
const missing = REQUIRED_ENV.filter(k => !process.env[k] || !String(process.env[k]).trim());
if (missing.length) {
  log('REFUSING TO BOOT — missing required environment variable(s): ' + missing.join(', '), 'ERR');
  log('Set these in your host\'s dashboard (or a local .env) and restart. See .env.example.', 'ERR');
  log('OpenSky (OSKY_ID/OSKY_SECRET) is required. SWIM is optional — set SWIM_ENABLED=1 to require SWIM_USER/SWIM_PASS/SWIM_QUEUE.', 'ERR');
  process.exit(1);
}
// ADB (AeroDataBox) is optional and off by default — see "ONLY if SWIM has real gaps" below.
if (process.env.ADB_ENABLED === '1' && !process.env.ADB_KEY) {
  log('REFUSING TO BOOT — ADB_ENABLED=1 but ADB_KEY is not set.', 'ERR');
  process.exit(1);
}

const PORT = parseInt(process.env.PORT || '8766', 10);
const OSKY_ID = process.env.OSKY_ID;
const OSKY_SECRET = process.env.OSKY_SECRET;
const TOKEN_URL = 'https://auth.opensky-network.org/auth/realms/opensky-network/protocol/openid-connect/token';

const SWIM_USER = process.env.SWIM_USER || '';
const SWIM_PASS = process.env.SWIM_PASS || '';
const SWIM_QUEUE = process.env.SWIM_QUEUE || '';
const SWIM_URL = process.env.SWIM_URL || 'tcps://ems2.swim.faa.gov:55443';
const SWIM_VPN = process.env.SWIM_VPN || 'TFMS';
// Optional second SFDPS/FDPS consumer (separate SWIFT subscription/queue). When unset, primary
// TFMS queue alone is used. Portal: https://swim.faa.gov → SWIFT → request SFDPS (or FDPS)
// pub/sub on the same or additional VPN; set SWIM_QUEUE_SFDPS (+ optional URL/VPN overrides).
const SWIM_QUEUE_SFDPS = process.env.SWIM_QUEUE_SFDPS || '';
const SWIM_URL_SFDPS = process.env.SWIM_URL_SFDPS || SWIM_URL;
const SWIM_VPN_SFDPS = process.env.SWIM_VPN_SFDPS || 'FDPS';
const LADD_URL = process.env.LADD_URL || '';
const LADD_FILE = process.env.LADD_FILE || '';

const ADB_ENABLED = process.env.ADB_ENABLED === '1';
const ADB_KEY = process.env.ADB_KEY || '';
const ADB_HOST = process.env.ADB_HOST || 'aerodatabox.p.rapidapi.com';
const ADB_MONTHLY_UNIT_BUDGET = parseInt(process.env.ADB_MONTHLY_UNIT_BUDGET || '600', 10);

const AIRPORT_ICAO = process.env.AIRPORT_ICAO || 'KSFO';
const IDLE_PAUSE_MINUTES = parseInt(process.env.IDLE_PAUSE_MINUTES || '10', 10);
// Daily hard cap on OpenSky's schedule-style /flights/* endpoints (arrival/departure lookups).
// Distinct from the credit-cost table below, which is specific to /states/all's bbox pricing —
// OpenSky doesn't publish a per-call credit cost for /flights/*, so this is a conservative,
// independent safety net rather than a precise budget.
const OSKY_FLIGHTS_DAILY_CAP = parseInt(process.env.OSKY_FLIGHTS_DAILY_CAP || '200', 10);
const ADSB_PRIMARY = (process.env.ADSB_PRIMARY || 'adsb.lol').toLowerCase();

const pool = new Pool({ connectionString: process.env.DATABASE_URL, ssl: process.env.DATABASE_URL.includes('localhost') ? false : { rejectUnauthorized: false } });

let wsClients = new Set();
let lastClientSeenAt = Date.now(); // updated on every WS connect + every /api or /osky request
function touchActivity() { lastClientSeenAt = Date.now(); }
function isIdle() {
  if (wsClients.size > 0) return false;
  return (Date.now() - lastClientSeenAt) > IDLE_PAUSE_MINUTES * 60000;
}

// ============================================================================================
// POSTGRES — shared ramp state (pax / spot / flags / tow notes) across all three views, plus a
// small durable usage-counter table so the AeroDataBox monthly cap survives restarts (Render's
// free tier sleeps and respawns the process — an in-memory-only counter would reset for free
// every time it woke up, quietly defeating the whole point of a hard cap).
// ============================================================================================
async function initSchema() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS ramp_state (
      id TEXT PRIMARY KEY,
      pax INTEGER,
      spot TEXT,
      flags JSONB NOT NULL DEFAULT '[]'::jsonb,
      tow_notes TEXT,
      updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
      updated_by TEXT
    );
    CREATE TABLE IF NOT EXISTS usage_counters (
      source TEXT NOT NULL,
      period_key TEXT NOT NULL,
      units INTEGER NOT NULL DEFAULT 0,
      PRIMARY KEY (source, period_key)
    );
  `);
  log('Postgres schema ready', 'OK');
}
function monthKey(d = new Date()) { return d.getUTCFullYear() + '-' + String(d.getUTCMonth() + 1).padStart(2, '0'); }
function dayKey(d = new Date()) { return d.toISOString().slice(0, 10); }
async function addUsage(source, periodKey, units) {
  await pool.query(
    `INSERT INTO usage_counters(source, period_key, units) VALUES ($1,$2,$3)
     ON CONFLICT (source, period_key) DO UPDATE SET units = usage_counters.units + EXCLUDED.units`,
    [source, periodKey, units]
  );
}
async function getUsage(source, periodKey) {
  const r = await pool.query(`SELECT units FROM usage_counters WHERE source=$1 AND period_key=$2`, [source, periodKey]);
  return r.rows.length ? r.rows[0].units : 0;
}

async function getAllRampState() {
  const r = await pool.query(`SELECT id, pax, spot, flags, tow_notes, updated_at FROM ramp_state`);
  return r.rows.map(rowToRamp);
}
async function getRampRow(id) {
  const r = await pool.query(`SELECT id, pax, spot, flags, tow_notes, updated_at FROM ramp_state WHERE id=$1`, [id]);
  return r.rows.length ? rowToRamp(r.rows[0]) : null;
}
function rowToRamp(row) {
  return { id: row.id, pax: row.pax, spot: row.spot || '', flags: row.flags || [], towNotes: row.tow_notes || '', updatedAt: row.updated_at };
}
async function upsertRampState(id, fields, updatedBy) {
  const current = (await getRampRow(id)) || { pax: null, spot: '', flags: [], towNotes: '' };
  const next = {
    pax: fields.pax !== undefined ? fields.pax : current.pax,
    spot: fields.spot !== undefined ? fields.spot : current.spot,
    flags: fields.flags !== undefined ? fields.flags : current.flags,
    towNotes: fields.towNotes !== undefined ? fields.towNotes : current.towNotes
  };
  await pool.query(
    `INSERT INTO ramp_state (id, pax, spot, flags, tow_notes, updated_at, updated_by)
     VALUES ($1,$2,$3,$4,$5, now(), $6)
     ON CONFLICT (id) DO UPDATE SET pax=$2, spot=$3, flags=$4, tow_notes=$5, updated_at=now(), updated_by=$6`,
    [id, next.pax, next.spot, JSON.stringify(next.flags), next.towNotes, updatedBy || null]
  );
  return { id, ...next, updatedAt: new Date().toISOString() };
}

// ============================================================================================
// OPENSKY — OAuth2 client-credentials, server-side response cache, and full credit tracking.
// Ported from v249 essentially unchanged (it was already solid — this IS the "usage meter" the
// rebuild asked for). Added: a proactive hard cutoff before we'd even try a call once our own
// estimate says we're out of budget, instead of only reacting after OpenSky 429s us.
// ============================================================================================
let oskyToken = null, oskyExp = 0, oskyAuthMode = 'unknown';
async function getToken() {
  if (oskyToken && Date.now() < oskyExp - 30000) return oskyToken;
  try {
    const r = await fetch(TOKEN_URL, {
      method: 'POST',
      headers: { 'Content-Type': 'application/x-www-form-urlencoded', 'Accept': 'application/json' },
      body: 'grant_type=client_credentials&client_id=' + encodeURIComponent(OSKY_ID) + '&client_secret=' + encodeURIComponent(OSKY_SECRET)
    });
    if (!r.ok) {
      var errTxt = '';
      try { errTxt = (await r.text()).slice(0, 120); } catch (e2) {}
      throw new Error('HTTP ' + r.status + (errTxt ? (': ' + errTxt) : ''));
    }
    const d = await r.json();
    if (!d.access_token) throw new Error('token response missing access_token');
    oskyToken = d.access_token; oskyExp = Date.now() + (d.expires_in || 1800) * 1000;
    log('OpenSky token OK (authenticated tier: 4000 credits/day)', 'OK');
    oskyAuthMode = 'authenticated';
    return oskyToken;
  } catch (e) {
    log('[OSKY AUTH] Token fetch failed: ' + e.message + ' — ANONYMOUS (400/day). Arrivals board uses adsb.lol inbound.', 'ERR');
    oskyAuthMode = 'anonymous';
    OSKY_DAILY_BUDGET = 400;
    return null;
  }
}
var oskyCache = {};
var OSKY_CACHE_TTL = 30000;
var oskyBackoffUntil = 0;
var oskyCreditsSpent = [];
var OSKY_DAILY_BUDGET = 4000;
var oskyLastSuccess = 0, oskyLast429 = 0;
var oskyRemainingFromHeader = null, oskyRetryAfterUntil = 0;

function calcCreditCost(apiPath) {
  var m = apiPath.match(/lamin=([\-0-9.]+)&lomin=([\-0-9.]+)&lamax=([\-0-9.]+)&lomax=([\-0-9.]+)/);
  if (!m) return 4;
  var latSpan = Math.abs(parseFloat(m[3]) - parseFloat(m[1]));
  var lonSpan = Math.abs(parseFloat(m[4]) - parseFloat(m[2]));
  var sqDeg = latSpan * lonSpan;
  if (sqDeg <= 25) return 4;
  if (sqDeg <= 100) return 3;
  if (sqDeg <= 400) return 2;
  return 1;
}
function recordCredit(apiPath, cost) {
  var now = Date.now(), cutoff = now - 86400000;
  while (oskyCreditsSpent.length > 0 && oskyCreditsSpent[0].ts < cutoff) oskyCreditsSpent.shift();
  oskyCreditsSpent.push({ ts: now, cost: cost, path: apiPath });
}
function getCreditSummary() {
  var now = Date.now(), cutoff = now - 86400000;
  while (oskyCreditsSpent.length > 0 && oskyCreditsSpent[0].ts < cutoff) oskyCreditsSpent.shift();
  var total = 0, hourTotal = 0, hourCutoff = now - 3600000;
  for (var i = 0; i < oskyCreditsSpent.length; i++) { total += oskyCreditsSpent[i].cost; if (oskyCreditsSpent[i].ts >= hourCutoff) hourTotal += oskyCreditsSpent[i].cost; }
  var budget = oskyAuthMode === 'anonymous' ? 400 : OSKY_DAILY_BUDGET;
  var remaining, remainingSource;
  if (oskyRemainingFromHeader !== null) { remaining = oskyRemainingFromHeader; remainingSource = 'opensky-header'; }
  else { remaining = Math.max(0, budget - total); remainingSource = 'estimate'; }
  var backoffSec = 0, backoffSource = 'none';
  if (oskyRetryAfterUntil > now) { backoffSec = Math.ceil((oskyRetryAfterUntil - now) / 1000); backoffSource = 'opensky-header'; }
  else if (oskyBackoffUntil > now) { backoffSec = Math.ceil((oskyBackoffUntil - now) / 1000); backoffSource = 'local-estimate'; }
  return {
    spent24h: total, budget: budget, remaining: remaining, remainingSource: remainingSource,
    spentLastHour: hourTotal, callCount24h: oskyCreditsSpent.length, authMode: oskyAuthMode,
    inBackoff: backoffSec > 0, backoffSecondsRemaining: backoffSec, backoffSource: backoffSource,
    lastSuccessAgo: oskyLastSuccess ? Math.round((now - oskyLastSuccess) / 1000) : null,
    last429Ago: oskyLast429 ? Math.round((now - oskyLast429) / 1000) : null
  };
}
async function proxyOsky(apiPath, res) {
  try {
    var nowT = Date.now();
    var cached = oskyCache[apiPath];
    if (cached && (nowT - cached.ts < OSKY_CACHE_TTL || nowT < oskyBackoffUntil)) {
      res.writeHead(cached.status, { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*', 'X-Cache': 'HIT' });
      res.end(cached.body); return;
    }
    if (nowT < oskyBackoffUntil) {
      res.writeHead(429, { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*', 'Retry-After': Math.ceil((oskyBackoffUntil - nowT) / 1000) });
      res.end(JSON.stringify({ error: 'rate limited, retry later', retryAfter: Math.ceil((oskyBackoffUntil - nowT) / 1000) })); return;
    }
    // BOOT SAFEGUARD #2 — proactive daily hard cutoff. v249 only reacted to a 429 AFTER asking
    // OpenSky; this refuses locally the moment our own tracking says the budget is gone, so a
    // runaway client loop can't even attempt the call.
    var summary = getCreditSummary();
    var cost = calcCreditCost(apiPath);
    if (summary.remaining < cost) {
      if (cached) { res.writeHead(cached.status, { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*', 'X-Cache': 'STALE-BUDGET' }); res.end(cached.body); return; }
      res.writeHead(402, { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' });
      res.end(JSON.stringify({ error: 'OpenSky daily credit budget exhausted', remaining: summary.remaining, budget: summary.budget })); return;
    }
    const tk = await getToken();
    const h = tk ? { 'Authorization': 'Bearer ' + tk } : {};
    const fullUrl = 'https://opensky-network.org/api' + apiPath;
    const r = await fetch(fullUrl, { headers: h });
    const body = await r.arrayBuffer();
    const buf = Buffer.from(body);
    recordCredit(apiPath, cost);
    var rlRemain = r.headers.get('x-rate-limit-remaining');
    if (rlRemain !== null && rlRemain !== undefined) { var n = parseInt(rlRemain, 10); if (!isNaN(n)) oskyRemainingFromHeader = n; }
    var rlRetry = r.headers.get('x-rate-limit-retry-after-seconds');
    if (rlRetry !== null && rlRetry !== undefined) { var retrySec = parseInt(rlRetry, 10); if (!isNaN(retrySec) && retrySec > 0) oskyRetryAfterUntil = nowT + (retrySec * 1000); }
    oskyAuthMode = tk ? 'authenticated' : 'anonymous';
    if (r.status === 429) {
      var backoffMs = oskyRetryAfterUntil > nowT ? oskyRetryAfterUntil - nowT : 60000;
      oskyBackoffUntil = Math.max(oskyBackoffUntil, nowT + backoffMs);
      oskyLast429 = nowT;
      log('[OSKY] 429 rate-limited — backoff ' + Math.ceil(backoffMs / 1000) + 's', 'WARN');
      if (cached) { res.writeHead(cached.status, { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*', 'X-Cache': 'STALE-429' }); res.end(cached.body); return; }
    }
    if (r.status >= 200 && r.status < 300) { oskyCache[apiPath] = { body: buf, status: r.status, ts: nowT }; oskyLastSuccess = nowT; }
    res.writeHead(r.status, { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*', 'X-Cache': 'MISS', 'X-Credit-Cost': cost });
    res.end(buf);
  } catch (e) {
    log('Proxy err: ' + e.message, 'ERR');
    var cached2 = oskyCache[apiPath];
    if (cached2) { res.writeHead(cached2.status, { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*', 'X-Cache': 'STALE-ERR' }); res.end(cached2.body); return; }
    res.writeHead(502, { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' });
    res.end(JSON.stringify({ error: e.message }));
  }
}
// Separate, conservative daily cap for OpenSky's /flights/arrival + /flights/departure —
// these aren't priced by the bbox-credit table above, so we just count raw calls per day.
var oskyFlightsCallsToday = 0, oskyFlightsCallsDay = dayKey();
async function oskyFlightsCall(apiPath) {
  var today = dayKey();
  if (today !== oskyFlightsCallsDay) { oskyFlightsCallsDay = today; oskyFlightsCallsToday = 0; }
  if (oskyFlightsCallsToday >= OSKY_FLIGHTS_DAILY_CAP) { log('[OSKY] /flights daily cap reached (' + OSKY_FLIGHTS_DAILY_CAP + '), skipping', 'WARN'); return null; }
  const tk = await getToken();
  const h = tk ? { 'Authorization': 'Bearer ' + tk } : {};
  try {
    const r = await fetch('https://opensky-network.org/api' + apiPath, { headers: h });
    oskyFlightsCallsToday++;
    if (!r.ok) { log('[OSKY flights] HTTP ' + r.status + ' for ' + apiPath, 'WARN'); return null; }
    return await r.json();
  } catch (e) { log('[OSKY flights] ' + e.message, 'ERR'); return null; }
}

// ============================================================================================
// ADSB.LOL — primary live position source for the map (OpenSky /states/all is fallback).
// Free, no API key. Max radius 250nm. Client always drives /adsb/states (touchActivity); idle
// pause does not block these user-tab fetches. OpenSky credit safeguards apply only on fallback.
// ============================================================================================
var adsbLolStatus = { ok: false, lastFetchAt: 0, lastCount: 0, lastError: '' };
function adsbLolStatusPayload() {
  return {
    ok: !!adsbLolStatus.ok,
    lastFetchAgo: adsbLolStatus.lastFetchAt ? Math.round((Date.now() - adsbLolStatus.lastFetchAt) / 1000) : null,
    lastCount: adsbLolStatus.lastCount || 0,
    lastError: adsbLolStatus.lastError || null
  };
}
function feetToMeters(ft) { return ft == null || ft === '' ? null : Number(ft) * 0.3048; }
function knotsToMs(kts) { return kts == null || kts === '' ? null : Number(kts) * 0.514444; }
function fpmToMs(fpm) { return fpm == null || fpm === '' ? null : Number(fpm) * 0.00508; }
function padCallsign(cs) {
  var s = (cs == null ? '' : String(cs)).trim().toUpperCase();
  if (!s) return '        ';
  if (s.length >= 8) return s.slice(0, 8);
  return s + ' '.repeat(8 - s.length);
}
function adsbAcToState(ac) {
  var hex = String(ac.hex || '').toLowerCase();
  var onGround = ac.alt_baro === 'ground' || ac.alt_baro === 'GROUND';
  var altBaroM = onGround ? 0 : feetToMeters(typeof ac.alt_baro === 'number' ? ac.alt_baro : parseFloat(ac.alt_baro));
  if (altBaroM != null && isNaN(altBaroM)) altBaroM = null;
  var altGeomM = feetToMeters(typeof ac.alt_geom === 'number' ? ac.alt_geom : parseFloat(ac.alt_geom));
  if (altGeomM != null && isNaN(altGeomM)) altGeomM = null;
  var vel = knotsToMs(ac.gs);
  if (vel != null && isNaN(vel)) vel = null;
  var vr = null;
  if (ac.geom_rate != null) vr = fpmToMs(ac.geom_rate);
  else if (ac.baro_rate != null) vr = fpmToMs(ac.baro_rate);
  if (vr != null && isNaN(vr)) vr = null;
  var track = (typeof ac.track === 'number' && !isNaN(ac.track)) ? ac.track : null;
  var lat = (typeof ac.lat === 'number') ? ac.lat : null;
  var lon = (typeof ac.lon === 'number') ? ac.lon : null;
  var squawk = ac.squawk != null ? String(ac.squawk) : null;
  return [
    hex,                          // 0 icao24
    padCallsign(ac.flight),       // 1 callsign
    '',                           // 2 origin_country
    null, null,                   // 3 time_position, 4 last_contact
    lon, lat,                     // 5 lon, 6 lat
    altBaroM,                     // 7 baro_altitude m
    !!onGround,                   // 8 on_ground
    vel,                          // 9 velocity m/s
    track,                        // 10 true_track
    vr,                           // 11 vertical_rate m/s
    null,                         // 12 sensors
    altGeomM,                     // 13 geo_altitude m
    squawk,                       // 14 squawk
    false,                        // 15 spi
    0,                            // 16 position_source
    0                             // 17 category
  ];
}
function bboxToCenterDist(lamin, lomin, lamax, lomax) {
  var la1 = parseFloat(lamin), lo1 = parseFloat(lomin), la2 = parseFloat(lamax), lo2 = parseFloat(lomax);
  if ([la1, lo1, la2, lo2].some(function (n) { return isNaN(n); })) return null;
  var lat = (la1 + la2) / 2;
  var lon = (lo1 + lo2) / 2;
  var dLatNm = Math.abs(la2 - la1) * 60 / 2;
  var cosLat = Math.cos(lat * Math.PI / 180);
  var dLonNm = Math.abs(lo2 - lo1) * 60 * Math.max(0.2, Math.abs(cosLat)) / 2;
  var dist = Math.sqrt(dLatNm * dLatNm + dLonNm * dLonNm);
  if (!isFinite(dist) || dist <= 0) dist = 50;
  return { lat: lat, lon: lon, dist: dist };
}
function centerDistToBbox(lat, lon, distNm) {
  var dLat = distNm / 60;
  var cosLat = Math.cos(lat * Math.PI / 180);
  var dLon = distNm / (60 * Math.max(0.2, Math.abs(cosLat)));
  return {
    lamin: (lat - dLat).toFixed(4),
    lamax: (lat + dLat).toFixed(4),
    lomin: (lon - dLon).toFixed(4),
    lomax: (lon + dLon).toFixed(4)
  };
}
// Shared cache + single-flight so map tabs + board poll do not stampede adsb.lol into 429.
var adsbLolCache = { key: '', at: 0, data: null, inflight: null };
var adsbLolBackoffUntil = 0;
var ADSB_CACHE_FRESH_MS = 20000;
var ADSB_CACHE_STALE_MS = 10 * 60 * 1000;
function adsbCacheKey(lat, lon, dist) {
  return Number(lat).toFixed(2) + ',' + Number(lon).toFixed(2) + ',' + dist;
}
function buildAdsbPayload(list) {
  var states = [];
  var acMeta = [];
  for (var i = 0; i < list.length; i++) {
    var ac = list[i];
    if (!ac || ac.lat == null || ac.lon == null || !ac.hex) continue;
    states.push(adsbAcToState(ac));
    acMeta.push({
      hex: String(ac.hex || '').toLowerCase(),
      reg: ac.r || null,
      type: ac.t || null,
      flight: (ac.flight || '').trim() || null,
      lat: typeof ac.lat === 'number' ? ac.lat : null,
      lon: typeof ac.lon === 'number' ? ac.lon : null,
      alt_baro: ac.alt_baro,
      gs: ac.gs,
      track: ac.track,
      dst: ac.dst,
      dir: ac.dir,
      r: ac.r || null,
      t: ac.t || null,
      source: 'adsb.lol'
    });
  }
  return { states: states, ac: acMeta, source: 'adsb.lol' };
}
async function fetchAdsbLol(lat, lon, distNm) {
  var dist = Math.max(1, Math.min(250, Math.round(Number(distNm) || 50)));
  var key = adsbCacheKey(lat, lon, dist);
  var now = Date.now();
  if (adsbLolCache.data && adsbLolCache.key === key && (now - adsbLolCache.at) < ADSB_CACHE_FRESH_MS) {
    return adsbLolCache.data;
  }
  if (now < adsbLolBackoffUntil && adsbLolCache.data && (now - adsbLolCache.at) < ADSB_CACHE_STALE_MS) {
    return adsbLolCache.data;
  }
  if (adsbLolCache.inflight && adsbLolCache.key === key) {
    try { return await adsbLolCache.inflight; } catch (e) { /* fall through */ }
  }
  var run = (async function () {
    if (Date.now() < adsbLolBackoffUntil) {
      if (adsbLolCache.data && (Date.now() - adsbLolCache.at) < ADSB_CACHE_STALE_MS) return adsbLolCache.data;
      return null;
    }
    var url = 'https://api.adsb.lol/v2/lat/' + encodeURIComponent(lat) + '/lon/' + encodeURIComponent(lon) + '/dist/' + dist;
    var ctrl = new AbortController();
    var timer = setTimeout(function () { ctrl.abort(); }, 12000);
    try {
      var r = await fetch(url, {
        signal: ctrl.signal,
        headers: {
          'Accept': 'application/json',
          'User-Agent': 'Mozilla/5.0 (compatible; SkywayAirportBoard/250; +https://github.com/skyway4k/skyway-v250)'
        }
      });
      clearTimeout(timer);
      if (!r.ok) {
        adsbLolStatus.ok = false;
        adsbLolStatus.lastError = 'HTTP ' + r.status;
        adsbLolStatus.lastFetchAt = Date.now();
        if (r.status === 429 || r.status === 503) {
          adsbLolBackoffUntil = Date.now() + 60000;
          log('[ADSB.LOL] HTTP ' + r.status + ' — backing off 60s, serving stale if any', 'WARN');
        } else {
          log('[ADSB.LOL] HTTP ' + r.status, 'WARN');
        }
        if (adsbLolCache.data && (Date.now() - adsbLolCache.at) < ADSB_CACHE_STALE_MS) return adsbLolCache.data;
        return null;
      }
      var data = await r.json();
      var list = Array.isArray(data.ac) ? data.ac : [];
      var payload = buildAdsbPayload(list);
      adsbLolStatus.ok = payload.states.length > 0;
      adsbLolStatus.lastCount = payload.states.length;
      adsbLolStatus.lastError = payload.states.length ? '' : 'empty';
      adsbLolStatus.lastFetchAt = Date.now();
      adsbLolCache = { key: key, at: Date.now(), data: payload, inflight: null };
      return payload;
    } catch (e) {
      clearTimeout(timer);
      adsbLolStatus.ok = false;
      adsbLolStatus.lastError = e.name === 'AbortError' ? 'timeout' : (e.message || String(e));
      adsbLolStatus.lastFetchAt = Date.now();
      log('[ADSB.LOL] ' + adsbLolStatus.lastError, 'WARN');
      if (adsbLolCache.data && (Date.now() - adsbLolCache.at) < ADSB_CACHE_STALE_MS) return adsbLolCache.data;
      return null;
    }
  })();
  adsbLolCache.key = key;
  adsbLolCache.inflight = run;
  try {
    return await run;
  } finally {
    if (adsbLolCache.inflight === run) adsbLolCache.inflight = null;
  }
}
/** Internal OpenSky /states/all fetch — same credit/cache/backoff rules as proxyOsky, returns parsed JSON or null. */
async function fetchOpenSkyStates(apiPath) {
  try {
    var nowT = Date.now();
    var cached = oskyCache[apiPath];
    if (cached && (nowT - cached.ts < OSKY_CACHE_TTL || nowT < oskyBackoffUntil)) {
      try { return JSON.parse(cached.body.toString('utf8')); } catch (e) { return null; }
    }
    if (nowT < oskyBackoffUntil) {
      if (cached) { try { return JSON.parse(cached.body.toString('utf8')); } catch (e) { return null; } }
      return null;
    }
    var summary = getCreditSummary();
    var cost = calcCreditCost(apiPath);
    if (summary.remaining < cost) {
      if (cached) { try { return JSON.parse(cached.body.toString('utf8')); } catch (e) { return null; } }
      return null;
    }
    const tk = await getToken();
    const h = tk ? { 'Authorization': 'Bearer ' + tk } : {};
    const r = await fetch('https://opensky-network.org/api' + apiPath, { headers: h });
    const body = await r.arrayBuffer();
    const buf = Buffer.from(body);
    recordCredit(apiPath, cost);
    var rlRemain = r.headers.get('x-rate-limit-remaining');
    if (rlRemain !== null && rlRemain !== undefined) { var n = parseInt(rlRemain, 10); if (!isNaN(n)) oskyRemainingFromHeader = n; }
    var rlRetry = r.headers.get('x-rate-limit-retry-after-seconds');
    if (rlRetry !== null && rlRetry !== undefined) { var retrySec = parseInt(rlRetry, 10); if (!isNaN(retrySec) && retrySec > 0) oskyRetryAfterUntil = nowT + (retrySec * 1000); }
    oskyAuthMode = tk ? 'authenticated' : 'anonymous';
    if (r.status === 429) {
      var backoffMs = oskyRetryAfterUntil > nowT ? oskyRetryAfterUntil - nowT : 60000;
      oskyBackoffUntil = Math.max(oskyBackoffUntil, nowT + backoffMs);
      oskyLast429 = nowT;
      log('[OSKY] 429 rate-limited (adsb fallback) — backoff ' + Math.ceil(backoffMs / 1000) + 's', 'WARN');
      if (cached) { try { return JSON.parse(cached.body.toString('utf8')); } catch (e) { return null; } }
      return null;
    }
    if (r.status >= 200 && r.status < 300) {
      oskyCache[apiPath] = { body: buf, status: r.status, ts: nowT };
      oskyLastSuccess = nowT;
      try { return JSON.parse(buf.toString('utf8')); } catch (e) { return null; }
    }
    if (cached) { try { return JSON.parse(cached.body.toString('utf8')); } catch (e) { return null; } }
    return null;
  } catch (e) {
    log('[OSKY fallback] ' + e.message, 'ERR');
    var cached2 = oskyCache[apiPath];
    if (cached2) { try { return JSON.parse(cached2.body.toString('utf8')); } catch (e2) { return null; } }
    return null;
  }
}
async function handleAdsbStates(query, res) {
  var lat, lon, dist;
  var bboxPath = null;
  if (query.lat != null && query.lon != null) {
    lat = parseFloat(query.lat);
    lon = parseFloat(query.lon);
    dist = parseFloat(query.dist != null ? query.dist : 50);
    if (isNaN(lat) || isNaN(lon) || isNaN(dist)) { sendJSON(res, 400, { error: 'invalid lat/lon/dist' }); return; }
    var bb = centerDistToBbox(lat, lon, dist);
    bboxPath = '/states/all?extended=1&lamin=' + bb.lamin + '&lomin=' + bb.lomin + '&lamax=' + bb.lamax + '&lomax=' + bb.lomax;
  } else if (query.lamin != null && query.lomin != null && query.lamax != null && query.lomax != null) {
    var c = bboxToCenterDist(query.lamin, query.lomin, query.lamax, query.lomax);
    if (!c) { sendJSON(res, 400, { error: 'invalid bbox' }); return; }
    lat = c.lat; lon = c.lon; dist = c.dist;
    bboxPath = '/states/all?extended=1&lamin=' + query.lamin + '&lomin=' + query.lomin + '&lamax=' + query.lamax + '&lomax=' + query.lomax;
  } else {
    sendJSON(res, 400, { error: 'provide lat,lon,dist or lamin,lomin,lamax,lomax' });
    return;
  }

  // adsb.lol caps at 250nm — skip it for huge bboxes (e.g. CONUS 3D) and go straight to OpenSky.
  var preferAdsb = ADSB_PRIMARY !== 'opensky' && dist <= 250;
  if (preferAdsb) {
    var adsb = await fetchAdsbLol(lat, lon, dist);
    if (adsb && adsb.states && adsb.states.length) {
      sendJSON(res, 200, adsb);
      return;
    }
  }
  // OpenSky fallback (credit-safeguarded)
  var osky = await fetchOpenSkyStates(bboxPath);
  var states = (osky && Array.isArray(osky.states)) ? osky.states : [];
  sendJSON(res, 200, { states: states, source: 'opensky', time: osky && osky.time ? osky.time : null });
}

// ============================================================================================
// ============================================================================================
// GA / BIZJET BOARD FILTER — KSFO GA board: keep fractional, N-reg GA, light/mid biz + piston/
// turboprop types; drop airline callsigns (UAL/AAL/...) and airliner/heavy ICAO types.
// Applied to ADS-B, SWIM/TFMS, and OpenSky /flights so boards stay GA/biz-heavy.
// ============================================================================================
var AIRLINE_CS = new Set(('UAL AAL DAL SWA ASA JBU NKS FFT SKW EDV RPA ASH ENY QXE HAL CPA BAW AFR DLH UAE CSG CCA CES CSN CHH CES FDX UPS GTI ATN ABX GTI VRD SCX WOA UAL CKS MPO').split(/\s+/));
var FRAC_CS = /^(EJA|EJM|LXJ|TWY|JTL|XOJ|OPT|JRE|GTT|DPJ|VJT|GAJ|HRT|TIV|LNJ|CVC)/;
function isGaBizTraffic(flight, reg, typeCode) {
  var cs = String(flight || '').trim().toUpperCase();
  var r = String(reg || '').trim().toUpperCase();
  var t = String(typeCode || '').trim().toUpperCase();
  // Airliners / heavies are never KSFO GA board traffic even on N-reg.
  if (/^(A318|A319|A320|A321|A19N|A20N|A21N|A332|A333|A339|A359|A35K|A388|B71|B72|B73|B74|B75|B76|B77|B78|B37M|B38M|B39M|CRJ|E17|E19|E75|E29|BCS|MD8|MD9|DH8)/.test(t)) return false;
  if (cs && AIRLINE_CS.has(cs.substring(0, 3))) return false;
  if (FRAC_CS.test(cs)) return true;
  if (r.charAt(0) === 'N' && r.length > 1 && r.charAt(1) >= '0' && r.charAt(1) <= '9') {
    if (/^[A-Z]{3}\d/.test(cs) && AIRLINE_CS.has(cs.substring(0, 3))) return false;
    // N-reg with empty/own callsign — GA/biz default OK unless type is airliner (handled above)
    return true;
  }
  if (/^(C25|C25A|C25B|C25C|C500|C510|C525|C550|C560|C56X|C680|C68A|C700|C750|CL30|CL35|CL60|GLF|GLEX|GA[45678]C|LJ|EA50|E50P|E55P|PC12|PC24|TBM|BE20|B350|BE9L|C172|C182|C206|C208|C210|P28|PA46|SR2|DA4|DA6|H25|FA5|FA7|FA8|F2TH|HDJT|SF50)/.test(t)) return true;
  return false;
}

// FAA SWIM SCDS — the primary free enrichment source now that FlightAware is gone. Ported from
// v249's scaffolding (it parsed FIXM messages correctly but nothing consumed the output — the
// client never even connected). v250 turns this ON by default (creds are required to boot) and
// actually feeds SWIM's arrival/departure pings into the movements board when SWIM_ENABLED=1.
// ============================================================================================
var swimStats = {
  connected: false, msgs: 0, arrivals: 0, departures: 0, reason: '',
  feeds: { tfms: { connected: false, msgs: 0 }, sfdps: { connected: false, msgs: 0, enabled: !!SWIM_QUEUE_SFDPS } }
};
var movements = { arrivals: new Map(), departures: new Map() }; // key: canonical ident (prefer N-reg)
var landedDroppedAt = new Map(); // arrivals pruned after 5m landed hold — block midnight SWIM re-add

// ------------------------------------------------------------------------------------------
// Board day window — keep KSFO TFMS/SWIM filed plans through midnight America/Los_Angeles
// of the calendar day they belong to (not a rolling N-hour UI filter).
// ------------------------------------------------------------------------------------------
function ptDateStr(ms) {
  return new Date(ms).toLocaleDateString('en-CA', { timeZone: 'America/Los_Angeles' });
}
function endOfDayPTMs(nowMs) {
  var now = nowMs || Date.now();
  var startDate = ptDateStr(now);
  for (var i = 1; i <= 48; i++) {
    var probe = now + i * 3600000;
    if (ptDateStr(probe) !== startDate) {
      var lo = probe - 3600000, hi = probe;
      while (hi - lo > 500) {
        var mid = Math.floor((lo + hi) / 2);
        if (ptDateStr(mid) === startDate) lo = mid; else hi = mid;
      }
      return hi; // first ms of next PT calendar day (exclusive)
    }
  }
  return now + 24 * 3600000;
}
function withinPTDayWindow(iso, nowMs) {
  if (!iso) return null;
  var t = new Date(iso).getTime();
  if (!t || isNaN(t)) return null;
  return t < endOfDayPTMs(nowMs || Date.now());
}
function isSwimishSource(src) {
  var s = String(src || '');
  return s.indexOf('swim') >= 0 || s.indexOf('tfms') >= 0 || s.indexOf('sfdps') >= 0 || s.indexOf('fdps') >= 0;
}
function isAdsbBoardSource(src) {
  var s = String(src || '');
  return s.indexOf('adsb') >= 0;
}

// ------------------------------------------------------------------------------------------
// Identity merge — one board row for tail↔callsign (N680QS↔EJA680, LXJ/EJM/TWY/…).
// Canonical key prefers N-reg when known; alias map links acid/callsign/reg/hex.
// ------------------------------------------------------------------------------------------
var identAliases = new Map(); // any norm ident → canonical key
var tfmsPlanByAircraft = new Map(); // canonical → last KSFO-related TFMS/SWIM plan fields

function normIdent(s) {
  return String(s || '').toUpperCase().replace(/[^A-Z0-9]/g, '');
}
function isNRegIdent(s) {
  return /^N[0-9][A-Z0-9]*$/.test(s);
}
function preferCanonical(a, b) {
  if (!a) return b;
  if (!b) return a;
  if (isNRegIdent(a) && !isNRegIdent(b)) return a;
  if (isNRegIdent(b) && !isNRegIdent(a)) return b;
  return a;
}
function linkIdents() {
  var keys = [];
  for (var i = 0; i < arguments.length; i++) {
    var k = normIdent(arguments[i]);
    if (k) keys.push(k);
  }
  if (!keys.length) return '';
  var canon = '';
  for (var j = 0; j < keys.length; j++) {
    var mapped = identAliases.get(keys[j]) || keys[j];
    canon = preferCanonical(canon, mapped);
  }
  // Also collapse if any key already maps elsewhere through a chain
  for (var j2 = 0; j2 < keys.length; j2++) {
    var m2 = identAliases.get(keys[j2]);
    if (m2) canon = preferCanonical(canon, m2);
  }
  if (!canon) canon = keys[0];
  for (var j3 = 0; j3 < keys.length; j3++) identAliases.set(keys[j3], canon);
  identAliases.set(canon, canon);
  return canon;
}
function fracHeuristicLinks(reg, cs) {
  // Common fractional patterns: N680QS↔EJA680, N450FX↔LXJ450, N17TW↔TWY17 (loose digit match).
  var r = normIdent(reg), c = normIdent(cs);
  if (!r || !c) return;
  var cm = c.match(/^(EJA|EJM|LXJ|TWY|JTL|XOJ|OPT|JRE|GTT|DPJ|VJT|GAJ|HRT|TIV|LNJ|CVC)(\d+[A-Z]?)$/);
  var rm = r.match(/^N(\d+)([A-Z]{0,3})$/);
  if (cm && rm) {
    var csNum = cm[2].replace(/[A-Z]/g, '');
    var regNum = rm[1];
    if (csNum && regNum && (csNum === regNum || regNum.endsWith(csNum) || csNum.endsWith(regNum))) {
      linkIdents(r, c);
    }
  }
}
function resolveBoardKey(reg, callsign, hex) {
  var r = normIdent(reg), c = normIdent(callsign), h = normIdent(hex);
  fracHeuristicLinks(r, c);
  return linkIdents(r, c, h);
}
function mergeMovementMaps(fromKey, toKey) {
  if (!fromKey || !toKey || fromKey === toKey) return toKey;
  ['arrivals', 'departures'].forEach(function (board) {
    var m = movements[board];
    var from = m.get(fromKey);
    if (!from) return;
    var to = m.get(toKey) || {};
    var merged = Object.assign({}, from, to);
    // Prefer non-empty airports / swim times from either
    if (!merged.from) merged.from = from.from || to.from || '';
    if (!merged.to) merged.to = from.to || to.to || '';
    if (isSwimishSource(from.source) && isAdsbBoardSource(to.source)) {
      if (from.arriveISO) { merged.arriveISO = from.arriveISO; merged.arrive = from.arrive; }
      if (from.departISO) { merged.departISO = from.departISO; merged.depart = from.depart; }
      merged.source = from.source;
    }
    m.set(toKey, merged);
    m.delete(fromKey);
  });
  if (tfmsPlanByAircraft.has(fromKey) && !tfmsPlanByAircraft.has(toKey)) {
    tfmsPlanByAircraft.set(toKey, tfmsPlanByAircraft.get(fromKey));
  }
  tfmsPlanByAircraft.delete(fromKey);
  identAliases.set(fromKey, toKey);
  return toKey;
}
function rememberTfmsPlan(key, fields) {
  if (!key) return;
  var prev = tfmsPlanByAircraft.get(key) || {};
  var next = Object.assign({}, prev, fields, { rememberedAt: Date.now() });
  tfmsPlanByAircraft.set(key, next);
  // Mirror under every alias that points here
  identAliases.forEach(function (canon, alias) {
    if (canon === key && alias !== key) {
      var p = tfmsPlanByAircraft.get(alias) || {};
      tfmsPlanByAircraft.set(alias, Object.assign({}, p, next));
    }
  });
}
function lookupTfmsPlan(key) {
  if (!key) return null;
  var canon = identAliases.get(key) || key;
  return tfmsPlanByAircraft.get(canon) || tfmsPlanByAircraft.get(key) || null;
}
function pruneTfmsPlansPastMidnight() {
  var eod = endOfDayPTMs(Date.now());
  var dropped = 0;
  // Roll landed-drop suppressions from prior PT days
  landedDroppedAt.forEach(function (at, key) {
    if (at < eod - 24 * 3600000) landedDroppedAt.delete(key);
  });
  tfmsPlanByAircraft.forEach(function (plan, key) {
    var t = plan.arriveISO ? Date.parse(plan.arriveISO) : (plan.departISO ? Date.parse(plan.departISO) : 0);
    // Drop plans whose filed time is past end of their PT day (+ small grace after midnight)
    if (t && t < eod - 24 * 3600000) { // older than previous day's EOD window
      tfmsPlanByAircraft.delete(key);
      dropped++;
    }
  });
  // Drop quiet SWIM scheduled board rows whose ETA/ETD is past today's PT midnight (and not landed-kept)
  var now = Date.now();
  ['arrivals', 'departures'].forEach(function (board) {
    movements[board].forEach(function (f, key) {
      if (!f || !isSwimishSource(f.source)) return;
      if (f.arrived || f.departed) return;
      var iso = board === 'arrivals' ? f.arriveISO : f.departISO;
      if (!iso) {
        // Keep no-time swim rows until rememberedAt day rolls (accuracy: don't drop quiet schedules early)
        if (f.lastUpdate && (now - f.lastUpdate) < 36 * 3600000) return;
        return;
      }
      var win = withinPTDayWindow(iso, now);
      if (win === false) {
        // Past midnight window — allow drop of future-beyond-today already filtered; past times handled by landed prune
        var ms = Date.parse(iso);
        if (ms && ms >= endOfDayPTMs(now)) {
          movements[board].delete(key);
          dropped++;
        }
      }
    });
  });
  return dropped > 0;
}

function connectSWIM() {
  var solace;
  try { solace = require('solclientjs'); } catch (e) { swimStats.reason = 'solclientjs not installed'; log('solclientjs missing: npm install', 'ERR'); return; }
  var fp = new solace.SolclientFactoryProperties();
  fp.profile = solace.SolclientFactoryProfiles.version10;
  solace.SolclientFactory.init(fp);

  function attachConsumer(sess, queueName, feedLabel) {
    try {
      var consumer = sess.createMessageConsumer({
        queueDescriptor: { name: queueName, type: solace.QueueType.QUEUE },
        acknowledgeMode: solace.MessageConsumerAcknowledgeMode.AUTO,
        createIfMissing: false
      });
      consumer.on(solace.MessageConsumerEventName.UP, function () {
        log('SWIM ' + feedLabel + ' queue consumer UP ✓ (' + queueName + ')', 'OK');
        if (swimStats.feeds[feedLabel]) swimStats.feeds[feedLabel].connected = true;
      });
      consumer.on(solace.MessageConsumerEventName.MESSAGE, function (msg) {
        if (swimStats.feeds[feedLabel]) swimStats.feeds[feedLabel].msgs++;
        handleSwimMsg(msg, feedLabel);
      });
      consumer.on(solace.MessageConsumerEventName.DOWN_ERROR, function () {
        log('SWIM ' + feedLabel + ' queue error', 'ERR');
        if (swimStats.feeds[feedLabel]) swimStats.feeds[feedLabel].connected = false;
      });
      consumer.connect();
    } catch (e) {
      log('SWIM ' + feedLabel + ' consumer failed: ' + e.message, 'ERR');
    }
  }

  function openSession(url, vpn, queueName, feedLabel) {
    log('Connecting to SWIM ' + feedLabel + ' (' + vpn + ')...');
    var sess = solace.SolclientFactory.createSession({
      url: url, vpnName: vpn, userName: SWIM_USER, password: SWIM_PASS,
      connectRetries: 3, reconnectRetries: 10, reconnectRetryWaitInMsecs: 5000
    });
    sess.on(solace.SessionEventCode.UP_NOTICE, function () {
      swimStats.connected = true; swimStats.reason = '';
      if (swimStats.feeds[feedLabel]) swimStats.feeds[feedLabel].connected = true;
      log('SWIM ' + feedLabel + ' connected ✓', 'OK');
      broadcast({ type: 'status', data: buildStatusPayload() });
      attachConsumer(sess, queueName, feedLabel);
    });
    sess.on(solace.SessionEventCode.CONNECT_FAILED_ERROR, function (e) {
      if (swimStats.feeds[feedLabel]) swimStats.feeds[feedLabel].connected = false;
      swimStats.reason = (e && e.infoStr) || 'connect failed';
      log('SWIM ' + feedLabel + ' connect failed: ' + swimStats.reason, 'ERR');
      broadcast({ type: 'status', data: buildStatusPayload() });
    });
    sess.on(solace.SessionEventCode.DISCONNECTED, function () {
      if (swimStats.feeds[feedLabel]) swimStats.feeds[feedLabel].connected = false;
      swimStats.connected = !!(swimStats.feeds.tfms.connected || swimStats.feeds.sfdps.connected);
      log('SWIM ' + feedLabel + ' disconnected', 'WARN');
    });
    sess.on(solace.SessionEventCode.RECONNECTED_NOTICE, function () {
      if (swimStats.feeds[feedLabel]) swimStats.feeds[feedLabel].connected = true;
      swimStats.connected = true; swimStats.reason = '';
      log('SWIM ' + feedLabel + ' reconnected ✓', 'OK');
    });
    sess.connect();
    return sess;
  }

  openSession(SWIM_URL, SWIM_VPN, SWIM_QUEUE, 'tfms');
  if (SWIM_QUEUE_SFDPS) {
    swimStats.feeds.sfdps.enabled = true;
    openSession(SWIM_URL_SFDPS, SWIM_VPN_SFDPS, SWIM_QUEUE_SFDPS, 'sfdps');
    log('SFDPS second consumer enabled (SWIM_QUEUE_SFDPS set). SWIFT portal: subscribe SFDPS/FDPS if not already on this account.', 'INFO');
  } else {
    log('SFDPS second consumer idle — set SWIM_QUEUE_SFDPS (+ SWIM_VPN_SFDPS/SWIM_URL_SFDPS) after SWIFT portal SFDPS subscription.', 'INFO');
  }
}
function xval(xml) {
  for (var i = 1; i < arguments.length; i++) {
    var tag = arguments[i];
    var re = new RegExp('<[^>]*?' + tag + '[^>]*?>\\s*([^<]+)', 'is');
    var m = xml.match(re);
    if (m && m[1] && m[1].trim().length < 200) return m[1].trim();
  }
  return null;
}
function fmtTimeLA(iso) {
  if (!iso) return '';
  return new Date(iso).toLocaleTimeString('en-US', { hour: '2-digit', minute: '2-digit', hour12: false, timeZone: 'America/Los_Angeles' });
}
// Normalize airport codes so TFMS 3-letter (SFO) matches AIRPORT_ICAO (KSFO).
function normAirport(code) {
  if (!code) return '';
  var c = String(code).toUpperCase().replace(/[^A-Z0-9]/g, '');
  return c;
}
function airportMatch(code, target) {
  var a = normAirport(code), b = normAirport(target);
  if (!a || !b) return false;
  if (a === b) return true;
  if (a.length === 4 && a[0] === 'K' && a.slice(1) === b) return true;
  if (b.length === 4 && b[0] === 'K' && b.slice(1) === a) return true;
  return false;
}
// Divert destinations we keep on the KSFO board briefly after a TFMS amend (SFO → OAK/SJC).
var DIVERT_HOLD_MS = 30 * 60 * 1000;
var DIVERT_LABELS = { KOAK: 'OAK', OAK: 'OAK', KSJC: 'SJC', SJC: 'SJC' };
function divertLabel(code) {
  var c = normAirport(code);
  if (!c) return null;
  if (DIVERT_LABELS[c]) return DIVERT_LABELS[c];
  if (c.length === 4 && c[0] === 'K' && DIVERT_LABELS[c.slice(1)]) return DIVERT_LABELS[c.slice(1)];
  return null;
}
function pruneDivertedMovements() {
  var now = Date.now();
  var changed = false;
  movements.arrivals.forEach(function (f, key) {
    if (f && f.divertAt && (now - f.divertAt) > DIVERT_HOLD_MS) {
      movements.arrivals.delete(key);
      changed = true;
    }
  });
  return changed;
}
function parseIsoLoose(s) {
  if (!s) return null;
  var t = Date.parse(s);
  if (!isNaN(t)) return new Date(t).toISOString();
  if (/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}/.test(s)) {
    t = Date.parse(s.endsWith('Z') ? s : s + 'Z');
    if (!isNaN(t)) return new Date(t).toISOString();
  }
  return null;
}
function xmlAttr(block, name) {
  var re = new RegExp('\\b' + name + '\\s*=\\s*"([^"]+)"', 'i');
  var m = block.match(re);
  return m ? m[1].trim() : null;
}
function swimPayload(message) {
  var payload = '';
  try {
    if (message.getXmlContent) {
      var xml = message.getXmlContent();
      if (xml) payload = String(xml);
    }
  } catch (e) { }
  if (payload) return payload;
  try {
    var bin = message.getBinaryAttachment();
    if (bin) {
      if (typeof bin === 'string') payload = bin;
      else if (Buffer.isBuffer(bin)) payload = bin.toString('utf-8');
      else if (ArrayBuffer.isView(bin)) payload = Buffer.from(bin.buffer, bin.byteOffset, bin.byteLength).toString('utf-8');
      else payload = String(bin);
    }
    if (!payload && message.getSdtContainer) {
      var v = message.getSdtContainer().getValue();
      if (v != null) payload = typeof v === 'string' ? v : String(v);
    }
  } catch (e) { }
  return payload || '';
}
// Split a TFMS/SFDPS document into per-flight message blocks when present.
function swimFlightBlocks(payload) {
  var blocks = [];
  var patterns = [
    /<fdm:fltdMessage\b[\s\S]*?<\/fdm:fltdMessage>/gi,
    /<nxcm:fltdMessage\b[\s\S]*?<\/nxcm:fltdMessage>/gi,
    /<flight\b[^>]*xmlns[\s\S]*?<\/flight>/gi,
    /<fx:Flight\b[\s\S]*?<\/fx:Flight>/gi,
    /<FbmsFlight\b[\s\S]*?<\/FbmsFlight>/gi
  ];
  for (var pi = 0; pi < patterns.length; pi++) {
    var re = patterns[pi], m;
    while ((m = re.exec(payload))) blocks.push(m[0]);
    if (blocks.length) break;
  }
  if (!blocks.length) blocks.push(payload);
  return blocks;
}
function handleSwimMsg(message, feedLabel) {
  swimStats.msgs++;
  var payload = swimPayload(message);
  if (!payload) return;
  var blocks = swimFlightBlocks(payload);
  var changed = false;
  for (var bi = 0; bi < blocks.length; bi++) {
    if (ingestSwimFlightBlock(blocks[bi], feedLabel || 'tfms')) changed = true;
  }
  if (changed) broadcast({ type: 'board' });
}
function ingestSwimFlightBlock(block, feedLabel) {
  // TFMS R14 fltdMessage attributes (acid/depArpt/arrArpt) + nested nxce/nxcm tags.
  // SFDPS/FDPS FIXM often uses aircraftIdentification / aerodrome locationIndicator instead.
  var cs = xmlAttr(block, 'acid') || xval(block, 'aircraftId', 'aircraftIdentification', 'callSign', 'callsign', 'flightIdentification');
  var orig = xmlAttr(block, 'depArpt') || xval(block, 'departurePoint[\\s\\S]*?airport', 'departureAirport', 'departureAerodrome.*?locationIndicator', 'originAirport', 'departureAerodrome', 'dep');
  var dest = xmlAttr(block, 'arrArpt') || xval(block, 'arrivalPoint[\\s\\S]*?airport', 'arrivalAirport', 'destinationAerodrome.*?locationIndicator', 'destinationAirport', 'destinationAerodrome', 'arr');
  var acType = xval(block, 'aircraftType', 'typeDesignator', 'aircraftSpecification', 'icaoAircraftType') || xmlAttr(block, 'aircraftType');
  var tail = xval(block, 'registration', 'aircraftRegistration', 'tailNumber', 'aircraftRegistrationMark');
  var etd = parseIsoLoose(
    xmlAttr(block, 'igtd') ||
    xval(block, 'earliestRunwayDepartureTime', 'estimatedOffBlockTime', 'EOBT', 'departureDateTime', 'gateDepartureTime', 'estimatedDepartureTime', 'igtd', 'actualOffBlockTime') ||
    xmlAttr(block.match(/<nxcm:departureFixAndTime\b[^>]*>/i)?.[0] || '', 'arrTime')
  );
  var etaAttrBlock = (block.match(/<nxcm:eta\b[^>]*>/i) || [])[0] || '';
  var eta = parseIsoLoose(
    xmlAttr(etaAttrBlock, 'timeValue') ||
    xval(block, 'earliestRunwayArrivalTime', 'estimatedArrivalTime', 'ETA', 'arrivalDateTime', 'gateArrivalTime', 'estimatedTimeOfArrival', 'actualLandingTime') ||
    xmlAttr(block.match(/<nxcm:arrivalFixAndTime\b[^>]*>/i)?.[0] || '', 'arrTime')
  );
  var msgType = (xmlAttr(block, 'msgType') || '').toLowerCase();
  var type = 'UNKNOWN';
  if (block.indexOf('DepartureInformation') >= 0 || block.indexOf('flightDeparture') >= 0 ||
      block.indexOf('actualDeparture') >= 0 || msgType.indexOf('depart') >= 0) type = 'DEPARTURE';
  else if (block.indexOf('ArrivalInformation') >= 0 || block.indexOf('flightArrival') >= 0 ||
      block.indexOf('actualArrival') >= 0 || msgType.indexOf('arriv') >= 0) type = 'ARRIVAL';
  else if (block.indexOf('EnRoute') >= 0 || block.indexOf('enRoute') >= 0 ||
      msgType.indexOf('track') >= 0 || msgType.indexOf('flightplan') >= 0 ||
      block.indexOf('flightCreate') >= 0 || block.indexOf('flightModify') >= 0 ||
      block.indexOf('trackInformation') >= 0 || block.indexOf('fltdMessage') >= 0 ||
      block.indexOf('FlightPlan') >= 0 || block.indexOf('tfmData') >= 0 ||
      block.indexOf('FxFlight') >= 0 || block.indexOf('fbms') >= 0 ||
      (feedLabel === 'sfdps')) type = 'FLIGHT_PLAN';

  if (!cs && !tail) return false;
  if (isLaddBlocked(tail, cs)) return false;

  // KSFO GA board: GA / fractional / bizjet only — drop airline callsigns and airliner types
  if (!isGaBizTraffic(cs || '', tail || '', acType || '')) return false;

  var key = resolveBoardKey(tail, cs, null);
  if (!key) return false;
  // Collapse duplicate rows if callsign-only and reg-only keys both existed
  var csKey = normIdent(cs), tailKey = normIdent(tail);
  if (csKey && csKey !== key && movements.arrivals.has(csKey)) mergeMovementMaps(csKey, key);
  if (csKey && csKey !== key && movements.departures.has(csKey)) mergeMovementMaps(csKey, key);
  if (tailKey && tailKey !== key && movements.arrivals.has(tailKey)) mergeMovementMaps(tailKey, key);
  if (tailKey && tailKey !== key && movements.departures.has(tailKey)) mergeMovementMaps(tailKey, key);

  var ident = (isNRegIdent(normIdent(tail)) ? tail : null) || (isNRegIdent(key) ? key : null) || tail || cs;
  var srcTag = (feedLabel === 'sfdps') ? 'swim-sfdps' : 'swim';
  var nowISO = new Date().toISOString();
  var did = false;
  var divertLbl = divertLabel(dest);
  var existingArr = movements.arrivals.get(key);
  var touchesHome = airportMatch(orig, AIRPORT_ICAO) || airportMatch(dest, AIRPORT_ICAO);

  // TFMS amend / arrival dest change: was on the KSFO arrivals board, now filed to KOAK or KSJC.
  if (divertLbl && existingArr && !existingArr.arrived && !airportMatch(dest, AIRPORT_ICAO)
      && !(type === 'DEPARTURE' && airportMatch(orig, AIRPORT_ICAO))) {
    upsertMovement('arrivals', key, {
      ident: ident,
      callsign: cs || existingArr.callsign || '',
      reg: tail || existingArr.reg || '',
      type: acType || existingArr.type || '',
      from: orig || existingArr.from || '',
      to: dest || existingArr.to || '',
      filedDest: existingArr.filedDest || AIRPORT_ICAO,
      divertTo: divertLbl,
      divertAirport: normAirport(dest),
      divertAt: existingArr.divertAt || Date.now(),
      arrived: false,
      arriveISO: eta || existingArr.arriveISO || '',
      arrive: eta ? fmtTimeLA(eta) : (existingArr.arrive || ''),
      departISO: etd || existingArr.departISO || '',
      depart: etd ? fmtTimeLA(etd) : (existingArr.depart || ''),
      etaNote: '',
      source: 'swim-divert',
      timeSource: 'swim'
    });
    return true;
  }

  if (!touchesHome) return false;

  function rememberIfKsfo(boardHint) {
    rememberTfmsPlan(key, {
      ident: ident, callsign: cs || '', reg: tail || '', type: acType || '',
      from: orig || '', to: dest || '',
      arriveISO: eta || '', arrive: eta ? fmtTimeLA(eta) : '',
      departISO: etd || '', depart: etd ? fmtTimeLA(etd) : '',
      board: boardHint, feed: feedLabel || 'tfms'
    });
  }

  if (type === 'ARRIVAL' && airportMatch(dest, AIRPORT_ICAO)) {
    swimStats.arrivals++;
    rememberIfKsfo('arrivals');
    upsertMovement('arrivals', key, { ident: ident, callsign: cs || '', reg: tail || '', type: acType || '', from: orig || '', to: dest || AIRPORT_ICAO, filedDest: AIRPORT_ICAO, divertTo: '', divertAirport: '', divertAt: 0, arrived: true, arriveISO: eta || nowISO, arrive: fmtTimeLA(eta || nowISO), source: srcTag, timeSource: 'swim', etaNote: '' });
    did = true;
  } else if (type === 'DEPARTURE' && airportMatch(orig, AIRPORT_ICAO)) {
    swimStats.departures++;
    rememberIfKsfo('departures');
    upsertMovement('departures', key, { ident: ident, callsign: cs || '', reg: tail || '', type: acType || '', to: dest || '', departed: true, departISO: etd || nowISO, depart: fmtTimeLA(etd || nowISO), source: srcTag, timeSource: 'swim', etaNote: '' });
    did = true;
  } else if (type === 'DEPARTURE' && airportMatch(dest, AIRPORT_ICAO)) {
    swimStats.arrivals++;
    rememberIfKsfo('arrivals');
    upsertMovement('arrivals', key, { ident: ident, callsign: cs || '', reg: tail || '', type: acType || '', from: orig || '', to: dest || AIRPORT_ICAO, filedDest: AIRPORT_ICAO, divertTo: '', divertAirport: '', divertAt: 0, arrived: false, departISO: etd || nowISO, depart: fmtTimeLA(etd || nowISO), arriveISO: eta || '', arrive: eta ? fmtTimeLA(eta) : '', source: srcTag, timeSource: 'swim', etaNote: '' });
    did = true;
  } else if (type === 'FLIGHT_PLAN' || type === 'EN_ROUTE' || type === 'UNKNOWN') {
    if (airportMatch(dest, AIRPORT_ICAO)) {
      swimStats.arrivals++;
      rememberIfKsfo('arrivals');
      upsertMovement('arrivals', key, {
        ident: ident, callsign: cs || '', reg: tail || '', type: acType || '', from: orig || '',
        to: dest || AIRPORT_ICAO, filedDest: AIRPORT_ICAO,
        arrived: false,
        divertTo: '', divertAirport: '', divertAt: 0,
        arriveISO: eta || '', arrive: eta ? fmtTimeLA(eta) : '',
        departISO: etd || '', depart: etd ? fmtTimeLA(etd) : '',
        source: srcTag, timeSource: 'swim', etaNote: ''
      });
      did = true;
    }
    if (airportMatch(orig, AIRPORT_ICAO)) {
      swimStats.departures++;
      rememberIfKsfo('departures');
      upsertMovement('departures', key, {
        ident: ident, callsign: cs || '', reg: tail || '', type: acType || '', to: dest || '',
        departed: false,
        departISO: etd || '', depart: etd ? fmtTimeLA(etd) : '',
        arriveISO: eta || '', arrive: eta ? fmtTimeLA(eta) : '',
        source: srcTag, timeSource: 'swim', etaNote: ''
      });
      did = true;
    }
  }
  return did;
}

function upsertMovement(board, key, patch) {
  var m = movements[board];
  var resolved = identAliases.get(normIdent(key)) || normIdent(key) || key;
  // If patch brings a new reg/callsign pair, re-resolve and merge orphans
  if (patch) {
    var linked = resolveBoardKey(patch.reg || (isNRegIdent(normIdent(patch.ident)) ? patch.ident : ''), patch.callsign || '', null);
    if (linked) resolved = linked;
  }
  if (resolved !== key && m.has(key)) mergeMovementMaps(key, resolved);

  var existing = m.get(resolved) || {};
  var merged = Object.assign({}, existing, patch || {});

  // Never blank out known FROM/TO with empty ADS-B patches
  if (!(patch && patch.from) && existing.from) merged.from = existing.from;
  if (!(patch && patch.to) && existing.to) merged.to = existing.to;
  if (!(patch && patch.reg) && existing.reg) merged.reg = existing.reg;
  if (!(patch && patch.callsign) && existing.callsign) merged.callsign = existing.callsign;
  if (!(patch && patch.type) && existing.type) merged.type = existing.type;

  var patchIsAdsb = isAdsbBoardSource(patch && patch.source);
  var existingSwim = isSwimishSource(existing.source) || existing.timeSource === 'swim';
  var plan = lookupTfmsPlan(resolved);

  // Prefer SWIM filed dep/ETA over ADS-B geometric ETA when both exist
  if (patchIsAdsb && (existingSwim || (plan && (plan.arriveISO || plan.departISO)))) {
    if (existing.arriveISO || (plan && plan.arriveISO)) {
      merged.arriveISO = existing.arriveISO || plan.arriveISO;
      merged.arrive = existing.arrive || (plan && plan.arrive) || fmtTimeLA(merged.arriveISO);
      merged.etaNote = '';
      delete merged.etaMin;
      merged.timeSource = 'swim';
    }
    if (existing.departISO || (plan && plan.departISO)) {
      merged.departISO = existing.departISO || plan.departISO;
      merged.depart = existing.depart || (plan && plan.depart) || fmtTimeLA(merged.departISO);
      merged.timeSource = 'swim';
    }
    if ((existing.from || (plan && plan.from)) && !merged.from) merged.from = existing.from || plan.from;
    if ((existing.to || (plan && plan.to)) && !merged.to) merged.to = existing.to || plan.to;
    // Keep swim as authoritative source label; ADS-B still contributes alt/dist/gs for map/progress
    if (existingSwim) merged.source = existing.source;
    else if (plan) merged.source = 'swim';
    merged.fromNote = merged.from ? '' : (merged.fromNote || '');
  }

  // Backfill FROM/TO on ADS-B (or thin) rows from last TFMS plan for same aircraft
  if (plan) {
    if (!merged.from && plan.from) { merged.from = plan.from; merged.fromNote = ''; }
    if (!merged.to && plan.to) { merged.to = plan.to; merged.toNote = ''; }
    if (patchIsAdsb || isAdsbBoardSource(merged.source) || merged.etaNote === 'est. from ADS-B') {
      if (plan.arriveISO && board === 'arrivals') {
        merged.arriveISO = plan.arriveISO;
        merged.arrive = plan.arrive || fmtTimeLA(plan.arriveISO);
        merged.etaNote = '';
        delete merged.etaMin;
        merged.timeSource = 'swim';
        if (!isSwimishSource(merged.source)) merged.source = 'swim+adsb';
      }
      if (plan.departISO && (board === 'departures' || !merged.departISO)) {
        merged.departISO = plan.departISO;
        merged.depart = plan.depart || fmtTimeLA(plan.departISO);
        merged.timeSource = 'swim';
      }
      if (plan.callsign && !merged.callsign) merged.callsign = plan.callsign;
      if (plan.type && !merged.type) merged.type = plan.type;
    }
  }

  if (patch && patch.ident) merged.ident = preferCanonical(normIdent(existing.ident), normIdent(patch.ident)) === normIdent(patch.ident)
    ? patch.ident
    : (existing.ident || patch.ident);
  if (isNRegIdent(resolved) && (!merged.ident || !isNRegIdent(normIdent(merged.ident)))) merged.ident = resolved;

  // Do not blank filed times with empty SWIM/ADS-B patches.
  if (!(patch && patch.arriveISO) && existing.arriveISO) {
    merged.arriveISO = existing.arriveISO;
    if (!merged.arrive) merged.arrive = existing.arrive;
  }
  if (!(patch && patch.departISO) && existing.departISO) {
    merged.departISO = existing.departISO;
    if (!merged.depart) merged.depart = existing.depart;
  }

  // Once landed, do not un-land via FLIGHT_PLAN/EN_ROUTE keep-alive (midnight retention is for pre-departure only).
  // Also freeze the land clock so repeated SWIM ARRIVAL msgs with nowISO cannot extend the 5m hold.
  if (existing.arrived && board === 'arrivals') {
    merged.arrived = true;
    merged.landedAt = existing.landedAt || landClockMs(existing, Date.now());
    if (existing.arriveISO) {
      merged.arriveISO = existing.arriveISO;
      merged.arrive = existing.arrive || merged.arrive;
    }
  }
  if (merged.arrived && board === 'arrivals' && !merged.landedAt) {
    merged.landedAt = Date.now(); // 5m hold starts at arrival detection
  }

  // Suppress re-adding a pruned landed arrival unless a new future ETA is filed.
  if (board === 'arrivals' && landedDroppedAt.has(resolved)) {
    var newEta = merged.arriveISO ? Date.parse(merged.arriveISO) : 0;
    var futurePlan = newEta && !isNaN(newEta) && newEta > Date.now() + 10 * 60000;
    if (!futurePlan) return;
    landedDroppedAt.delete(resolved);
    merged.arrived = false;
    delete merged.landedAt;
  }

  merged.lastUpdate = Date.now();
  m.set(resolved, merged);
}

// ============================================================================================
// AERODATABOX — optional, capped, OFF by default. Per the rebuild brief: only used "if SWIM
// has real gaps." Called per-flight (never for the whole board) when a movement is missing
// aircraft type after SWIM + OpenSky have both had a chance to fill it, and only while under
// budget. Response shape adapted to the same fields the rest of the app expects.
// ============================================================================================
var adbUnitsSpentCache = 0, adbMonthCache = monthKey();
async function adbLoadUsage() {
  adbMonthCache = monthKey();
  adbUnitsSpentCache = await getUsage('adb', adbMonthCache);
}
async function adbSpendUnits(n) {
  var mk = monthKey();
  if (mk !== adbMonthCache) { adbMonthCache = mk; adbUnitsSpentCache = 0; }
  adbUnitsSpentCache += n;
  await addUsage('adb', mk, n);
}
function adbStatus() {
  var remaining = Math.max(0, ADB_MONTHLY_UNIT_BUDGET - adbUnitsSpentCache);
  return { enabled: ADB_ENABLED, monthKey: adbMonthCache, unitsSpent: adbUnitsSpentCache, monthlyBudget: ADB_MONTHLY_UNIT_BUDGET, unitsRemaining: remaining };
}
async function adbEnrichIfGap(movement) {
  if (!ADB_ENABLED) return movement;
  if (!movement) return movement;
  // Ready path: fill type/operator AND missing FROM / filed ETA for GA/biz only. Never runs without ADB_KEY
  // (boot refuses ADB_ENABLED=1 without key). Still off by default.
  var needType = !(movement.type && movement.operator);
  var needRouteOrEta = !(movement.from) || !(movement.arriveISO) || movement.etaNote === 'est. from ADS-B';
  if (!needType && !needRouteOrEta) return movement;
  if (!isGaBizTraffic(movement.callsign || '', movement.ident || movement.reg || '', movement.type || '')) return movement;

  var mk = monthKey();
  if (mk !== adbMonthCache) { adbMonthCache = mk; adbUnitsSpentCache = await getUsage('adb', mk); }
  var UNIT_COST_AC = 6;
  var UNIT_COST_FL = 10;
  if (adbUnitsSpentCache + UNIT_COST_AC > ADB_MONTHLY_UNIT_BUDGET * 0.95) return movement;

  var ident = movement.reg || movement.ident || '';
  var cs = movement.callsign || '';
  var headers = { 'x-rapidapi-key': ADB_KEY, 'x-rapidapi-host': ADB_HOST };

  try {
    if (needType && ident && isNRegIdent(normIdent(ident))) {
      var r = await fetch('https://' + ADB_HOST + '/aircraft/reg/' + encodeURIComponent(ident), { headers: headers });
      await adbSpendUnits(UNIT_COST_AC);
      if (r.ok) {
        var d = await r.json();
        if (d) {
          movement.type = movement.type || d.typeCode || d.model || '';
          movement.operator = movement.operator || (d.airlineName || (d.owner ? d.owner : '')) || '';
          movement.source = (movement.source || '') + '+adb';
        }
      }
    }
  } catch (e) { log('[ADB] aircraft enrich failed for ' + ident + ': ' + e.message, 'WARN'); }

  // Optional flight status / reg flights for missing FROM or ETA (GA/biz gap-fill toward FA-closeness).
  if (needRouteOrEta && adbUnitsSpentCache + UNIT_COST_FL <= ADB_MONTHLY_UNIT_BUDGET * 0.95) {
    try {
      var day = new Date().toLocaleDateString('en-CA', { timeZone: 'America/Los_Angeles' });
      var url = '';
      if (ident && isNRegIdent(normIdent(ident))) {
        url = 'https://' + ADB_HOST + '/flights/reg/' + encodeURIComponent(ident) + '/' + day + '?withLocation=false';
      } else if (cs) {
        url = 'https://' + ADB_HOST + '/flights/number/' + encodeURIComponent(cs) + '/' + day;
      }
      if (url) {
        var fr = await fetch(url, { headers: headers });
        await adbSpendUnits(UNIT_COST_FL);
        if (fr.ok) {
          var fd = await fr.json();
          var flights = Array.isArray(fd) ? fd : (fd && (fd.flights || fd.items || fd.departures || fd.arrivals)) || [];
          if (!Array.isArray(flights) && fd && typeof fd === 'object') flights = [fd];
          var best = null;
          for (var i = 0; i < flights.length; i++) {
            var fl = flights[i];
            var arrApt = (fl.arrival && (fl.arrival.airport && (fl.arrival.airport.icao || fl.arrival.airport.iata))) || fl.arrivalAirport || '';
            var depApt = (fl.departure && (fl.departure.airport && (fl.departure.airport.icao || fl.departure.airport.iata))) || fl.departureAirport || '';
            if (airportMatch(arrApt, AIRPORT_ICAO) || airportMatch(depApt, AIRPORT_ICAO)) { best = fl; break; }
            if (!best) best = fl;
          }
          if (best) {
            var bFrom = (best.departure && best.departure.airport && (best.departure.airport.icao || best.departure.airport.iata)) || best.departureAirport || '';
            var bTo = (best.arrival && best.arrival.airport && (best.arrival.airport.icao || best.arrival.airport.iata)) || best.arrivalAirport || '';
            var bEta = (best.arrival && (best.arrival.revisedTime || best.arrival.predictedTime || best.arrival.scheduledTime)) || best.arrivalTime || '';
            var bEtd = (best.departure && (best.departure.revisedTime || best.departure.predictedTime || best.departure.scheduledTime)) || best.departureTime || '';
            if (typeof bEta === 'object') bEta = bEta.utc || bEta.local || '';
            if (typeof bEtd === 'object') bEtd = bEtd.utc || bEtd.local || '';
            if (!movement.from && bFrom) { movement.from = String(bFrom).toUpperCase(); movement.fromNote = ''; }
            if (!movement.to && bTo) movement.to = String(bTo).toUpperCase();
            var etaIso = parseIsoLoose(String(bEta || ''));
            var etdIso = parseIsoLoose(String(bEtd || ''));
            // Prefer ADB filed/predicted over ADS-B geometric; never override SWIM filed times
            if (etaIso && (movement.etaNote === 'est. from ADS-B' || !movement.arriveISO) && movement.timeSource !== 'swim') {
              movement.arriveISO = etaIso;
              movement.arrive = fmtTimeLA(etaIso);
              movement.etaNote = 'ADB';
              movement.timeSource = 'adb';
              delete movement.etaMin;
            }
            if (etdIso && !movement.departISO && movement.timeSource !== 'swim') {
              movement.departISO = etdIso;
              movement.depart = fmtTimeLA(etdIso);
            }
            movement.source = (movement.source || '') + '+adb';
          }
        }
      }
    } catch (e2) { log('[ADB] flight enrich failed for ' + (ident || cs) + ': ' + e2.message, 'WARN'); }
  }
  return movement;
}

// ============================================================================================
// LADD — Industry Limited Aircraft Data Distribution block list
// FAA/NBAA Industry LADD is not a free public streaming API. ADX / NBAA often require a manual
// download or member portal fetch. Hook: set LADD_URL (http/https text or JSON) or LADD_FILE
// (local path). When unavailable, we stub an empty set and keep a clear TODO for operators.
// Blocked regs/callsigns are filtered off public boards (privacy).
// TODO(LADD/ADX): If your org has ADX access, periodically download Industry LADD and point
// LADD_FILE at it (one registration or callsign per line, or JSON array of strings). LADD_URL
// may work when a stable HTTPS endpoint exists for your account — do not commit the list.
// ============================================================================================
var laddBlocked = new Set();
var laddStatus = { loaded: false, count: 0, source: '', error: '', source: 'stub' };
function isLaddBlocked(reg, callsign) {
  var r = normIdent(reg), c = normIdent(callsign);
  if (r && laddBlocked.has(r)) return true;
  if (c && laddBlocked.has(c)) return true;
  return false;
}
function ingestLaddText(body, sourceLabel) {
  laddBlocked = new Set();
  var raw = String(body || '');
  var items = [];
  try {
    var j = JSON.parse(raw);
    if (Array.isArray(j)) items = j;
    else if (j && Array.isArray(j.aircraft)) items = j.aircraft;
    else if (j && Array.isArray(j.registrations)) items = j.registrations;
  } catch (e) {
    items = raw.split(/[\r\n,;]+/);
  }
  for (var i = 0; i < items.length; i++) {
    var v = items[i];
    if (v && typeof v === 'object') v = v.registration || v.reg || v.callsign || v.tail || '';
    var n = normIdent(v);
    if (n && n.length >= 2) laddBlocked.add(n);
  }
  laddStatus = { loaded: true, count: laddBlocked.size, source: sourceLabel, error: '', at: Date.now() };
  log('[LADD] loaded ' + laddBlocked.size + ' blocked idents from ' + sourceLabel, 'OK');
}
async function loadLaddBlocklist() {
  try {
    if (LADD_URL) {
      var r = await fetch(LADD_URL, { headers: { 'Accept': 'application/json,text/plain,*/*' } });
      if (!r.ok) throw new Error('HTTP ' + r.status);
      await ingestLaddText(await r.text(), 'url');
      return;
    }
    if (LADD_FILE) {
      var fsPath = LADD_FILE;
      if (!fs.existsSync(fsPath)) throw new Error('LADD_FILE not found: ' + fsPath);
      ingestLaddText(fs.readFileSync(fsPath, 'utf8'), 'file');
      return;
    }
    laddStatus = {
      loaded: false, count: 0, source: 'stub',
      error: 'No LADD_URL/LADD_FILE — Industry LADD not loaded (ADX manual download may be required)',
      at: Date.now()
    };
    log('[LADD] stub active — set LADD_URL or LADD_FILE when Industry LADD is available (ADX may need manual download)', 'INFO');
  } catch (e) {
    laddStatus = { loaded: false, count: 0, source: 'error', error: e.message, at: Date.now() };
    log('[LADD] load failed: ' + e.message, 'WARN');
  }
}

// ============================================================================================
// MOVEMENTS BOARD — merges SWIM pings (live) with OpenSky's /flights/arrival + /flights/departure
// (schedule context with some reporting lag) into the same shape v249's FlightAware fetch used
// to produce, so the existing client-side rendering code (mkRow, HUD counters, etc.) needs no
// changes beyond the endpoint rename already done in the HTML.
// ============================================================================================
async function pollOpenSkyFlights() {
  if (isIdle()) return;
  var end = Math.floor(Date.now() / 1000);
  var begin = end - 21600; // last 6h window
  var icao = AIRPORT_ICAO.toLowerCase();
  var arr = await oskyFlightsCall('/flights/arrival?airport=' + AIRPORT_ICAO + '&begin=' + begin + '&end=' + end);
  var dep = await oskyFlightsCall('/flights/departure?airport=' + AIRPORT_ICAO + '&begin=' + begin + '&end=' + end);
  if (Array.isArray(arr)) {
    arr.forEach(function (f) {
      var cs = (f.callsign || '').trim();
      var regGuess = /^N[0-9]/i.test(cs) ? cs.replace(/\s+/g, '') : '';
      if (!isGaBizTraffic(cs, regGuess, '')) return;
      var key = resolveBoardKey(regGuess, cs, f.icao24);
      if (!key) return;
      var arriveISO = f.lastSeen ? new Date(f.lastSeen * 1000).toISOString() : '';
      upsertMovement('arrivals', key, {
        ident: regGuess || (cs || f.icao24 || '').trim(), callsign: cs, reg: regGuess || '',
        from: (f.estDepartureAirport || '').trim(), arriveISO: arriveISO, arrive: fmtTimeLA(arriveISO),
        arrived: !!f.lastSeen, source: 'opensky-flights'
      });
    });
  }
  if (Array.isArray(dep)) {
    dep.forEach(function (f) {
      var cs = (f.callsign || '').trim();
      var regGuess = /^N[0-9]/i.test(cs) ? cs.replace(/\s+/g, '') : '';
      if (!isGaBizTraffic(cs, regGuess, '')) return;
      var key = resolveBoardKey(regGuess, cs, f.icao24);
      if (!key) return;
      var departISO = f.firstSeen ? new Date(f.firstSeen * 1000).toISOString() : '';
      upsertMovement('departures', key, {
        ident: regGuess || (cs || f.icao24 || '').trim(), callsign: cs, reg: regGuess || '',
        to: (f.estArrivalAirport || '').trim(), departISO: departISO, depart: fmtTimeLA(departISO),
        departed: !!f.firstSeen, source: 'opensky-flights'
      });
    });
  }
  broadcast({ type: 'board' });
}

// ============================================================================================
// ADSB live board — when SWIM is off and OpenSky /flights TLS fails, fill arrivals + departures
// from adsb.lol near the field. GA/bizjet only (KSFO GA board). FROM/TO unknown without
// SWIM/OpenSky schedules — ETA is geometric (distance / closing speed), not a filed ETA.
// ============================================================================================
var ADSB_BOARD_RADIUS_NM = parseInt(process.env.ADSB_BOARD_RADIUS_NM || '50', 10);
var SFO_LAT = 37.6213, SFO_LON = -122.3790;
function nmDist(lat1, lon1, lat2, lon2) {
  var dLat = (lat2 - lat1) * 60;
  var mid = ((lat1 + lat2) / 2) * Math.PI / 180;
  var dLon = (lon2 - lon1) * 60 * Math.cos(mid);
  return Math.sqrt(dLat * dLat + dLon * dLon);
}
function bearingDeg(lat1, lon1, lat2, lon2) {
  var φ1 = lat1 * Math.PI / 180, φ2 = lat2 * Math.PI / 180;
  var Δλ = (lon2 - lon1) * Math.PI / 180;
  var y = Math.sin(Δλ) * Math.cos(φ2);
  var x = Math.cos(φ1) * Math.sin(φ2) - Math.sin(φ1) * Math.cos(φ2) * Math.cos(Δλ);
  return (Math.atan2(y, x) * 180 / Math.PI + 360) % 360;
}
function angleDiffDeg(a, b) {
  var d = Math.abs(a - b) % 360;
  return d > 180 ? 360 - d : d;
}
function estimateEtaMin(distNm, gs, track, brgToField) {
  var speed = (typeof gs === 'number' && gs > 40) ? gs : 120;
  var closing = speed;
  if (typeof track === 'number') {
    var ang = angleDiffDeg(track, brgToField);
    closing = Math.max(35, speed * Math.cos(ang * Math.PI / 180));
  }
  return Math.max(1, Math.min(180, Math.round((distNm / closing) * 60)));
}
async function pollAdsbInboundBoard() {
  if (isIdle()) { log('[ADSB board] skipped (idle)', 'INFO'); return; }
  try {
    var dist = Math.max(10, Math.min(80, ADSB_BOARD_RADIUS_NM));
    var data = await fetchAdsbLol(SFO_LAT, SFO_LON, dist);
    if (!data || !Array.isArray(data.states)) {
      log('[ADSB board] no data from adsb.lol (backoff or error)', 'WARN');
      return;
    }
    var list = Array.isArray(data.ac) ? data.ac : [];
    var seen = 0, arrN = 0, depN = 0;
    var keepArr = {}, keepDep = {};
    if (!list.length && Array.isArray(data.states)) {
      list = data.states.map(function (st) {
        return {
          hex: st[0], flight: (st[1] || '').trim(), lat: st[6], lon: st[5],
          alt_baro: st[8] ? 'ground' : (st[7] != null ? st[7] / 0.3048 : null),
          gs: st[9] != null ? st[9] / 0.514444 : null,
          track: st[10], r: null, t: null
        };
      });
    }
    for (var i = 0; i < list.length; i++) {
      var ac = list[i];
      seen++;
      var lat = ac.lat, lon = ac.lon;
      if (lat == null || lon == null) continue;
      var onGnd = ac.alt_baro === 'ground' || ac.alt_baro === 'GROUND';
      var alt = onGnd ? 0 : (typeof ac.alt_baro === 'number' ? ac.alt_baro : parseFloat(ac.alt_baro));
      if (!onGnd && (alt == null || isNaN(alt))) continue;
      var distNm = (typeof ac.dst === 'number') ? ac.dst : nmDist(lat, lon, SFO_LAT, SFO_LON);
      if (distNm > dist + 5) continue;
      var flight = (ac.flight || '').trim().toUpperCase();
      var reg = String(ac.r || ac.reg || '').trim().toUpperCase();
      var typeCode = String(ac.t || ac.type || '').trim();
      if (typeCode === 'adsb_icao' || typeCode === 'adsr_icao' || typeCode === 'mlat') typeCode = String(ac.t || '').trim();
      // adsb.lol uses `t` for ICAO type; `type` is often the ADS-B emitter kind
      typeCode = String(ac.t || '').trim();
      if (!isGaBizTraffic(flight, reg, typeCode)) continue;
      var track = (typeof ac.track === 'number') ? ac.track : (typeof ac.true_heading === 'number' ? ac.true_heading : null);
      var brgIn = bearingDeg(lat, lon, SFO_LAT, SFO_LON); // toward field
      var brgOut = (brgIn + 180) % 360; // away from field
      var toward = track == null ? (distNm < 25) : (angleDiffDeg(track, brgIn) <= 70);
      var away = track != null && angleDiffDeg(track, brgOut) <= 70;
      var gs = (typeof ac.gs === 'number') ? ac.gs : null;
      var ident = reg || flight || String(ac.hex || '').toUpperCase();
      if (!ident || ident.charAt(0) === '~') continue;
      var key = ident.replace(/[^A-Z0-9]/g, '');
      if (!key) continue;
      var nowISO = new Date().toISOString();

      // Inbound: airborne, toward field or close/low
      var inbound = !onGnd && alt <= 18000 && (toward || (distNm <= 25 && alt <= 10000));
      if (inbound) {
        var etaMin = estimateEtaMin(distNm, gs, track, brgIn);
        var etaISO = new Date(Date.now() + etaMin * 60000).toISOString();
        var boardKey = resolveBoardKey(reg, flight, ac.hex);
        if (!boardKey) boardKey = key;
        // Merge orphan callsign-only / reg-only rows into canonical
        if (key !== boardKey && movements.arrivals.has(key)) mergeMovementMaps(key, boardKey);
        var existingA = movements.arrivals.get(boardKey);
        var planA = lookupTfmsPlan(boardKey);
        var hasSwimTime = (existingA && (isSwimishSource(existingA.source) || existingA.timeSource === 'swim') && existingA.arriveISO)
          || (planA && planA.arriveISO);
        var patchA = {
          ident: (reg || (existingA && existingA.ident) || ident),
          callsign: flight || (existingA && existingA.callsign) || ident,
          reg: reg || (existingA && existingA.reg) || '',
          type: typeCode || (existingA && existingA.type) || '',
          arrived: false,
          // ADS-B geometric ETA only when no SWIM filed time — map/progress always updated
          alt: Math.round(alt),
          distNm: Math.round(distNm * 10) / 10,
          gs: gs != null ? Math.round(gs) : null,
          adsbProgress: true
        };
        if (!hasSwimTime) {
          patchA.arriveISO = etaISO;
          patchA.arrive = fmtTimeLA(etaISO);
          patchA.etaMin = etaMin;
          patchA.etaNote = 'est. from ADS-B';
          patchA.timeSource = 'adsb';
          patchA.source = (existingA && isSwimishSource(existingA.source)) ? existingA.source : 'adsb-inbound';
          if (!(existingA && existingA.from) && !(planA && planA.from)) {
            patchA.from = '';
            patchA.fromNote = 'ADS-B live';
          }
        } else {
          patchA.source = (existingA && existingA.source) || 'swim+adsb';
          patchA.timeSource = 'swim';
          patchA.etaNote = '';
        }
        upsertMovement('arrivals', boardKey, patchA);
        keepArr[boardKey] = true;
        keepArr[key] = true;
        arrN++;
        continue;
      }

      // Outbound: just left / climbing away within ~35nm
      var outbound = !onGnd && away && distNm <= 35 && alt <= 16000 && alt >= 200;
      if (outbound) {
        var boardKeyD = resolveBoardKey(reg, flight, ac.hex);
        if (!boardKeyD) boardKeyD = key;
        if (key !== boardKeyD && movements.departures.has(key)) mergeMovementMaps(key, boardKeyD);
        var existingD = movements.departures.get(boardKeyD);
        var planD = lookupTfmsPlan(boardKeyD);
        var patchD = {
          ident: (reg || (existingD && existingD.ident) || ident),
          callsign: flight || (existingD && existingD.callsign) || ident,
          reg: reg || (existingD && existingD.reg) || '',
          type: typeCode || (existingD && existingD.type) || '',
          departed: true,
          alt: Math.round(alt),
          distNm: Math.round(distNm * 10) / 10,
          gs: gs != null ? Math.round(gs) : null,
          adsbProgress: true
        };
        if (existingD && isSwimishSource(existingD.source) && existingD.departISO) {
          patchD.source = existingD.source;
          patchD.timeSource = 'swim';
        } else if (planD && planD.departISO) {
          patchD.source = 'swim+adsb';
          patchD.timeSource = 'swim';
          patchD.departISO = planD.departISO;
          patchD.depart = planD.depart || fmtTimeLA(planD.departISO);
        } else {
          patchD.departISO = nowISO;
          patchD.depart = fmtTimeLA(nowISO);
          patchD.source = 'adsb-outbound';
          patchD.timeSource = 'adsb';
          if (!(existingD && existingD.to) && !(planD && planD.to)) {
            patchD.to = '';
            patchD.toNote = 'ADS-B live';
          }
        }
        upsertMovement('departures', boardKeyD, patchD);
        keepDep[boardKeyD] = true;
        keepDep[key] = true;
        depN++;
      }
    }
    var droppedA = 0, droppedD = 0;
    movements.arrivals.forEach(function (f, key) {
      if (!f || keepArr[key]) return;
      // Never drop KSFO TFMS/SWIM filed rows just because ADS-B went quiet — keep through PT midnight.
      if (isSwimishSource(f.source) || f.timeSource === 'swim' || lookupTfmsPlan(key)) return;
      if (f.source === 'adsb-inbound' || f.source === 'adsb') { movements.arrivals.delete(key); droppedA++; }
    });
    movements.departures.forEach(function (f, key) {
      if (!f || keepDep[key]) return;
      if (isSwimishSource(f.source) || f.timeSource === 'swim' || lookupTfmsPlan(key)) return;
      if (f.source === 'adsb-outbound' || f.source === 'adsb') { movements.departures.delete(key); droppedD++; }
    });
    log('[ADSB board] seen=' + seen + ' arr=' + arrN + ' dep=' + depN + ' dropA=' + droppedA + ' dropD=' + droppedD + ' r=' + dist + 'nm', (arrN || depN) ? 'OK' : 'WARN');
    if (arrN || depN || droppedA || droppedD) broadcast({ type: 'board' });
  } catch (e) {
    log('[ADSB board] ' + e.message, 'WARN');
  }
}


var groundCache = {}; // accumulated from arrivals marked arrived, same idea as v249

const LANDED_KEEP_MS = 5 * 60 * 1000; // show landed arrivals for 5 minutes, then drop from board
// After prune, suppress SWIM midnight keep-alive from re-adding the same landed row until a future plan.
function landClockMs(f, now) {
  // Prefer detection time (landedAt) so hold is 5m from when we saw the landing,
  // not from a stale filed ETA that may be hours old when SWIM ARRIVAL arrives late.
  if (f.landedAt && !isNaN(f.landedAt)) return f.landedAt;
  var arrMs = f.arriveISO ? new Date(f.arriveISO).getTime() : 0;
  if (arrMs && !isNaN(arrMs) && arrMs <= now) return arrMs;
  if (f.lastUpdate && !isNaN(f.lastUpdate)) return f.lastUpdate;
  return now;
}

function pruneLandedArrivals() {
  var now = Date.now();
  var dropped = 0;
  movements.arrivals.forEach(function (f, key) {
    if (!f) return;
    var arrMs = f.arriveISO ? new Date(f.arriveISO).getTime() : 0;
    if (arrMs && isNaN(arrMs)) arrMs = 0;
    var landed = !!f.arrived || (arrMs > 0 && arrMs <= now);
    if (!landed) return;
    var landMs = landClockMs(f, now);
    if (!f.landedAt) f.landedAt = landMs;
    if ((now - landMs) < LANDED_KEEP_MS) return;
    // Preserve for On Ground HUD before dropping from the live arrivals board.
    if (f.ident) {
      groundCache[key] = {
        ident: f.ident, callsign: f.callsign, type: f.type, from: f.from,
        city: f.city, country: f.country, intl: f.intl,
        arrivedTime: f.arrive, arrivedISO: f.arriveISO || (f.landedAt ? new Date(f.landedAt).toISOString() : ''), departISO: f.departISO
      };
    }
    movements.arrivals.delete(key);
    landedDroppedAt.set(key, now);
    dropped++;
  });
  return dropped > 0;
}

function pruneAndAccumulateGround() {
  movements.arrivals.forEach(function (f, key) {
    if (f.arrived && f.ident) groundCache[key] = { ident: f.ident, callsign: f.callsign, type: f.type, from: f.from, arrivedTime: f.arrive, arrivedISO: f.arriveISO, departISO: f.departISO };
  });
  movements.departures.forEach(function (f, key) {
    if (f.departed && groundCache[key]) delete groundCache[key];
  });
  var cutoff = Date.now() - 30 * 24 * 60 * 60 * 1000;
  for (var k in groundCache) { if (groundCache[k].arrivedISO && new Date(groundCache[k].arrivedISO).getTime() < cutoff) delete groundCache[k]; }
}


// ------------------------------------------------------------------------------------------
// Airport city / intl enrichment for board rows (SWIM/OpenSky do not ship city names).
// Same rules as the client APT_CITY table — keep the lists roughly in sync.
// ------------------------------------------------------------------------------------------
const APT_CITY = {
  KSFO:'San Francisco',SFO:'San Francisco',KOAK:'Oakland',KSJC:'San Jose',KLAX:'Los Angeles',LAX:'Los Angeles',
  KVNY:'Van Nuys',KBUR:'Burbank',KSNA:'Santa Ana',KLGB:'Long Beach',KSAN:'San Diego',KSMF:'Sacramento',
  KSBA:'Santa Barbara',KMRY:'Monterey',KSTS:'Santa Rosa',KPAO:'Palo Alto',KSQL:'San Carlos',
  KRNO:'Reno',KLAS:'Las Vegas',KPHX:'Phoenix',KSDL:'Scottsdale',KDEN:'Denver',KAPA:'Centennial',
  KASE:'Aspen',KSLC:'Salt Lake City',KPVU:'Provo',KSEA:'Seattle',KBFI:'Boeing Field',KPDX:'Portland',
  KMSO:'Missoula',KBZN:'Bozeman',KJAC:'Jackson Hole',KTEB:'Teterboro',KJFK:'New York',KLGA:'New York',
  KEWR:'Newark',KHPN:'White Plains',KBOS:'Boston',KBED:'Bedford',KIAD:'Washington Dulles',KDCA:'Washington',
  KBWI:'Baltimore',KPHL:'Philadelphia',KORD:'Chicago',KMDW:'Chicago Midway',KPWK:'Chicago Executive',
  KDFW:'Dallas',KDAL:'Dallas Love',KADS:'Addison',KHOU:'Houston Hobby',KIAH:'Houston',KAUS:'Austin',
  KSAT:'San Antonio',KSTL:'St Louis',KSUS:'Spirit of St Louis',KMIA:'Miami',KOPF:'Opa-locka',KFLL:'Fort Lauderdale',
  KPBI:'West Palm Beach',KTPA:'Tampa',KMCO:'Orlando',KATL:'Atlanta',KPDK:'Atlanta Peachtree',KCLT:'Charlotte',
  KBNA:'Nashville',KMEM:'Memphis',KCOS:'Colorado Springs',PHNL:'Honolulu',PHOG:'Kahului',PANC:'Anchorage',
  CYVR:'Vancouver',CYYZ:'Toronto',CYUL:'Montreal',CYYC:'Calgary',MMMX:'Mexico City',MMTO:'Toluca',
  MMUN:'Cancun',MMSD:'Los Cabos',EGLL:'London',EGGW:'Luton',EGKB:'Biggin Hill',LFPB:'Paris Le Bourget',
  LSZH:'Zurich',LSGG:'Geneva',LEPA:'Palma',LEMD:'Madrid',LEBL:'Barcelona',EHAM:'Amsterdam',OMDB:'Dubai',
  VHHH:'Hong Kong',RJTT:'Tokyo Haneda',WSSS:'Singapore',YSSY:'Sydney',NZAA:'Auckland',SBGR:'Sao Paulo'
};
function isIntlCode(code) {
  if (!code) return false;
  var c = String(code).toUpperCase().replace(/[^A-Z0-9]/g, '');
  if (!c) return false;
  if (c.length === 4 && (c.charAt(0) === 'K' || c.charAt(0) === 'P')) return false;
  if (c.length === 3) return false;
  if (c.length >= 2 && c.charAt(0) >= '0' && c.charAt(0) <= '9') return false;
  if (c.length === 4) return true;
  return false;
}
function lookupAptCity(code) {
  if (!code) return '';
  var c = String(code).toUpperCase().replace(/[^A-Z0-9]/g, '');
  if (APT_CITY[c]) return APT_CITY[c];
  if (c.length === 4 && (c.charAt(0) === 'K' || c.charAt(0) === 'C' || c.charAt(0) === 'P') && APT_CITY[c.substring(1)]) return APT_CITY[c.substring(1)];
  return '';
}
function enrichMovementLoc(f, kind) {
  if (!f) return f;
  var loc = kind === 'arrivals' ? (f.from || '') : (f.to || '');
  if (!loc) return f;
  var intl = !!f.intl || isIntlCode(loc);
  f.intl = intl;
  var city = lookupAptCity(loc);
  if (intl) {
    if (!f.country) f.country = city || '';
    if (!f.city && city) f.city = city;
  } else if (!f.city && city) {
    f.city = city;
  }
  return f;
}

async function buildBoard(kind) {
  var m = movements[kind];
  var list = [];
  var rampAll = await getAllRampState();
  var rampById = {}; rampAll.forEach(function (r) { rampById[r.id] = r; });
  var entries = Array.from(m.values());
  pruneDivertedMovements();
  if (kind === 'arrivals') pruneLandedArrivals();
  entries = Array.from(m.values());
  for (var i = 0; i < entries.length; i++) {
    var f = Object.assign({}, entries[i]);
    f = enrichMovementLoc(f, kind);
    // Drop expired diverts (also pruned above; belt-and-suspenders for in-flight hold).
    if (f.divertAt && (Date.now() - f.divertAt) > DIVERT_HOLD_MS) continue;
    // Safety net: never surface airline/airliner rows on KSFO GA boards regardless of source.
    var boardCs = f.callsign || '';
    var boardReg = f.reg || f.ident || '';
    if (isLaddBlocked(boardReg, boardCs)) continue;
    if (!isGaBizTraffic(boardCs, boardReg, f.type || '')) continue;
    if (ADB_ENABLED && !isIdle()) f = await adbEnrichIfGap(f);
    var key = (f.ident || '').toUpperCase().replace(/[^A-Z0-9]/g, '');
    var ramp = rampById[key];
    f.spot = ramp ? ramp.spot : '';
    f.pax = ramp ? ramp.pax : null;
    f.flags = ramp ? ramp.flags : [];
    f.towNotes = ramp ? ramp.towNotes : '';
    list.push(f);
  }
  list.sort(function (a, b) {
    var av = kind === 'arrivals' ? (a.arriveISO || '') : (a.departISO || '');
    var bv = kind === 'arrivals' ? (b.arriveISO || '') : (b.departISO || '');
    return av.localeCompare(bv);
  });
  return list;
}

// ============================================================================================
// HTTP + WS SERVER
// ============================================================================================
const PUBLIC_DIR = path.join(__dirname, 'public');
function serveFile(res, filePath, contentType) {
  fs.readFile(filePath, (err, data) => {
    if (err) { res.writeHead(404); res.end('not found'); return; }
    res.writeHead(200, { 'Content-Type': contentType });
    res.end(data);
  });
}
function sendJSON(res, status, obj) {
  res.writeHead(status, { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' });
  res.end(JSON.stringify(obj));
}
function buildStatusPayload() {
  return Object.assign(getCreditSummary(), {
    swim: swimStats,
    adb: adbStatus(),
    ladd: { loaded: !!laddStatus.loaded, count: laddStatus.count || 0, source: laddStatus.source || 'stub', error: laddStatus.error || null },
    adsbLol: Object.assign(adsbLolStatusPayload(), {
      backoffSecondsRemaining: Math.max(0, Math.ceil((adsbLolBackoffUntil - Date.now()) / 1000))
    }),
    adsbPrimary: ADSB_PRIMARY,
    boardMode: SWIM_ENABLED ? 'swim' : 'adsb-live',
    boardWindow: 'until-midnight-PT',
    landedHoldMs: LANDED_KEEP_MS,
    idle: { paused: isIdle(), secondsSinceLastClient: Math.round((Date.now() - lastClientSeenAt) / 1000) },
    connectedClients: wsClients.size,
    airport: AIRPORT_ICAO
  });
}

const server = http.createServer(async (req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  if (req.method === 'OPTIONS') { res.writeHead(204); res.end(); return; }
  var parsed = url.parse(req.url, true);
  var pathname = parsed.pathname;

  if (pathname === '/' ) { res.writeHead(302, { Location: '/dispatch' }); res.end(); return; }
  if (pathname === '/dispatch' || pathname === '/dispatch.html') { serveFile(res, path.join(PUBLIC_DIR, 'dispatch.html'), 'text/html; charset=utf-8'); return; }
  if (pathname === '/line-room' || pathname === '/line-room.html') { serveFile(res, path.join(PUBLIC_DIR, 'line-room.html'), 'text/html; charset=utf-8'); return; }
  if (pathname === '/arrivals' || pathname === '/arrivals.html') { serveFile(res, path.join(PUBLIC_DIR, 'arrivals.html'), 'text/html; charset=utf-8'); return; }
  if (pathname === '/airloom' || pathname === '/airloom.html') { serveFile(res, path.join(PUBLIC_DIR, 'airloom.html'), 'text/html; charset=utf-8'); return; }
  if (pathname.startsWith('/vendor/')) {
    var rel = pathname.slice('/vendor/'.length);
    if (!rel || rel.includes('..') || path.isAbsolute(rel)) { res.writeHead(400); res.end('bad path'); return; }
    var vPath = path.join(PUBLIC_DIR, 'vendor', rel);
    if (!vPath.startsWith(path.join(PUBLIC_DIR, 'vendor'))) { res.writeHead(400); res.end('bad path'); return; }
    var ext = path.extname(vPath).toLowerCase();
    var ctype = ext === '.js' ? 'text/javascript; charset=utf-8'
      : ext === '.mjs' ? 'text/javascript; charset=utf-8'
      : ext === '.css' ? 'text/css; charset=utf-8'
      : ext === '.map' ? 'application/json' : 'application/octet-stream';
    serveFile(res, vPath, ctype); return;
  }

  if (pathname === '/adsb/states') { touchActivity(); await handleAdsbStates(parsed.query, res); return; }
  if (pathname.startsWith('/osky/')) { touchActivity(); await proxyOsky(req.url.replace('/osky', ''), res); return; }
  if (pathname === '/status') { sendJSON(res, 200, buildStatusPayload()); return; }

  if (pathname === '/api/arrivals') { touchActivity(); sendJSON(res, 200, await buildBoard('arrivals')); return; }
  if (pathname === '/api/departures') { touchActivity(); sendJSON(res, 200, await buildBoard('departures')); return; }
  if (pathname === '/api/ground') {
    touchActivity();
    pruneAndAccumulateGround();
    var rampAll = await getAllRampState(); var rampById = {}; rampAll.forEach(function (r) { rampById[r.id] = r; });
    var depByTail = {};
    movements.departures.forEach(function (f, key) { if (!f.departed) depByTail[key] = f; });
    var ground = Object.keys(groundCache).map(function (k) {
      var f = groundCache[k]; var dep = depByTail[k];
      var ramp = rampById[k];
      return Object.assign({}, f, {
        nextDest: dep ? dep.to : '', nextDepart: dep ? dep.depart : '', nextDepartISO: dep ? dep.departISO : '',
        spot: ramp ? ramp.spot : '', pax: ramp ? ramp.pax : null, flags: ramp ? ramp.flags : [], towNotes: ramp ? ramp.towNotes : ''
      });
    });
    ground.sort(function (a, b) { return (b.arrivedISO || '').localeCompare(a.arrivedISO || ''); });
    sendJSON(res, 200, ground); return;
  }
  if (pathname === '/api/lookup') {
    touchActivity();
    var ident = String(parsed.query.ident || '').trim().toUpperCase();
    if (!ident || ident.length < 2 || ident.length > 10 || !/^[A-Z0-9-]+$/.test(ident)) { sendJSON(res, 400, { error: 'invalid ident format' }); return; }
    var key = ident.replace(/[^A-Z0-9]/g, '');
    var found = movements.arrivals.get(key) || movements.departures.get(key) || null;
    var out = { ident: ident, type: found ? found.type : null, model: null, owner: null, flights: [], lastArrival: null, nextDeparture: null };
    if (ADB_ENABLED && found) await adbEnrichIfGap(found);
    if (found) out.type = found.type || out.type;
    sendJSON(res, 200, out); return;
  }

  if (pathname === '/api/ramp' && req.method === 'GET') { touchActivity(); sendJSON(res, 200, await getAllRampState()); return; }

  var rampWrite = pathname.match(/^\/api\/(dispatch|line-room)\/ramp\/([^\/]+)$/);
  if (rampWrite && req.method === 'PATCH') {
    touchActivity();
    var view = rampWrite[1], id = decodeURIComponent(rampWrite[2]).toUpperCase().replace(/[^A-Z0-9]/g, '');
    var chunks = [];
    req.on('data', c => chunks.push(c));
    req.on('end', async () => {
      try {
        var body = JSON.parse(Buffer.concat(chunks).toString('utf-8') || '{}');
        // Server-side permission enforcement, not just client-side hiding: line-room may only
        // ever touch spot. Dispatch may touch pax/spot/flags/towNotes.
        var allowed = view === 'dispatch' ? ['pax', 'spot', 'flags', 'towNotes'] : ['spot'];
        var fields = {};
        for (var k in body) { if (allowed.indexOf(k) >= 0) fields[k] = body[k]; }
        var extraKeys = Object.keys(body).filter(function (k) { return allowed.indexOf(k) < 0; });
        if (extraKeys.length && view !== 'dispatch') { sendJSON(res, 403, { error: view + ' may only edit: ' + allowed.join(', '), rejected: extraKeys }); return; }
        if (fields.pax !== undefined && fields.pax !== null) fields.pax = Math.max(0, Math.min(99, parseInt(fields.pax, 10) || 0));
        if (fields.towNotes !== undefined) fields.towNotes = String(fields.towNotes).slice(0, 300);
        if (fields.flags !== undefined && !Array.isArray(fields.flags)) fields.flags = [];
        var saved = await upsertRampState(id, fields, view);
        broadcast({ type: 'ramp_update', id: id, fields: saved });
        sendJSON(res, 200, saved);
      } catch (e) { sendJSON(res, 400, { error: e.message }); }
    });
    return;
  }

  sendJSON(res, 200, { name: 'Skyway v250', views: ['/dispatch', '/line-room', '/arrivals', '/airloom'] });
});

function broadcast(d) {
  var m = JSON.stringify(d);
  wsClients.forEach(function (c) { if (c.readyState === 1) try { c.send(m); } catch (e) { } });
}

const wss = new WebSocketServer({ server, path: '/ws' });
wss.on('connection', ws => {
  wsClients.add(ws); touchActivity();
  log('WS connected (' + wsClients.size + ' open)');
  ws.send(JSON.stringify({ type: 'status', data: buildStatusPayload() }));
  ws.on('close', () => { wsClients.delete(ws); log('WS closed (' + wsClients.size + ' open)'); });
  ws.on('message', () => touchActivity());
});

// ------------------------------------------------------------------------------------------
// BOOT SAFEGUARD #3 — auto-pause when no browser tabs open. Background polling (OpenSky
// /flights schedule poll, status broadcast) all check isIdle() before doing anything that
// costs API budget. The one thing that stays running while idle is SWIM — it's free and
// event-driven (Solace pushes messages to us; there's no "poll less often" lever), so leaving
// it connected costs nothing and keeps the board correct the instant someone opens a tab again.
// ------------------------------------------------------------------------------------------
setInterval(pollOpenSkyFlights, 120000);
setInterval(pollAdsbInboundBoard, 45000);
setInterval(() => broadcast({ type: 'status', data: buildStatusPayload() }), 15000);
setInterval(pruneAndAccumulateGround, 60000);
setInterval(function () { if (pruneDivertedMovements()) broadcast({ type: 'board' }); }, 60000);
setInterval(function () { if (pruneLandedArrivals()) broadcast({ type: 'board' }); }, 30000);
setInterval(function () { if (pruneTfmsPlansPastMidnight()) broadcast({ type: 'board' }); }, 300000);
setInterval(function () { loadLaddBlocklist().catch(function () {}); }, 6 * 3600000);

async function main() {
  await initSchema();
  await adbLoadUsage();
  await loadLaddBlocklist();
  server.listen(PORT, '0.0.0.0', () => log('Skyway v250 — http://0.0.0.0:' + PORT, 'OK'));
  // Don't let OpenSky auth block the adsb board — race token, await inbound fill.
  getToken().catch(function () {});
  pollOpenSkyFlights();
  try { await pollAdsbInboundBoard(); } catch (e) { log('[ADSB board] boot ' + e.message, 'WARN'); }
  if (SWIM_ENABLED) {
    connectSWIM();
  } else {
    swimStats.reason = 'SWIM_ENABLED!=1 (OpenSky + adsb.lol only)';
    log('SWIM skipped — set SWIM_ENABLED=1 with SWIM_USER/SWIM_PASS/SWIM_QUEUE when SWIFT is restored', 'WARN');
  }
  log('Views: /dispatch  /line-room  /arrivals  /airloom', 'OK');
  log('AeroDataBox: ' + (ADB_ENABLED ? ('ENABLED, budget ' + ADB_MONTHLY_UNIT_BUDGET + ' units/mo') : 'disabled (set ADB_ENABLED=1 to turn on)'), 'INFO');
  log('ADSB primary: ' + ADSB_PRIMARY + ' (set ADSB_PRIMARY=opensky to force OpenSky)', 'INFO');
  if (process.env.DEMO_SEED === '1') {
    var now = Date.now();
    var iso = function (ms) { return new Date(ms).toISOString(); };
    upsertMovement('arrivals', 'N77WJ', { ident: 'N77WJ', callsign: 'N77WJ', type: 'GLF5', from: 'KSDL', operator: 'Walmart Aviation', arrived: false, arriveISO: iso(now + 25*60000), arrive: fmtTimeLA(iso(now + 25*60000)), source: 'demo' });
    upsertMovement('arrivals', 'N123NJ', { ident: 'N123NJ', callsign: 'EJA123', type: 'C680', from: 'KTEB', operator: 'NetJets', arrived: true, arriveISO: iso(now - 40*60000), arrive: fmtTimeLA(iso(now - 40*60000)), source: 'demo' });
    upsertMovement('arrivals', 'N9QX', { ident: 'N9QX', callsign: 'N9QX', type: 'CL30', from: 'KVNY', arrived: false, arriveISO: iso(now + 90*60000), arrive: fmtTimeLA(iso(now + 90*60000)), source: 'demo' });
    upsertMovement('departures', 'N550FX', { ident: 'N550FX', callsign: 'LXJ550', type: 'CL35', to: 'KAPA', operator: 'Flexjet', departed: false, departISO: iso(now + 55*60000), depart: fmtTimeLA(iso(now + 55*60000)), source: 'demo' });
    upsertMovement('departures', 'N88HE', { ident: 'N88HE', callsign: 'N88HE', type: 'EC35', to: 'KOAK', operator: 'REACH Air', departed: false, departISO: iso(now + 15*60000), depart: fmtTimeLA(iso(now + 15*60000)), source: 'demo' });
    await upsertRampState('N123NJ', { pax: 6, spot: 'A3', flags: ['VIP'], towNotes: 'GPU pre-stage' }, 'demo');
    await upsertRampState('N77WJ', { pax: 4, spot: 'B2', flags: ['GPU'], towNotes: '' }, 'demo');
    await upsertRampState('N550FX', { pax: 2, spot: 'C1', flags: [], towNotes: 'tow to 28L' }, 'demo');
    groundCache['N123NJ'] = { ident: 'N123NJ', callsign: 'EJA123', type: 'C680', from: 'KTEB', arrivedTime: fmtTimeLA(iso(now - 40*60000)), arrivedISO: iso(now - 40*60000) };
    log('DEMO_SEED: sample arrivals/departures/ramp_state loaded', 'OK');
    broadcast({ type: 'board' });
  }
}
main().catch(e => { log('Fatal boot error: ' + e.message, 'ERR'); process.exit(1); });
process.on('SIGINT', () => { log('Bye', 'WARN'); pool.end(); process.exit(0); });
