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
const SWIM_VPN = process.env.SWIM_VPN || 'FDPS';

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
// FAA SWIM SCDS — the primary free enrichment source now that FlightAware is gone. Ported from
// v249's scaffolding (it parsed FIXM messages correctly but nothing consumed the output — the
// client never even connected). v250 turns this ON by default (creds are required to boot) and
// actually feeds SWIM's arrival/departure pings into the movements board when SWIM_ENABLED=1.
// ============================================================================================
var swimStats = { connected: false, msgs: 0, arrivals: 0, departures: 0, reason: '' };
var movements = { arrivals: new Map(), departures: new Map() }; // key: ident (tail or callsign, uppercased)

function connectSWIM() {
  var solace;
  try { solace = require('solclientjs'); } catch (e) { swimStats.reason = 'solclientjs not installed'; log('solclientjs missing: npm install', 'ERR'); return; }
  var fp = new solace.SolclientFactoryProperties();
  fp.profile = solace.SolclientFactoryProfiles.version10;
  solace.SolclientFactory.init(fp);
  log('Connecting to SWIM SCDS...');
  var sess = solace.SolclientFactory.createSession({ url: SWIM_URL, vpnName: SWIM_VPN, userName: SWIM_USER, password: SWIM_PASS, connectRetries: 3, reconnectRetries: 10, reconnectRetryWaitInMsecs: 5000 });
  sess.on(solace.SessionEventCode.UP_NOTICE, function () {
    swimStats.connected = true; swimStats.reason = '';
    log('SWIM connected ✓', 'OK');
    broadcast({ type: 'status', data: buildStatusPayload() });
    try {
      var consumer = sess.createMessageConsumer({ queueDescriptor: { name: SWIM_QUEUE, type: solace.QueueType.QUEUE }, acknowledgeMode: solace.MessageConsumerAcknowledgeMode.AUTO, createIfMissing: false });
      consumer.on(solace.MessageConsumerEventName.UP, function () { log('SWIM queue consumer UP ✓', 'OK'); });
      consumer.on(solace.MessageConsumerEventName.MESSAGE, function (msg) { handleSwimMsg(msg); });
      consumer.on(solace.MessageConsumerEventName.DOWN_ERROR, function () { log('SWIM queue error', 'ERR'); });
      consumer.connect();
    } catch (e) { log('SWIM queue err: ' + e.message, 'ERR'); }
  });
  sess.on(solace.SessionEventCode.CONNECT_FAILED_ERROR, function (e) {
    swimStats.connected = false; swimStats.reason = e.infoStr || 'connect failed';
    log('SWIM FAILED: ' + swimStats.reason, 'ERR');
    broadcast({ type: 'status', data: buildStatusPayload() });
  });
  sess.on(solace.SessionEventCode.DISCONNECTED, function () { swimStats.connected = false; swimStats.reason = 'disconnected'; log('SWIM disconnected', 'WARN'); });
  sess.on(solace.SessionEventCode.RECONNECTED_NOTICE, function () { swimStats.connected = true; swimStats.reason = ''; log('SWIM reconnected ✓', 'OK'); });
  sess.connect();
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
function handleSwimMsg(message) {
  swimStats.msgs++;
  var payload = '';
  try {
    var bin = message.getBinaryAttachment();
    if (bin) payload = typeof bin === 'string' ? bin : Buffer.isBuffer(bin) ? bin.toString('utf-8') : String(bin);
    if (!payload && message.getSdtContainer) payload = message.getSdtContainer().getValue();
  } catch (e) { }
  if (!payload) return;
  var type = 'UNKNOWN';
  if (payload.indexOf('DepartureInformation') >= 0 || payload.indexOf('flightDeparture') >= 0) type = 'DEPARTURE';
  else if (payload.indexOf('ArrivalInformation') >= 0 || payload.indexOf('flightArrival') >= 0) type = 'ARRIVAL';
  else if (payload.indexOf('EnRoute') >= 0 || payload.indexOf('enRoute') >= 0) type = 'EN_ROUTE';
  var cs = xval(payload, 'aircraftIdentification', 'callsign');
  var orig = xval(payload, 'departureAerodrome.*?locationIndicator', 'departureAirport', 'originAirport');
  var dest = xval(payload, 'destinationAerodrome.*?locationIndicator', 'arrivalAirport', 'destinationAirport');
  var acType = xval(payload, 'aircraftType', 'typeDesignator');
  var tail = xval(payload, 'registration', 'aircraftRegistration');
  var nowISO = new Date().toISOString();
  if (!cs) return;
  var key = (tail || cs).toUpperCase().replace(/[^A-Z0-9]/g, '');
  if (type === 'ARRIVAL' && dest === AIRPORT_ICAO) {
    swimStats.arrivals++;
    upsertMovement('arrivals', key, { ident: tail || cs, callsign: cs, type: acType || '', from: orig || '', arrived: true, arriveISO: nowISO, arrive: fmtTimeLA(nowISO), source: 'swim' });
  } else if (type === 'DEPARTURE' && orig === AIRPORT_ICAO) {
    swimStats.departures++;
    upsertMovement('departures', key, { ident: tail || cs, callsign: cs, type: acType || '', to: dest || '', departed: true, departISO: nowISO, depart: fmtTimeLA(nowISO), source: 'swim' });
  } else if (type === 'DEPARTURE' && dest === AIRPORT_ICAO) {
    // A departure ping whose destination IS us = an inbound flight that just left its origin —
    // this is our earliest-possible signal, before SWIM ever sends an arrival ping.
    swimStats.arrivals++;
    upsertMovement('arrivals', key, { ident: tail || cs, callsign: cs, type: acType || '', from: orig || '', arrived: false, departISO: nowISO, depart: fmtTimeLA(nowISO), source: 'swim' });
  }
  broadcast({ type: 'board' });
}
function upsertMovement(board, key, patch) {
  var m = movements[board];
  var existing = m.get(key) || {};
  m.set(key, Object.assign({}, existing, patch, { lastUpdate: Date.now() }));
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
  if (movement.type && movement.operator) return movement; // no gap — SWIM/OpenSky already had it
  var mk = monthKey();
  if (mk !== adbMonthCache) { adbMonthCache = mk; adbUnitsSpentCache = await getUsage('adb', mk); }
  var UNIT_COST = 6; // AeroDataBox aircraft-lookup call; adjust if your plan's actual cost differs
  if (adbUnitsSpentCache + UNIT_COST > ADB_MONTHLY_UNIT_BUDGET * 0.95) return movement; // hard cutoff at 95%
  var ident = movement.ident;
  if (!ident) return movement;
  try {
    var r = await fetch('https://' + ADB_HOST + '/aircraft/reg/' + encodeURIComponent(ident), {
      headers: { 'x-rapidapi-key': ADB_KEY, 'x-rapidapi-host': ADB_HOST }
    });
    await adbSpendUnits(UNIT_COST);
    if (!r.ok) return movement;
    var d = await r.json();
    if (d) {
      movement.type = movement.type || d.typeCode || d.model || '';
      movement.operator = movement.operator || (d.airlineName || (d.owner ? d.owner : '')) || '';
      movement.source = (movement.source || '') + '+adb';
    }
  } catch (e) { log('[ADB] enrich failed for ' + ident + ': ' + e.message, 'WARN'); }
  return movement;
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
      var key = (f.callsign || f.icao24 || '').trim().toUpperCase().replace(/[^A-Z0-9]/g, '');
      if (!key) return;
      var arriveISO = f.lastSeen ? new Date(f.lastSeen * 1000).toISOString() : '';
      upsertMovement('arrivals', key, {
        ident: (f.callsign || f.icao24 || '').trim(), callsign: (f.callsign || '').trim(),
        from: (f.estDepartureAirport || '').trim(), arriveISO: arriveISO, arrive: fmtTimeLA(arriveISO),
        arrived: !!f.lastSeen, source: 'opensky-flights'
      });
    });
  }
  if (Array.isArray(dep)) {
    dep.forEach(function (f) {
      var key = (f.callsign || f.icao24 || '').trim().toUpperCase().replace(/[^A-Z0-9]/g, '');
      if (!key) return;
      var departISO = f.firstSeen ? new Date(f.firstSeen * 1000).toISOString() : '';
      upsertMovement('departures', key, {
        ident: (f.callsign || f.icao24 || '').trim(), callsign: (f.callsign || '').trim(),
        to: (f.estArrivalAirport || '').trim(), departISO: departISO, depart: fmtTimeLA(departISO),
        departed: !!f.firstSeen, source: 'opensky-flights'
      });
    });
  }
  broadcast({ type: 'board' });
}

// ============================================================================================
// ADSB live board — when SWIM is off and OpenSky /flights TLS fails, fill arrivals + departures
// from adsb.lol near the field. GA/bizjet only (Signature FBO board). FROM/TO unknown without
// SWIM/OpenSky schedules — ETA is geometric (distance / closing speed), not a filed ETA.
// ============================================================================================
var ADSB_BOARD_RADIUS_NM = parseInt(process.env.ADSB_BOARD_RADIUS_NM || '50', 10);
var SFO_LAT = 37.6213, SFO_LON = -122.3790;
var AIRLINE_CS = new Set(('UAL AAL DAL SWA ASA JBU NKS FFT SKW EDV RPA ASH ENY QXE HAL CPA BAW AFR DLH UAE CSG CCA CES CSN CHH CES FDX UPS GTI ATN ABX GTI VRD SCX WOA UAL CKS MPO').split(/\s+/));
var FRAC_CS = /^(EJA|EJM|LXJ|TWY|JTL|XOJ|OPT|JRE|GTT|DPJ|VJT|GAJ|HRT|TIV|LNJ|CVC)/;
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
function isGaBizTraffic(flight, reg, typeCode) {
  var cs = String(flight || '').trim().toUpperCase();
  var r = String(reg || '').trim().toUpperCase();
  var t = String(typeCode || '').trim().toUpperCase();
  // Airliners / heavies are never Signature FBO board traffic even on N-reg.
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
        upsertMovement('arrivals', key, {
          ident: ident,
          callsign: flight || ident,
          type: typeCode,
          from: '',
          fromNote: 'ADS-B live',
          arrived: false,
          arriveISO: etaISO,
          arrive: fmtTimeLA(etaISO),
          etaMin: etaMin,
          etaNote: 'est. from ADS-B',
          source: 'adsb-inbound',
          alt: Math.round(alt),
          distNm: Math.round(distNm * 10) / 10,
          gs: gs != null ? Math.round(gs) : null
        });
        keepArr[key] = true;
        arrN++;
        continue;
      }

      // Outbound: just left / climbing away within ~35nm
      var outbound = !onGnd && away && distNm <= 35 && alt <= 16000 && alt >= 200;
      if (outbound) {
        upsertMovement('departures', key, {
          ident: ident,
          callsign: flight || ident,
          type: typeCode,
          to: '',
          toNote: 'ADS-B live',
          departed: true,
          departISO: nowISO,
          depart: fmtTimeLA(nowISO),
          source: 'adsb-outbound',
          alt: Math.round(alt),
          distNm: Math.round(distNm * 10) / 10,
          gs: gs != null ? Math.round(gs) : null
        });
        keepDep[key] = true;
        depN++;
      }
    }
    var droppedA = 0, droppedD = 0;
    movements.arrivals.forEach(function (f, key) {
      if (f && f.source === 'adsb-inbound' && !keepArr[key]) { movements.arrivals.delete(key); droppedA++; }
    });
    movements.departures.forEach(function (f, key) {
      if (f && f.source === 'adsb-outbound' && !keepDep[key]) { movements.departures.delete(key); droppedD++; }
    });
    log('[ADSB board] seen=' + seen + ' arr=' + arrN + ' dep=' + depN + ' dropA=' + droppedA + ' dropD=' + droppedD + ' r=' + dist + 'nm', (arrN || depN) ? 'OK' : 'WARN');
    if (arrN || depN || droppedA || droppedD) broadcast({ type: 'board' });
  } catch (e) {
    log('[ADSB board] ' + e.message, 'WARN');
  }
}


var groundCache = {}; // accumulated from arrivals marked arrived, same idea as v249
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

async function buildBoard(kind) {
  var m = movements[kind];
  var list = [];
  var rampAll = await getAllRampState();
  var rampById = {}; rampAll.forEach(function (r) { rampById[r.id] = r; });
  var entries = Array.from(m.values());
  for (var i = 0; i < entries.length; i++) {
    var f = Object.assign({}, entries[i]);
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
    adsbLol: Object.assign(adsbLolStatusPayload(), {
      backoffSecondsRemaining: Math.max(0, Math.ceil((adsbLolBackoffUntil - Date.now()) / 1000))
    }),
    adsbPrimary: ADSB_PRIMARY,
    boardMode: SWIM_ENABLED ? 'swim' : 'adsb-live',
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

async function main() {
  await initSchema();
  await adbLoadUsage();
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
