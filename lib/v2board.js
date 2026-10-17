'use strict';
// ============================================================================================
// Skyway v2 board composer (additive). Reads the SAME rows /api/arrivals and /api/departures
// serve (via the injected buildBoard), never touches ingest/merge state, and turns them into:
//   sections (arrivals / on ground / departures / departed), a "Now" strip, a per-spot ramp
//   timeline with conflict detection + spot suggestions, GM summary, and an hourly profile.
// Everything derived here that is a guess is marked `est: true` or carries an `est` label.
// Role projection (dispatch / line / lobby / gm) happens server-side so it can sit behind auth.
// ============================================================================================
var fs = require('fs');
var path = require('path');

var MIN = 60000, HOUR = 3600000;
var KSFO_LL = { lat: 37.6188, lon: -122.3754 };

// ---- airport coordinates (OurAirports, public domain; data/v2-airports.json) --------------
var AIRPORTS = {};
try { AIRPORTS = JSON.parse(fs.readFileSync(path.join(__dirname, '..', 'data', 'v2-airports.json'), 'utf8')); } catch (e) { AIRPORTS = {}; }
function aptLL(code) {
  if (!code) return null;
  var c = String(code).toUpperCase().replace(/[^A-Z0-9]/g, '');
  var a = AIRPORTS[c] || (c.length === 3 ? AIRPORTS['K' + c] : null);
  return a ? { lat: a[0], lon: a[1], city: a[2] || '' } : null;
}
function gcNm(a, b) {
  if (!a || !b) return null;
  var R = 3440.065, r = Math.PI / 180;
  var dLat = (b.lat - a.lat) * r, dLon = (b.lon - a.lon) * r;
  var h = Math.sin(dLat / 2) * Math.sin(dLat / 2) + Math.cos(a.lat * r) * Math.cos(b.lat * r) * Math.sin(dLon / 2) * Math.sin(dLon / 2);
  return R * 2 * Math.atan2(Math.sqrt(h), Math.sqrt(1 - h));
}

// ---- aircraft size (wingspan ft → ramp category), same table as v1 plus a few missing types --
var SPAN = { GL7T: 104, GL5T: 94, GLEX: 99, GLF6: 99, GLF5: 93, GLF4: 78, GLF3: 78, GLF2: 69, GA7C: 103, GA8C: 103, GA6C: 94, GA5C: 87, GA4C: 78, G280: 63, G200: 58, GALX: 58, G150: 55, GX6C: 94, CL30: 64, CL35: 69, CL60: 64, E35L: 69, E545: 66, E550: 69, E55P: 52, E50P: 40, EA50: 38, F2TH: 70, F900: 70, FA50: 62, FA6X: 86, FA7X: 86, FA8X: 86, FA10: 43, FA20: 53, C25A: 47, C25B: 47, C25C: 50, C25M: 50, C500: 47, C501: 47, C510: 43, C525: 47, C550: 52, C560: 55, C56X: 56, C650: 54, C680: 64, C68A: 72, C700: 69, C750: 64, H25B: 54, H25C: 54, HA4T: 40, HDJT: 40, LJ31: 44, LJ35: 44, LJ40: 48, LJ45: 48, LJ60: 44, LJ70: 51, LJ75: 51, PC12: 53, PC24: 56, B350: 58, BE20: 55, BE40: 44, BE9L: 50, BE30: 55, SF50: 39, SR22: 38, SR20: 37, PRM1: 44, TBM7: 42, TBM8: 42, TBM9: 42, PA46: 43, P46T: 43, PA32: 37, PA28: 35, PA31: 40, PA34: 39, C172: 36, C182: 36, C206: 36, C208: 52, C210: 37, BE36: 33, BE58: 38, DA42: 44, DA62: 48, P180: 46, ASTR: 55, B06: 33, EC35: 33, EC45: 36, S76: 44, A139: 46, A109: 36, AS50: 35, B407: 35, B429: 36 };
var HELI = { S76: 1, EC35: 1, EC45: 1, B06: 1, A139: 1, AS50: 1, AS55: 1, EC30: 1, EC55: 1, H60: 1, R22: 1, R44: 1, R66: 1, BK17: 1, B407: 1, B412: 1, B429: 1, H500: 1, MD52: 1, MD60: 1, AS65: 1, S92: 1, AW09: 1, AW69: 1, AW18: 1, H135: 1, H145: 1, H160: 1, H175: 1, H215: 1, H225: 1, B505: 1, B105: 1, B212: 1, A109: 1 };
function wsCat(t) { var ws = SPAN[String(t || '').toUpperCase()] || 60; if (ws < 45) return 1; if (ws < 55) return 2; if (ws < 70) return 3; if (ws < 95) return 4; if (ws < 105) return 5; return 6; }
var CAT_LABEL = { 1: 'light', 2: 'light jet', 3: 'midsize', 4: 'super-mid / large', 5: 'heavy', 6: 'heavy+' };

// Fuel burn ESTIMATE profiles (same as v1 GM): gallons ≈ gph × (gc_nm / ktas) × 1.15
function burnProfile(t) {
  var c = wsCat(t); t = String(t || '').toUpperCase();
  if (/^(GL7T|GL5T|GLEX|GLF6|GLF5|GA7C|GA8C|GA6C|FA7X|FA8X|FA6X|GX6C)$/.test(t) || c >= 5) return { gph: 450, ktas: 470, family: 'heavy' };
  if (/^(CL60|CL30|CL35|F900|F2TH|C700|E550|E35L)$/.test(t) || c === 4) return { gph: 320, ktas: 460, family: 'mid-heavy' };
  if (/^(C680|C68A|C56X|C750|E545|E55P|LJ75|LJ60|FA50)$/.test(t) || c === 3) return { gph: 250, ktas: 450, family: 'mid' };
  if (/^(C25A|C25B|C25C|C525|C510|C500|HDJT|HA4T|LJ35|LJ45|E50P|SF50|PRM1|BE40)$/.test(t) || c === 2) return { gph: 160, ktas: 420, family: 'light' };
  if (/^(PC12|PC24|B350|BE20|BE9L|TBM7|TBM8|TBM9|C208|P46T|PAY3|MU2)$/.test(t)) return { gph: 70, ktas: 270, family: 'turboprop' };
  if (c <= 1) return { gph: 15, ktas: 140, family: 'piston' };
  return { gph: 200, ktas: 400, family: 'mid' };
}
function fuelFor(type, otherApt, price) {
  var ll = aptLL(otherApt); if (!ll) return null;
  var nm = gcNm(KSFO_LL, ll); if (!(nm > 0)) return null;
  var p = burnProfile(type), hrs = (nm / p.ktas) * 1.15, gal = Math.round(p.gph * hrs);
  return { gal: gal, usd: Math.round(gal * price), nm: Math.round(nm), family: p.family, gph: p.gph, ktas: p.ktas, est: true };
}

// Operator from the ICAO 3-letter callsign prefix only (published ICAO designators — no owner names).
var OPS = {
  EJA: { name: 'NetJets', kind: 'fractional' }, EJM: { name: 'Executive Jet Mgmt', kind: 'management' },
  LXJ: { name: 'Flexjet', kind: 'fractional' }, VJT: { name: 'VistaJet', kind: 'membership' },
  VJA: { name: 'Vista America', kind: 'membership' }, XSR: { name: 'Airshare', kind: 'fractional' },
  KOW: { name: 'Baker Aviation', kind: 'charter' }
};
function opFor(callsign) {
  var m = String(callsign || '').toUpperCase().match(/^([A-Z]{3})\d/);
  if (!m || !OPS[m[1]]) return null;
  return { code: m[1], name: OPS[m[1]].name, kind: OPS[m[1]].kind };
}
// Known VIP tails (same list v1 uses). Manual 'VIP' ramp flag always wins.
var VIP_TAILS = { N950X: 1, N650SB: 1, N808XX: 1, N88L: 1, N652MP: 1, N717NT: 1, N802BC: 1, N800DL: 1, N512GV: 1 };

// ---- ramp spots (same names v1 stores in ramp_state.spot) ----------------------------------
var SPOTS = [
  ['Spot A', 'A', [1], 1, 'Front row'], ['Spot 1', '1', [1, 2], 1, 'Front row'], ['Spot 2', '2', [1, 2], 1, 'Front row'], ['Spot 3', '3', [1, 2, 3], 2, 'Front row'],
  ['Spot 4', '4', [3, 4, 5], 2, 'Front row'], ['Spot 5', '5', [3, 4, 5], 2, 'Front row'], ['Ken Salvage', 'KEN', [2, 3], 3, 'Front row'],
  ['2nd Line 1', '2L-1', [1, 2, 3], 4, '2nd line'], ['2nd Line 2', '2L-2', [1, 2, 3], 4, '2nd line'], ['2nd Line 3', '2L-3', [1, 2, 3], 4, '2nd line'], ['2nd Line 4', '2L-4', [1, 2, 3], 4, '2nd line'],
  ['Overflow 1', 'OF-1', [4, 5], 5, 'Overflow'], ['Overflow 2', 'OF-2', [4, 5], 5, 'Overflow'],
  ['3rd Line 1', '3L-1', [1, 2, 3], 5, '3rd line'], ['3rd Line 2', '3L-2', [1, 2, 3], 5, '3rd line'], ['3rd Line 3', '3L-3', [1, 2, 3], 5, '3rd line'], ['3rd Line 4', '3L-4', [1, 2, 3], 5, '3rd line'],
  ['3rd Line 5', '3L-5', [1, 2, 3], 5, '3rd line'], ['3rd Line 6', '3L-6', [1, 2, 3], 5, '3rd line'], ['3rd Line 7', '3L-7', [1, 2, 3], 5, '3rd line'], ['3rd Line 8', '3L-8', [1, 2, 3], 5, '3rd line'],
  ['Btwn Hangars 1', 'BH-1', [4, 5], 6, 'Between hangars'], ['Btwn Hangars 2', 'BH-2', [4, 5], 6, 'Between hangars'], ['Btwn Hangars 3', 'BH-3', [4, 5], 6, 'Between hangars'], ['Btwn Hangars 4', 'BH-4', [4, 5], 6, 'Between hangars'],
  ['The Shop', 'SHOP', [3, 4], 6, 'Other'], ['Airfield Safety 1', 'AFS-1', [4, 5], 6, 'Other'], ['Airfield Safety 2', 'AFS-2', [4, 5], 6, 'Other'],
  ['The Island', 'ISLE', [4, 5], 7, 'Tow only'], ['The Fence', 'FENCE', [1, 2], 7, 'Tow only'],
  ['42 West 1', '42W-1', [4, 5], 8, '42 West'], ['42 West 2', '42W-2', [4, 5], 8, '42 West'], ['42 West 3', '42W-3', [4, 5], 8, '42 West'], ['42 West 4', '42W-4', [4, 5], 8, '42 West'],
  ['41-7 A', '41-7A', [4, 5, 6], 9, 'Remote'], ['41-7 B', '41-7B', [4, 5, 6], 9, 'Remote'], ['41-11', '41-11', [4, 5], 9, 'Remote'],
  ['4th Line 1', '4L-1', [4, 5], 99, 'United approval'], ['4th Line 2', '4L-2', [4, 5], 99, 'United approval'], ['4th Line 3', '4L-3', [4, 5], 99, 'United approval'], ['4th Line 4', '4L-4', [4, 5], 99, 'United approval']
].map(function (s) { return { name: s[0], short: s[1], cats: s[2], pri: s[3], area: s[4], restricted: s[3] === 99, towOnly: s[3] === 7 }; });
var SPOT_BY_NAME = {}; SPOTS.forEach(function (s) { SPOT_BY_NAME[s.name.toUpperCase()] = s; });
function spotDef(name) { return name ? SPOT_BY_NAME[String(name).trim().toUpperCase()] || null : null; }

// ---- roles: who sees what, who may edit what (server already enforces writes per view) -----
var ROLES = {
  dispatch: { label: 'Dispatch', edit: ['spot', 'pax', 'flags', 'towNotes'], writeView: 'dispatch', gm: true },
  line: { label: 'Line room', edit: ['spot'], writeView: 'line-room', gm: false },
  lobby: { label: 'Lobby', edit: [], writeView: null, gm: false, lobby: true },
  gm: { label: 'GM', edit: [], writeView: null, gm: true }
};

function key(s) { return String(s || '').toUpperCase().replace(/[^A-Z0-9]/g, ''); }
function ms(iso) { if (!iso) return 0; var t = Date.parse(iso); return isNaN(t) ? 0 : t; }
function isoOf(t) { return t ? new Date(t).toISOString() : ''; }
function isSwimish(s) { s = String(s || ''); return /swim|tfms|sfdps|fdps/.test(s); }
function ptHour(t) { return parseInt(new Date(t).toLocaleString('en-US', { timeZone: 'America/Los_Angeles', hour: '2-digit', hourCycle: 'h23' }), 10); }

// Data confidence: where the row's time comes from and how fresh it is.
function confidence(f, now, kind) {
  var plan = isSwimish(f.source) || f.timeSource === 'swim';
  var liveAge = f.lastAdsbAt ? Math.round((now - f.lastAdsbAt) / 1000) : null;
  var live = liveAge != null && liveAge < 300;
  var upd = f.lastUpdate ? Math.round((now - f.lastUpdate) / 1000) : null;
  var hasTime = kind === 'arr' ? !!f.arriveISO : !!f.departISO;
  var level, src, note;
  if (plan && live) { level = 3; src = 'FAA plan + ADS-B'; note = 'Filed FAA flight plan (SWIM/TFMS), confirmed by a live ADS-B position ' + liveAge + 's ago.'; }
  else if (plan && hasTime) { level = 2; src = 'FAA plan'; note = 'Time from the filed FAA flight plan (SWIM/TFMS)' + (f.etaSrc === 'traj' ? ', TFMS trajectory update.' : '.'); }
  else if (!plan && live) { level = 1; src = 'ADS-B est'; note = 'No filed plan matched; ETA estimated from live ADS-B position/speed.'; }
  else if (plan) { level = 1; src = 'FAA plan, no time'; note = 'Flight plan seen but no ' + (kind === 'arr' ? 'ETA' : 'ETD') + ' yet.'; }
  else { level = 0; src = 'ADS-B (stale)'; note = 'Seen on ADS-B only; no live position now.'; }
  return { level: level, src: src, note: note, liveAgeSec: liveAge, updatedAgoSec: upd };
}

function createV2(deps) {
  var cache = { at: 0, data: null, pending: null };

  function baseRow(f, kind, now) {
    var ident = f.ident || '';
    var cs = f.callsign && key(f.callsign) !== key(ident) ? f.callsign : '';
    var ladd = ident === deps.LADD_MASK;
    var other = kind === 'arr' ? f.from : f.to;
    var city = f.city || (aptLL(other) || {}).city || '';
    var type = String(f.type || '').toUpperCase();
    return {
      id: key(ident) || key(f.callsign), rampId: key(ident), ident: ident, callsign: cs, reg: f.reg || '', type: type, model: f.model || '',
      cat: wsCat(type), heli: !!HELI[type], kind: kind,
      from: f.from || '', to: f.to || '', city: city, intl: !!f.intl, country: f.country || '',
      fromNote: f.fromNote || '', ladd: ladd,
      spot: f.spot || '', pax: (f.pax === 0 || f.pax) ? f.pax : null, flags: Array.isArray(f.flags) ? f.flags : [], towNotes: f.towNotes || '',
      op: opFor(f.callsign), conf: confidence(f, now, kind),
      live: f.lastAdsbAt ? { ageSec: Math.round((now - f.lastAdsbAt) / 1000), alt: f.alt != null ? f.alt : null, gs: f.gs != null ? f.gs : null, distNm: f.distNm != null ? f.distNm : null, evidence: f.adsbEvidence || '' } : null
    };
  }

  async function compute() {
    var now = Date.now();
    var arrRaw = await deps.buildBoard('arrivals');
    var depRaw = await deps.buildBoard('departures');
    var win = deps.boardWindowInfo();
    var price = deps.fuelPrice;
    var status = {};
    try { status = deps.status() || {}; } catch (e) { status = {}; }

    // departures index: departed-after-landing check + matched ground times
    var depByKey = {};
    depRaw.forEach(function (d) {
      [key(d.ident), key(d.callsign), key(d.reg)].forEach(function (k) { if (k && k !== key(deps.LADD_MASK)) (depByKey[k] = depByKey[k] || []).push(d); });
    });
    function departedAfter(f, landedMs) {
      var ks = [key(f.ident), key(f.callsign), key(f.reg)];
      for (var i = 0; i < ks.length; i++) {
        var list = depByKey[ks[i]] || [];
        for (var j = 0; j < list.length; j++) { var d = list[j]; if (d.departed && ms(d.departISO) > landedMs + 10 * MIN) return d; }
      }
      return null;
    }

    var arrivals = [], onGround = [], departures = [], departed = [];
    var landedByKey = {};

    arrRaw.forEach(function (f) {
      var r = baseRow(f, 'arr', now);
      var eta = ms(f.arriveISO), dep = ms(f.departISO);
      var landed = !!(f.arrived || f.onGround);
      var landedMs = landed ? (f.landedAt || eta || 0) : 0;
      var nd = ms(f.nextDepartISO);
      r.etaISO = f.arriveISO || ''; r.originDepISO = f.departISO || '';
      r.nextDepartISO = f.nextDepartISO || ''; r.nextDepartTo = f.nextDepartTo || '';
      var st;
      if (f.divertTo) {
        var holdUntil = (f.divertAt || now) + 30 * MIN;
        r.divert = { to: String(f.divertTo).toUpperCase(), holdUntilISO: isoOf(holdUntil) };
        st = { code: 'diverted', label: 'Diverted ' + r.divert.to };
        arrivals.push(r);
      } else if (landed || (eta && now - eta > 30 * MIN && f.landPending === undefined)) {
        var assumed = !landed;
        if (assumed) landedMs = eta;
        var gone = departedAfter(f, landedMs);
        if (gone) return; // a matching wheels-off after this landing: it has left, don't show on ground
        var sinceMin = (now - landedMs) / MIN;
        st = assumed ? { code: 'landed_unconfirmed', label: 'Landed?', note: 'ETA passed ' + Math.round(sinceMin) + ' min ago; no landing report seen.' }
          : sinceMin < 15 ? { code: 'landed', label: 'Landed' } : { code: 'onground', label: 'On ground' };
        r.landedISO = isoOf(landedMs); r.landedAssumed = assumed;
        r.stale = sinceMin > 12 * 60 && !(nd && nd > now - 3 * HOUR);
        onGround.push(r);
        [key(f.ident), key(f.callsign), key(f.reg)].forEach(function (k) { if (k) landedByKey[k] = landedMs; });
      } else {
        var mins = eta ? (eta - now) / MIN : null;
        var liveFinal = r.live && r.live.ageSec < 180 && ((r.live.evidence && /final/i.test(r.live.evidence)) || (r.live.distNm != null && r.live.distNm <= 12));
        if (mins != null && mins <= 0) st = liveFinal ? { code: 'final', label: 'Final' } : { code: 'due', label: 'Due', note: 'ETA passed, no landing seen yet.' };
        else if (liveFinal || (mins != null && mins <= 5)) st = { code: 'final', label: 'Final', est: !liveFinal };
        else if (dep && dep > now) st = { code: 'filed', label: 'Filed' };
        else if (dep && now - dep < 20 * MIN) st = { code: 'departed_origin', label: 'Departed ' + (r.from || 'origin') };
        else if (eta) st = { code: 'enroute', label: 'En route' };
        else st = { code: 'filed', label: 'Filed', note: 'No ETA in the plan yet.' };
        arrivals.push(r);
      }
      r.status = st;
      // ON BASE
      if (landed || st.code === 'landed_unconfirmed') {
        if (nd && nd > now) r.ob = { mode: 'countdown', toISO: r.nextDepartISO };
        else if (nd && now - nd <= 3 * HOUR) r.ob = { mode: 'overdue', toISO: r.nextDepartISO };
        else r.ob = { mode: 'none', sinceISO: r.landedISO };
      } else if (eta && nd && nd > eta) r.ob = { mode: 'planned', fromISO: r.etaISO, toISO: r.nextDepartISO };
      else r.ob = { mode: 'none' };
      var startMs = landedMs || eta;
      r.stayHrs = (startMs && nd && nd > startMs) ? (nd - startMs) / HOUR : null;
      r.fuel = r.nextDepartTo ? fuelFor(r.type, r.nextDepartTo, price) : null; // outbound leg = uplift
      r.legNm = (function () { var ll = aptLL(r.from); return ll ? Math.round(gcNm(KSFO_LL, ll)) : null; })();
    });

    depRaw.forEach(function (f) {
      var r = baseRow(f, 'dep', now);
      var etd = ms(f.departISO);
      r.etdISO = f.departISO || ''; r.etaDestISO = f.arriveISO || '';
      var k = [key(f.ident), key(f.callsign), key(f.reg)];
      var landedMs = 0; k.forEach(function (x) { if (x && landedByKey[x]) landedMs = landedByKey[x]; });
      if (f.departed) {
        if (!etd || now - etd > 6 * HOUR) return;
        r.status = { code: 'off', label: 'Off', actual: !!f.departActual };
        // A wheels-off stamped in the future is a filed ETD, not an actual: don't show it as the off time.
        if (etd > now + 2 * MIN) { r.offISO = ''; r.status.note = 'Departure seen, but no wheels-off time reported yet (filed ETD ' + new Date(etd).toLocaleTimeString('en-US', { timeZone: 'America/Los_Angeles', hour: '2-digit', minute: '2-digit', hourCycle: 'h23' }) + ' PT).'; }
        else r.offISO = r.etdISO;
        r.ob = { mode: 'none' };
        departed.push(r);
      } else {
        if (etd && now - etd > 3 * HOUR) return; // stale ETD, never departed: not tonight's board
        var mins = etd ? (etd - now) / MIN : null;
        var taxi = r.live && r.live.ageSec < 120 && r.live.gs != null && r.live.gs >= 3 && r.live.gs <= 45 && (r.live.alt == null || r.live.alt <= 0);
        if (taxi) r.status = { code: 'taxi', label: 'Taxi out' };
        else if (mins != null && mins < -5) r.status = { code: 'late', label: 'Late', note: 'ETD passed, no wheels-off seen.' };
        else if (mins != null && mins <= 15) r.status = { code: 'due', label: 'Due' };
        else r.status = { code: 'filed', label: 'Filed', note: etd ? '' : 'No ETD in the plan yet.' };
        r.ob = etd ? { mode: etd > now ? 'countdown' : 'overdue', toISO: r.etdISO } : { mode: 'none' };
        departures.push(r);
      }
      r.onBaseSinceISO = landedMs ? isoOf(landedMs) : '';
      r.stayHrs = (landedMs && etd > landedMs) ? (etd - landedMs) / HOUR : null;
      r.fuel = fuelFor(r.type, r.to, price);
      r.legNm = r.fuel ? r.fuel.nm : null;
    });

    // VIP + QT
    function vipOf(r) {
      var manual = r.flags.some(function (x) { return /^VIP\b/i.test(String(x)); });
      var known = !!(VIP_TAILS[key(r.ident)] || VIP_TAILS[key(r.callsign)]);
      var uhnw = known || (r.op && (r.op.kind === 'fractional' || r.op.kind === 'membership')) || r.cat >= 4;
      var reasons = [];
      var shortStay = r.stayHrs != null && r.stayHrs > 0 && r.stayHrs <= 2;
      var outNm = r.kind === 'dep' ? r.legNm : (r.nextDepartTo ? (function () { var ll = aptLL(r.nextDepartTo); return ll ? gcNm(KSFO_LL, ll) : null; })() : null);
      var longLeg = (r.legNm != null && r.legNm >= 1200) || (outNm != null && outNm >= 1200);
      if (r.op && uhnw) reasons.push(r.op.name + ' (' + r.op.kind + ')');
      if (r.cat >= 4) reasons.push(CAT_LABEL[r.cat] + ' cabin');
      if (shortStay) reasons.push('quick turn ' + r.stayHrs.toFixed(1) + 'h');
      if (longLeg) reasons.push('long leg ≥1,200 nm');
      if (known) reasons.push('known VIP tail');
      var heur = !!(uhnw && (shortStay || longLeg || known));
      return { vip: manual || heur, manual: manual, est: !manual && heur, reasons: manual ? ['flagged VIP by dispatch'] : reasons };
    }
    [arrivals, onGround, departures, departed].forEach(function (list) {
      list.forEach(function (r) {
        r.vip = vipOf(r);
        r.qt = r.stayHrs != null && r.stayHrs > 0 && r.stayHrs <= 2;
        r.tasks = tasksOf(r);
      });
    });

    // Row ids must be unique per kind (the server may hold a later leg for the same tail).
    var used = {};
    [arrivals, onGround, departures, departed].forEach(function (list) {
      list.forEach(function (r) { var k = r.kind + ':' + r.id, n = 1; while (used[k]) { n++; k = r.kind + ':' + r.id + '~' + n; } if (n > 1) r.id = r.id + '~' + n; used[k] = 1; });
    });
    function tOf(r) { return ms(r.etaISO || r.etdISO || r.originDepISO) || Infinity; }
    arrivals.sort(function (a, b) { return tOf(a) - tOf(b); });
    departures.sort(function (a, b) { return tOf(a) - tOf(b); });
    onGround.sort(function (a, b) { return (b.stale ? 0 : 1) - (a.stale ? 0 : 1) || ms(b.landedISO) - ms(a.landedISO); });
    departed.sort(function (a, b) { return (ms(b.offISO) || now) - (ms(a.offISO) || now); });

    var timeline = buildTimeline(now, arrivals, onGround, departures);
    // attach suggestions to rows (never written; dispatch/line confirm by tapping)
    var sugById = {}; timeline.suggestions.forEach(function (s) { sugById[s.id + ':' + s.kind] = s; });
    arrivals.concat(onGround).forEach(function (r) { var s = sugById[r.id + ':arr']; if (s) r.suggest = { spot: s.spot, short: s.short, why: s.why }; });

    // Now strip
    var nextArr = arrivals.filter(function (r) { return r.status.code !== 'diverted' && r.etaISO && ms(r.etaISO) > now - 5 * MIN; }).slice(0, 3)
      .map(function (r) { return { id: r.id, ident: r.ident, type: r.type, from: r.from, intl: r.intl, etaISO: r.etaISO, vip: r.vip.vip, status: r.status.code, spot: r.spot }; });
    var vipDue = arrivals.filter(function (r) { var t = ms(r.etaISO); return r.vip.vip && t && t - now <= 60 * MIN && t > now - 5 * MIN; });
    var depNext60 = departures.filter(function (r) { var t = ms(r.etdISO); return t && t - now <= 60 * MIN; });
    var nowStrip = {
      nextArrivals: nextArr,
      onGround: onGround.length, onGroundRecent: onGround.filter(function (r) { return !r.stale; }).length,
      onGroundNoSpot: onGround.filter(function (r) { return !r.stale && !r.spot; }).length,
      spotsOpen: timeline.openNow, spotsTotal: timeline.usableTotal, closeSpotsOpen: timeline.closeOpenNow,
      vipDue60: vipDue.length, vipDue60Idents: vipDue.map(function (r) { return r.ident; }),
      departuresNext60: depNext60.length, intlDue: arrivals.filter(function (r) { return r.intl && r.status.code !== 'diverted'; }).length,
      conflicts: timeline.conflicts.length
    };

    // GM summary + hourly profile
    var endMs = win.endMs || (now + 18 * HOUR);
    var upcomingDeps = departures.filter(function (r) { var t = ms(r.etdISO); return !t || t < endMs; });
    var fuelGal = 0, fuelUsd = 0, fuelN = 0, fuelMissing = 0;
    upcomingDeps.forEach(function (r) { if (r.fuel) { fuelGal += r.fuel.gal; fuelUsd += r.fuel.usd; fuelN++; } else fuelMissing++; });
    var hourly = [];
    var h0 = Math.floor((now - 6 * HOUR) / HOUR) * HOUR, h1 = Math.min(endMs, now + 24 * HOUR);
    for (var t = h0; t < h1; t += HOUR) hourly.push({ startISO: isoOf(t), hourPT: ptHour(t), arr: 0, dep: 0, past: t + HOUR <= now });
    function bucket(t, field) { if (!t) return; var i = Math.floor((t - h0) / HOUR); if (i >= 0 && i < hourly.length) hourly[i][field]++; }
    arrivals.forEach(function (r) { if (r.status.code !== 'diverted') bucket(ms(r.etaISO), 'arr'); });
    onGround.forEach(function (r) { bucket(ms(r.landedISO), 'arr'); });
    departures.forEach(function (r) { bucket(ms(r.etdISO), 'dep'); });
    departed.forEach(function (r) { bucket(ms(r.offISO) || now, 'dep'); });
    // Peak = busiest hour from the current hour to the end of the window.
    var curHourIso = isoOf(Math.floor(now / HOUR) * HOUR), peak = null;
    hourly.forEach(function (h) { var n = h.arr + h.dep; if (n > 0 && h.startISO >= curHourIso && (!peak || n > peak.count)) peak = { startISO: h.startISO, hourPT: h.hourPT, count: n, arr: h.arr, dep: h.dep }; });
    var gm = {
      arrivalsRemaining: arrivals.filter(function (r) { return r.status.code !== 'diverted'; }).length,
      departuresRemaining: upcomingDeps.length,
      movementsRemaining: arrivals.filter(function (r) { return r.status.code !== 'diverted'; }).length + upcomingDeps.length,
      landedLast6h: onGround.filter(function (r) { return now - ms(r.landedISO) <= 6 * HOUR; }).length,
      departedLast6h: departed.length,
      fuelGalEst: fuelGal, fuelUsdEst: fuelUsd, fuelLegsEstimated: fuelN, fuelLegsUnknown: fuelMissing,
      vipCount: arrivals.concat(onGround.filter(function (r) { return !r.stale; }), departures).filter(function (r) { return r.vip.vip; }).length,
      peakHour: peak, fuelPricePerGal: price
    };

    var swim = status.swim || {};
    var adsb = status.adsbLol || {};
    return {
      generatedAt: isoOf(now), nowMs: now, airport: 'KSFO', airportName: 'San Francisco Intl',
      window: { mode: win.mode, label: win.label, endISO: win.endISO, endPT: win.endPT, sunrisePT: win.sunrisePT, sunsetPT: win.sunsetPT, nextSunrisePT: win.nextSunrisePT },
      feed: {
        swim: { connected: !!swim.connected, msgs: swim.msgs || 0, reason: swim.reason || '' },
        adsb: { ok: !!adsb.ok, lastFetchAgo: adsb.lastFetchAgo != null ? adsb.lastFetchAgo : null, error: adsb.lastError || '', backoffSec: adsb.backoffSecondsRemaining || 0 },
        rampEditEnabled: !!(status.rampEdit && status.rampEdit.enabled), ladd: !!(status.ladd && status.ladd.loaded)
      },
      sections: { arrivals: arrivals, onGround: onGround, departures: departures, departed: departed },
      now: nowStrip, timeline: timeline, gm: gm, hourly: hourly,
      spots: SPOTS.map(function (s) { return { name: s.name, short: s.short, cats: s.cats, pri: s.pri, area: s.area, restricted: s.restricted, towOnly: s.towOnly }; })
    };
  }

  function tasksOf(r) {
    // Task cues from dispatch flags (ramp_state.flags). "X ✓" marks a cue done.
    var out = [], seen = {};
    var cues = ['TOW', 'FUEL', 'GPU', 'LAV', 'CATERING', 'CUSTOMS'];
    r.flags.forEach(function (fl) {
      var s = String(fl).toUpperCase().trim(), done = /\s*✓$/.test(s), base = s.replace(/\s*✓$/, '');
      if (cues.indexOf(base) >= 0 && !seen[base]) { seen[base] = 1; out.push({ code: base, done: done }); }
    });
    if (r.intl && r.kind === 'arr' && !seen.CUSTOMS) out.push({ code: 'CUSTOMS', done: false, auto: true });
    if (r.towNotes && !seen.TOW) out.push({ code: 'TOW', done: false, auto: true, note: r.towNotes });
    return out;
  }

  function buildTimeline(now, arrivals, onGround, departures) {
    var start = now - 2 * HOUR, end = now + 12 * HOUR;
    var lanes = {}; SPOTS.forEach(function (s) { lanes[s.name] = { spot: s.name, short: s.short, pri: s.pri, area: s.area, cats: s.cats, restricted: s.restricted, towOnly: s.towOnly, items: [] }; });
    var seenId = {};
    function add(r, s, e, kind, openEnd) {
      var sd = spotDef(r.spot);
      var laneName = sd ? sd.name : r.spot;
      if (!lanes[laneName]) lanes[laneName] = { spot: r.spot, short: String(r.spot).slice(0, 6), pri: 50, area: 'Other (free text)', cats: [1, 2, 3, 4, 5, 6], items: [], custom: true };
      lanes[laneName].items.push({ id: r.id, ident: r.ident, type: r.type, cat: r.cat, vip: !!(r.vip && r.vip.vip), startISO: isoOf(s), endISO: isoOf(e), openEnd: !!openEnd, kind: kind, status: r.status.code, fit: !sd || sd.cats.indexOf(r.cat) >= 0 });
    }
    onGround.forEach(function (r) {
      if (r.stale || !r.spot) return; seenId[r.id] = 1;
      var nd = ms(r.nextDepartISO), s = ms(r.landedISO) || start;
      if (nd && nd > now - 3 * HOUR) add(r, s, Math.max(nd, now), 'ground', false); else add(r, s, end, 'ground', true);
    });
    arrivals.forEach(function (r) {
      if (!r.spot || r.status.code === 'diverted') return; seenId[r.id] = 1;
      var eta = ms(r.etaISO); if (!eta) return; var nd = ms(r.nextDepartISO);
      if (nd && nd > eta) add(r, eta, nd, 'planned', false); else add(r, eta, end, 'planned', true);
    });
    departures.forEach(function (r) {
      if (!r.spot || seenId[r.id]) return;
      var etd = ms(r.etdISO) || now; add(r, start, Math.max(etd, now), 'ground', false);
    });
    var laneList = Object.keys(lanes).map(function (k) { return lanes[k]; });
    var conflicts = [];
    laneList.forEach(function (l) {
      l.items.sort(function (a, b) { return ms(a.startISO) - ms(b.startISO); });
      for (var i = 0; i < l.items.length; i++) for (var j = i + 1; j < l.items.length; j++) {
        var a = l.items[i], b = l.items[j];
        if (ms(b.startISO) < ms(a.endISO) && ms(a.startISO) < ms(b.endISO)) { a.conflict = b.conflict = true; conflicts.push({ spot: l.spot, a: a.ident, b: b.ident, atISO: b.startISO }); }
      }
    });
    function busy(l, s, e) { return l.items.some(function (it) { return ms(it.startISO) < e && s < ms(it.endISO); }); }
    var openNow = 0, usable = 0, closeOpen = 0;
    laneList.forEach(function (l) { if (l.custom || l.restricted) return; usable++; if (!busy(l, now, now + 1)) { openNow++; if (l.pri <= 2) closeOpen++; } });

    // Greedy spot suggestions for unassigned inbound + recent on-ground aircraft (ETA order).
    var suggestions = [];
    // Inbound only: an aircraft already on the ground is parked somewhere we don't know yet.
    var cand = arrivals.filter(function (r) { return !r.spot && r.status.code !== 'diverted' && ms(r.etaISO); });
    cand.sort(function (a, b) { return (ms(a.etaISO || a.landedISO)) - (ms(b.etaISO || b.landedISO)); });
    cand.forEach(function (r) {
      var s = Math.max(now, ms(r.etaISO));
      var nd = ms(r.nextDepartISO), stay = r.stayHrs, assumed = false;
      if (!(nd && nd > s)) { stay = 24; assumed = true; nd = s + 24 * HOUR; }
      var order = preferredOrder(r, stay, r.vip && r.vip.vip);
      for (var i = 0; i < order.length; i++) {
        var l = lanes[order[i].name];
        if (!busy(l, s, nd)) {
          l.items.push({ id: r.id, ident: r.ident, type: r.type, cat: r.cat, vip: !!(r.vip && r.vip.vip), startISO: isoOf(s), endISO: isoOf(nd), openEnd: assumed, kind: 'suggested', status: r.status.code, fit: true });
          var why = [];
          if (r.vip && r.vip.vip) why.push('VIP → closest free spot');
          else if (stay <= 2) why.push('quick turn ' + stay.toFixed(1) + 'h → front row');
          else if (assumed) why.push('no ETD filed → treated as long stay');
          else why.push(stay.toFixed(1) + 'h stay');
          why.push('fits ' + CAT_LABEL[r.cat]);
          suggestions.push({ id: r.id, kind: 'arr', ident: r.ident, spot: l.spot, short: l.short, why: why.join(' · '), est: true });
          break;
        }
      }
    });
    laneList.forEach(function (l) { l.items.sort(function (a, b) { return ms(a.startISO) - ms(b.startISO); }); });
    laneList.sort(function (a, b) { return a.pri - b.pri; });
    return { startISO: isoOf(start), endISO: isoOf(end), nowISO: isoOf(now), lanes: laneList, conflicts: conflicts, suggestions: suggestions, openNow: openNow, closeOpenNow: closeOpen, usableTotal: usable };
  }

  function preferredOrder(r, stay, vip) {
    var cat = r.cat;
    var fits = SPOTS.filter(function (s) { return !s.restricted && s.cats.indexOf(cat) >= 0; });
    if (r.heli) { var h = fits.filter(function (s) { return /^42 West/.test(s.name); }); return h.concat(fits.filter(function (s) { return h.indexOf(s) < 0; })); }
    var rank;
    if (vip || stay <= 2) rank = function (s) { return s.pri + (s.towOnly ? 50 : 0); };
    else if (stay <= 8) rank = function (s) { return (s.pri === 4 || s.pri === 6 ? 0 : s.pri <= 2 ? 30 : 10 + s.pri) + (s.towOnly ? 50 : 0); };
    else rank = function (s) { return (s.pri >= 5 && s.pri <= 8 ? s.pri - 5 : s.pri <= 2 ? 40 : 20 + s.pri) + (s.towOnly ? 3 : 0); };
    return fits.slice().sort(function (a, b) { return rank(a) - rank(b) || SPOTS.indexOf(a) - SPOTS.indexOf(b); });
  }

  async function getFull() {
    if (cache.data && Date.now() - cache.at < 3000) return cache.data;
    if (cache.pending) return cache.pending;
    cache.pending = compute().then(function (d) { cache.data = d; cache.at = Date.now(); cache.pending = null; return d; }, function (e) { cache.pending = null; throw e; });
    return cache.pending;
  }

  function project(full, roleName) {
    var role = ROLES[roleName] || ROLES.dispatch;
    var out = Object.assign({}, full, { role: { name: roleName in ROLES ? roleName : 'dispatch', label: role.label, edit: role.edit, writeView: role.writeView } });
    if (role.lobby) {
      var slim = function (r) { return { id: r.id, ident: r.ident, type: r.type, model: r.model, kind: r.kind, status: { code: r.status.code, label: r.status.label }, etaISO: r.etaISO, etdISO: r.etdISO, landedISO: r.landedISO, offISO: r.offISO }; };
      var now = full.nowMs;
      var notL = function (r) { return !r.ladd; };
      out.sections = {
        arrivals: full.sections.arrivals.filter(notL).filter(function (r) { return r.status.code !== 'diverted'; }).map(slim)
          .concat(full.sections.onGround.filter(notL).filter(function (r) { return now - Date.parse(r.landedISO) <= 30 * MIN; }).map(slim)),
        departures: full.sections.departures.filter(notL).map(slim)
          .concat(full.sections.departed.filter(notL).filter(function (r) { return !r.offISO || now - Date.parse(r.offISO) <= 30 * MIN; }).map(slim))
      };
      delete out.timeline; delete out.gm; delete out.now; delete out.hourly; delete out.spots; delete out.feed;
      return out;
    }
    if (!role.gm) {
      var strip = function (r) { var c = Object.assign({}, r); delete c.fuel; return c; };
      out.sections = {}; Object.keys(full.sections).forEach(function (k) { out.sections[k] = full.sections[k].map(strip); });
      delete out.gm;
    }
    return out;
  }

  var PUB = path.join(deps.PUBLIC_DIR, 'v2');
  var TYPES = { '.js': 'text/javascript; charset=utf-8', '.css': 'text/css; charset=utf-8', '.svg': 'image/svg+xml', '.png': 'image/png', '.json': 'application/json' };

  async function handle(req, res, pathname, query) {
    var m = pathname.match(/^\/v2(?:\/(dispatch|line|lobby|gm))?\/?$/);
    if (m) {
      if (!m[1]) { res.writeHead(302, { Location: '/v2/dispatch' + (req.url.indexOf('?') >= 0 ? req.url.slice(req.url.indexOf('?')) : '') }); res.end(); return true; }
      fs.readFile(path.join(PUB, 'index.html'), function (err, data) {
        if (err) { res.writeHead(404); res.end('not found'); return; }
        res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-cache' }); res.end(data);
      });
      return true;
    }
    if (pathname.indexOf('/v2/assets/') === 0) {
      var rel = pathname.slice('/v2/assets/'.length);
      if (!rel || rel.indexOf('..') >= 0 || rel.indexOf('/') >= 0) { res.writeHead(400); res.end('bad path'); return true; }
      var fp = path.join(PUB, rel);
      fs.readFile(fp, function (err, data) {
        if (err) { res.writeHead(404); res.end('not found'); return; }
        res.writeHead(200, { 'Content-Type': TYPES[path.extname(fp)] || 'application/octet-stream', 'Cache-Control': 'no-cache' }); res.end(data);
      });
      return true;
    }
    if (pathname === '/api/v2/board' && (req.method === 'GET' || req.method === 'HEAD')) {
      if (deps.touchActivity) deps.touchActivity();
      try {
        var full = await getFull();
        deps.sendJSON(res, 200, project(full, String((query && query.view) || 'dispatch')));
      } catch (e) { deps.sendJSON(res, 500, { error: 'v2 board failed: ' + e.message }); }
      return true;
    }
    return false;
  }

  return { handle: handle, compute: compute, project: project, _internals: { wsCat: wsCat, fuelFor: fuelFor, opFor: opFor, SPOTS: SPOTS } };
}

module.exports = createV2;
