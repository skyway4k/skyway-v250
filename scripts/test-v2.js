// Fixture checks for lib/v2board.js (no DB). node scripts/test-v2.js
var assert = require('assert'), path = require('path');
var H = 3600000, now = Date.now(), iso = function (d) { return new Date(now + d).toISOString(); };
var arr = [
  { ident: 'N1AB', callsign: 'EJA1', type: 'GLF6', from: 'KTEB', arriveISO: iso(20 * 60000), departISO: iso(-4 * H), source: 'swim', timeSource: 'swim', nextDepartISO: iso(1.5 * H), nextDepartTo: 'KTEB', spot: '', flags: [] },
  { ident: 'N2CD', type: 'C25B', from: 'KSBA', arriveISO: iso(-2 * H), arrived: true, onGround: true, landedAt: now - 2 * H, source: 'swim', nextDepartISO: iso(-5 * H), spot: 'Spot 1', flags: ['FUEL', 'GPU ✓'] },
  { ident: 'BLOCKED', type: 'GLEX', from: 'KVNY', arriveISO: iso(40 * 60000), source: 'swim' },
  { ident: 'N3EF', type: 'PC12', from: 'KOAK', arriveISO: iso(-50 * 60000), source: 'swim' }, // ETA long past, no landing → on ground, unconfirmed
  { ident: 'N4GH', type: 'CL35', from: 'MMSD', intl: true, arriveISO: iso(2 * H), source: 'swim', spot: 'Spot 1' }
];
var dep = [
  { ident: 'N2CD', type: 'C25B', to: 'KLAS', departISO: iso(-4 * H), departed: false },
  { ident: 'N9ZZ', type: 'GLF5', to: 'KTEB', departISO: iso(-2 * H), departed: true },
  { ident: 'N8YY', type: 'E55P', to: 'KSAN', departISO: iso(-10 * H), departed: false },
  { ident: 'N7XX', type: 'E55P', to: 'KSAN', departISO: iso(30 * 60000), departed: false }
];
var v2 = require('../lib/v2board')({
  buildBoard: async function (k) { return JSON.parse(JSON.stringify(k === 'arrivals' ? arr : dep)); },
  boardWindowInfo: function () { return { mode: 'until-midnight-PT', label: 'through midnight PT', endMs: now + 8 * H }; },
  status: function () { return { swim: { connected: true }, rampEdit: { enabled: true } }; },
  fuelPrice: 7.85, LADD_MASK: 'BLOCKED', PUBLIC_DIR: path.join(__dirname, '..', 'public'), sendJSON: function () { }
});
v2.compute().then(function (full) {
  var s = full.sections, ids = function (l) { return l.map(function (r) { return r.ident; }); };
  assert.deepStrictEqual(ids(s.arrivals).sort(), ['BLOCKED', 'N1AB', 'N4GH']);
  assert.deepStrictEqual(ids(s.onGround).sort(), ['N2CD', 'N3EF']);
  assert.strictEqual(s.onGround.find(function (r) { return r.ident === 'N3EF'; }).status.code, 'landed_unconfirmed');
  assert.deepStrictEqual(ids(s.departures), ['N7XX'], 'stale >3h ETD dropped, departed moved');
  assert.deepStrictEqual(ids(s.departed), ['N9ZZ']);
  var a1 = s.arrivals.find(function (r) { return r.ident === 'N1AB'; });
  assert.strictEqual(a1.ob.mode, 'planned'); assert.ok(a1.qt, 'quick turn'); assert.ok(a1.vip.vip && a1.vip.est, 'VIP heuristic');
  assert.ok(a1.suggest && /^Spot/.test(a1.suggest.spot), 'VIP gets a front-row suggestion: ' + JSON.stringify(a1.suggest));
  var g = s.onGround.find(function (r) { return r.ident === 'N2CD'; });
  assert.strictEqual(g.ob.mode, 'none', 'ETD >3h past ignored');
  assert.deepStrictEqual(g.tasks.map(function (x) { return x.code + (x.done ? '✓' : ''); }), ['FUEL', 'GPU✓']);
  assert.ok(full.timeline.conflicts.length === 0 || full.timeline.conflicts.every(function (c) { return c.spot === 'Spot 1'; }));
  var lob = v2.project(full, 'lobby');
  assert.ok(!JSON.stringify(lob).includes('BLOCKED'), 'lobby hides LADD rows');
  assert.ok(!lob.gm && !lob.timeline && !('callsign' in lob.sections.arrivals[0]), 'lobby is slim');
  var line = v2.project(full, 'line');
  assert.ok(!line.gm && line.sections.departures.every(function (r) { return !r.fuel; }), 'line has no $');
  assert.deepStrictEqual(line.role.edit, ['spot']);
  assert.ok(v2.project(full, 'gm').gm.fuelGalEst > 0);
  console.log('v2 board OK');
}).catch(function (e) { console.error(e); process.exit(1); });
