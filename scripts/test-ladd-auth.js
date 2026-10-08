// Local harness for LADD masking, ramp write auth and /api/swim/arrivals.
// node scripts/test-ladd-auth.js   (needs DATABASE_URL to a scratch Postgres)
process.env.SKYWAY_TEST = '1';
process.env.OSKY_ID = process.env.OSKY_ID || 'x';
process.env.OSKY_SECRET = process.env.OSKY_SECRET || 'x';
process.env.PORT = process.env.PORT || '18767';
const fs = require('fs'), os = require('os'), path = require('path');
const laddFile = path.join(os.tmpdir(), 'skyway-ladd-test.csv');
fs.writeFileSync(laddFile, 'N-Number,Owner,Call Sign\n883TR,Example LLC,\nN467QS,,EJA467\n"9H-VIO",x,\n');
process.env.LADD_FILE = laddFile;
delete process.env.RAMP_EDIT_KEY;
const S = require('../server.js');
const assert = require('assert');
const base = 'http://127.0.0.1:' + process.env.PORT;
function iso(m) { return new Date(Date.now() + m * 60000).toISOString().replace(/\.\d+Z$/, 'Z'); }
function fltd(o) {
  const spec = o.type ? `<nxcm:flightAircraftSpecs equipmentQualifier="L">${o.type}</nxcm:flightAircraftSpecs>` : '';
  const rmk = o.reg ? `<nxcm:flightPlanRemarks>REG/${o.reg}</nxcm:flightPlanRemarks>` : '';
  return `<fdm:fltdMessage acid="${o.acid}" arrArpt="${o.arr}" depArpt="${o.dep}" msgType="flightPlanInformation" sourceTimeStamp="${iso(0)}"${o.priv ? ' dataDistribution="LADD_BLOCKED"' : ''}>
<fdm:flightPlanInformation><nxcm:qualifiedAircraftId><nxce:aircraftId>${o.acid}</nxce:aircraftId><nxce:igtd>${o.etd}</nxce:igtd>
<nxce:departurePoint><nxce:airport>${o.dep}</nxce:airport></nxce:departurePoint><nxce:arrivalPoint><nxce:airport>${o.arr}</nxce:airport></nxce:arrivalPoint></nxcm:qualifiedAircraftId>
${spec}${rmk}<nxcm:ncsmTrackData><nxcm:eta etaType="ESTIMATED" timeValue="${o.eta}"/></nxcm:ncsmTrackData></fdm:flightPlanInformation></fdm:fltdMessage>`;
}
function batch(list) { const xml = `<?xml version="1.0"?><ds:tfmDataService xmlns:ds="urn:x"><fdm:fltdOutput>${list.map(fltd).join('\n')}</fdm:fltdOutput></ds:tfmDataService>`; return { getXmlContent: () => xml, getBinaryAttachment: () => null }; }
async function req(method, p, headers, body) {
  const r = await fetch(base + p, { method, headers: headers || {}, body });
  let j = null; try { j = await r.json(); } catch (e) {}
  return { status: r.status, body: j, headers: r.headers };
}
(async () => {
  await new Promise(r => setTimeout(r, 1500));
  // Parser
  const set = S.parseLaddList('N-Number,Owner\n883TR,Acme\n123AB,\n');
  assert(set.has('N883TR') && set.has('N123AB') && !set.has('ACME'), 'nnumber csv');
  const plain = S.parseLaddList('# list\nN100AA\nEJA999\nC-FABC\nGABCD\n\n');
  assert(plain.has('N100AA') && plain.has('EJA999') && plain.has('CFABC') && plain.has('GABCD'), 'plain list');
  const regcsv = S.parseLaddList('Registration,Callsign,Notes\nN1XY,ABC123,hello world\n');
  assert(regcsv.has('N1XY') && regcsv.has('ABC123') && regcsv.size === 2, 'reg/callsign csv ' + [...regcsv]);
  const js = S.parseLaddList('["N5", {"registration":"N7QQ"}]');
  assert(js.has('N5') && js.has('N7QQ'), 'json');
  console.log('parser OK');

  // Status: LADD loaded from file
  let st = await req('GET', '/status');
  console.log('ladd status', JSON.stringify(st.body.ladd));
  assert.strictEqual(st.body.ladd.loaded, true);
  assert(typeof st.body.ladd.count === 'number' && st.body.ladd.count >= 4);
  assert.strictEqual(st.body.rampEdit.enabled, false);

  S.handleSwimMsg(batch([
    { acid: 'KOW883', dep: 'KTEB', arr: 'KSFO', etd: iso(-300), eta: iso(30), type: 'CL30', reg: 'N883TR' },  // LADD by reg
    { acid: 'EJA938', dep: 'KSLC', arr: 'KSFO', etd: iso(-60), eta: iso(45), type: 'C68A' },                // normal
    { acid: 'N55PV', dep: 'KLAS', arr: 'KSFO', etd: iso(-60), eta: iso(50), type: 'C25B', priv: true },      // message-level privacy flag
  ]), 'tfms');

  const arr = (await req('GET', '/api/arrivals')).body;
  const blocked = arr.filter(r => r.ident === 'BLOCKED');
  console.log('arrivals', arr.map(r => [r.ident, r.callsign, r.reg, r.type, r.from, r.arriveISO].join('/')));
  assert(blocked.length >= 2, 'masked rows');
  blocked.forEach(b => { assert.strictEqual(b.callsign, 'BLOCKED'); assert.strictEqual(b.reg, 'BLOCKED'); assert(b.type && b.from && b.arriveISO); assert.strictEqual(b.spot, ''); });
  assert(!JSON.stringify(arr).includes('N883TR') && !JSON.stringify(arr).includes('KOW883') && !JSON.stringify(arr).includes('N55PV') && !JSON.stringify(arr).includes('9HVIO') && !JSON.stringify(arr).includes('EJA467'), 'no leak in /api/arrivals');
  assert(arr.some(r => r.callsign === 'EJA938'), 'normal row visible');

  const sw = await req('GET', '/api/swim/arrivals?airport=KSFO');
  assert.strictEqual(sw.status, 200);
  assert.strictEqual(sw.headers.get('access-control-allow-origin'), '*');
  console.log('swim/arrivals', sw.body.count, Object.keys(sw.body.feed), JSON.stringify(sw.body.rows[0]));
  const allowed = new Set(['ident', 'callsign', 'reg', 'type', 'model', 'from', 'to', 'filedDest', 'divertTo', 'departISO', 'arriveISO', 'arrived', 'onGround', 'intl', 'source']);
  sw.body.rows.forEach(r => Object.keys(r).forEach(k => assert(allowed.has(k), 'unexpected field ' + k)));
  assert(sw.body.rows.every(r => /swim/.test(r.source)), 'only swim rows');
  assert(!JSON.stringify(sw.body).includes('N883TR') && !JSON.stringify(sw.body).includes('N55PV'), 'no leak in swim/arrivals');
  assert(sw.body.rows.filter(r => r.ident === 'BLOCKED').length >= 2);
  const diag = await req('GET', '/api/swim-diag?raw=1');
  assert(!JSON.stringify(diag.body).includes('N883TR') && !JSON.stringify(diag.body).includes('KOW883') && !JSON.stringify(diag.body).includes('N55PV'), 'no leak in swim-diag');
  const lk = await req('GET', '/api/lookup?ident=N883TR');
  assert.strictEqual(lk.body.type, null, 'lookup hides LADD');

  // Ramp auth: unset key → 503
  let r1 = await req('PATCH', '/api/dispatch/ramp/N938QS', { 'Content-Type': 'application/json' }, JSON.stringify({ pax: 3 }));
  assert.strictEqual(r1.status, 503); assert.strictEqual(r1.body.error, 'ramp edits disabled: set RAMP_EDIT_KEY');
  process.env.RAMP_EDIT_KEY = 'test-key-123';
  let r2 = await req('PATCH', '/api/dispatch/ramp/N938QS', { 'Content-Type': 'application/json' }, JSON.stringify({ pax: 3 }));
  assert.strictEqual(r2.status, 401);
  let r3 = await req('PATCH', '/api/dispatch/ramp/N938QS', { 'Content-Type': 'application/json', 'X-Skyway-Key': 'wrong' }, JSON.stringify({ pax: 3 }));
  assert.strictEqual(r3.status, 401);
  let r4 = await req('PATCH', '/api/dispatch/ramp/N938QS', { 'Content-Type': 'application/json', 'X-Skyway-Key': 'test-key-123' }, JSON.stringify({ pax: 3 }));
  assert.strictEqual(r4.status, 200); assert.strictEqual(r4.body.pax, 3);
  let r5 = await req('PATCH', '/api/line-room/ramp/N938QS', { 'Content-Type': 'application/json', 'Authorization': 'Bearer test-key-123' }, JSON.stringify({ spot: 'A1' }));
  assert.strictEqual(r5.status, 200);
  let r6 = await req('PATCH', '/api/line-room/ramp/N938QS', { 'Content-Type': 'application/json', 'Authorization': 'Bearer test-key-123' }, JSON.stringify({ pax: 1 }));
  assert.strictEqual(r6.status, 403, 'line-room still limited to spot');
  let r7 = await req('PATCH', '/api/dispatch/ramp/BLOCKED', { 'Content-Type': 'application/json', 'X-Skyway-Key': 'test-key-123' }, JSON.stringify({ pax: 1 }));
  assert.strictEqual(r7.status, 409);
  let r8 = await req('POST', '/api/dispatch/ramp/N938QS', { 'Content-Type': 'application/json' }, '{}');
  assert.strictEqual(r8.status, 405);
  const pre = await fetch(base + '/api/dispatch/ramp/N938QS', { method: 'OPTIONS' });
  assert(!/PATCH/.test(pre.headers.get('access-control-allow-methods') || ''), 'preflight does not grant PATCH');
  st = await req('GET', '/status');
  assert.strictEqual(st.body.rampEdit.enabled, true);
  // Pages still serve
  for (const p of ['/dispatch', '/line-room', '/lobby', '/gm', '/arrivals', '/airloom']) { const r = await fetch(base + p); assert.strictEqual(r.status, 200, p); }
  console.log('ALL OK');
  process.exit(0);
})().catch(e => { console.error('FAIL', e); process.exit(1); });
