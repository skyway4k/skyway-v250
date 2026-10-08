// Local harness: node scripts/test-board.js (needs DATABASE_URL to a scratch Postgres).
process.env.SKYWAY_TEST = '1';
process.env.OSKY_ID = process.env.OSKY_ID || 'x';
process.env.OSKY_SECRET = process.env.OSKY_SECRET || 'x';
process.env.PORT = process.env.PORT || '18766';
const S = require('../server.js');
const assert = require('assert');
function iso(minFromNow) { return new Date(Date.now() + minFromNow * 60000).toISOString().replace(/\.\d+Z$/, 'Z'); }
function fltd(o) {
  const spec = o.type ? `<nxcm:flightAircraftSpecs equipmentQualifier="L" weightClass="L">${o.type}</nxcm:flightAircraftSpecs>` : '';
  const rmk = o.reg ? `<nxcm:flightPlanRemarks>PBN/A1 REG/${o.reg} EET/KZOA0130</nxcm:flightPlanRemarks>` : '';
  return `<fdm:fltdMessage acid="${o.acid}" airline="${o.acid.slice(0,3)}" arrArpt="${o.arr}" depArpt="${o.dep}" fdTrigger="X" flightRef="1" msgType="${o.msgType}" sensitivity="A" sourceFacility="KZOA" sourceTimeStamp="${iso(-0.2)}">
<fdm:${o.msgType}>
<nxcm:qualifiedAircraftId><nxce:aircraftId>${o.acid}</nxce:aircraftId><nxce:igtd>${o.etd}</nxce:igtd>
<nxce:departurePoint><nxce:airport>${o.dep}</nxce:airport></nxce:departurePoint>
<nxce:arrivalPoint><nxce:airport>${o.arr}</nxce:airport></nxce:arrivalPoint></nxcm:qualifiedAircraftId>
${spec}${rmk}
<nxcm:ncsmTrackData><nxcm:eta etaType="ESTIMATED" timeValue="${o.eta}"/></nxcm:ncsmTrackData>
</fdm:${o.msgType}></fdm:fltdMessage>`;
}
function batch(list) {
  const xml = `<?xml version="1.0"?><ds:tfmDataService xmlns:ds="urn:x"><fdm:fltdOutput>${list.map(fltd).join('\n')}</fdm:fltdOutput></ds:tfmDataService>`;
  return { getXmlContent: () => xml, getBinaryAttachment: () => null };
}
(async () => {
  await new Promise(r => setTimeout(r, 1500));
  // 1) Batch: track messages without type + an airline + a Bay-elsewhere GA
  S.handleSwimMsg(batch([
    { acid: 'JTL868', dep: 'MMTO', arr: 'KSFO', msgType: 'trackInformation', etd: iso(-230), eta: iso(10) },
    { acid: 'KOW883', dep: 'KTEB', arr: 'KSFO', msgType: 'trackInformation', etd: iso(-320), eta: iso(29) },
    { acid: 'UAL123', dep: 'KDEN', arr: 'KSFO', msgType: 'trackInformation', etd: iso(-100), eta: iso(30) },
    { acid: 'EJA183', dep: 'RJGG', arr: 'KSFO', msgType: 'trackInformation', etd: iso(-330), eta: iso(182) },
    { acid: 'N8312H', dep: 'KSTS', arr: 'KOAK', msgType: 'trackInformation', etd: iso(-20), eta: iso(15) },
  ]), 'tfms');
  // 2) Filed plans carrying type
  S.handleSwimMsg(batch([
    { acid: 'KOW883', dep: 'KTEB', arr: 'KSFO', msgType: 'flightPlanInformation', etd: iso(-320), eta: iso(29), type: 'CL30', reg: 'N883TR' },
    { acid: 'VJT793', dep: 'LIMF', arr: 'KSFO', msgType: 'flightPlanInformation', etd: iso(-560), eta: iso(134), type: 'GL7T', reg: '9HVIO' },
    { acid: 'EJA938', dep: 'KSLC', arr: 'KSFO', msgType: 'flightPlanInformation', etd: iso(18), eta: iso(105), type: 'C68A' },
    { acid: 'N3400C', dep: 'KSBA', arr: 'KSFO', msgType: 'flightPlanInformation', etd: iso(12 * 60), eta: iso(12 * 60 + 40), type: 'GA5C' },
  ]), 'tfms');
  const arr = await S.buildBoard('arrivals');
  const ids = arr.map(f => f.ident + '/' + (f.callsign || '') + '/' + (f.type || '') + '/' + (f.from || '') + '/' + f.arrive);
  console.log('arrivals:', ids);
  console.log('diag blocks', S.swimDiag.blocks, 'payloads', S.swimDiag.payloads, S.swimDiag.homeByDecision);
  const has = id => arr.some(f => [f.ident, f.callsign, f.reg].map(x => String(x || '').replace(/[^A-Z0-9]/gi, '').toUpperCase()).includes(id));
  ['JTL868', 'N883TR', 'N183QS', 'VJT793', 'N938QS'].forEach(id => assert(has(id), 'missing ' + id));
  assert(!has('UAL123'), 'airline leaked'); assert(!has('N3400C'), 'tomorrow leaked'); assert(!has('N8312H'), 'OAK leaked');
  assert(S.tfmsElsewhereFor(['N8312H']), 'OAK memory');
  // 2b) Next leg out of SFO for N183QS must NOT delete the inbound RJGG row
  S.handleSwimMsg(batch([
    { acid: 'EJA183', dep: 'KSFO', arr: 'KTEB', msgType: 'flightPlanInformation', etd: iso(23 * 60), eta: iso(28 * 60), type: 'GL7T' },
    { acid: 'EJA467', dep: 'SMO', arr: 'SFO', msgType: 'FlightModify', etd: iso(60), eta: iso(114), type: 'E55P' },
  ]), 'tfms');
  let a2 = await S.buildBoard('arrivals');
  const n183 = a2.find(f => /N183QS/.test(f.ident));
  assert(n183 && n183.from === 'RJGG', 'N183QS inbound row lost on next-leg msg');
  const n467 = a2.find(f => /N467QS/.test(f.ident));
  assert(n467 && n467.from === 'KSMO', 'SMO not normalized: ' + (n467 && n467.from));
  assert.strictEqual(S.toIcaoAirport('HNL'), 'PHNL');
  // 2c) Lost row is rehydrated from its TFMS plan
  const k183 = S.identAliases.get('N183QS') || 'N183QS';
  S.movements.arrivals.delete(k183);
  assert(S.rehydrateFromPlans() >= 1, 'no rehydrate');
  a2 = await S.buildBoard('arrivals');
  assert(a2.some(f => /N183QS/.test(f.ident)), 'N183QS not rehydrated');
  // 2d) Same-leg dest change (amend to KLAS) removes row and plan; no rehydrate
  S.handleSwimMsg(batch([{ acid: 'EJA938', dep: 'KSLC', arr: 'KLAS', msgType: 'flightPlanInformation', etd: iso(18), eta: iso(80), type: 'C68A' }]), 'tfms');
  S.rehydrateFromPlans();
  a2 = await S.buildBoard('arrivals');
  assert(!a2.some(f => /N938QS/.test(f.ident)), 'amended-away row came back');
  S.handleSwimMsg(batch([{ acid: 'EJA938', dep: 'KSLC', arr: 'KSFO', msgType: 'flightPlanInformation', etd: iso(18), eta: iso(105), type: 'C68A' }]), 'tfms');
  console.log('next-leg / SMO / rehydrate / amend OK');
  // 3) ADS-B evidence geometry
  const SFO = [37.6213, -122.3790];
  function pt(brgFromField, distNm) { // position at bearing/dist from SFO
    const r = brgFromField * Math.PI / 180;
    return { lat: SFO[0] + distNm * Math.cos(r) / 60, lon: SFO[1] + distNm * Math.sin(r) / (60 * Math.cos(SFO[0] * Math.PI / 180)) };
  }
  const cases = [
    ['28 final 5nm 1600ft', Object.assign(pt(118, 5), { distNm: 5, alt: 1600, track: 298, gs: 140, vrate: -700 }), true],
    ['28 final 10nm 3200ft descending', Object.assign(pt(118, 10), { distNm: 10, alt: 3200, track: 296, gs: 170, vrate: -800 }), true],
    ['N21866-like 11.7nm 675ft', Object.assign(pt(140, 11.7), { distNm: 11.7, alt: 675, track: 300, gs: 100, vrate: -200 }), false],
    ['piston 5.6nm 2350ft crossing', Object.assign(pt(60, 5.6), { distNm: 5.6, alt: 2350, track: 180, gs: 100, vrate: 0 }), false],
    ['SQL rwy30 final', Object.assign(pt(137, 9), { distNm: 9, alt: 900, track: 300, gs: 80, vrate: -500 }), false],
    ['inbound 20nm 6500ft desc toward SFO', Object.assign(pt(160, 20), { distNm: 20, alt: 6500, track: 340, gs: 230, vrate: -1200 }), true],
    ['level overflight 18nm 4500', Object.assign(pt(200, 18), { distNm: 18, alt: 4500, track: 20, gs: 120, vrate: 0 }), false],
  ];
  cases.forEach(([name, o, want]) => { const ev = S.adsbArrivalEvidence(o); console.log(want === ev.ok ? 'PASS' : 'FAIL', name, ev.reason); assert.strictEqual(ev.ok, want, name); });
  // 4) ADS-B-only geometric ETA passing must not become LANDED
  S.movements.arrivals.set('N999ZZ', { ident: 'N999ZZ', source: 'adsb-inbound', timeSource: 'adsb', arriveISO: iso(-3), lastAdsbAt: Date.now() - 200000 });
  S.pruneLandedArrivals();
  assert(!S.movements.arrivals.get('N999ZZ').onGround, 'adsb ghost landed');
  S.pruneStaleAdsbOnly();
  assert(!S.movements.arrivals.has('N999ZZ'), 'stale adsb row kept');
  // 5) Snapshot round trip
  const arrPre = await S.buildBoard('arrivals');
  await S.saveSnapshot('test');
  const before = S.movements.arrivals.size, plans = S.tfmsPlanByAircraft.size;
  S.movements.arrivals.clear(); S.movements.departures.clear(); S.tfmsPlanByAircraft.clear();
  await S.restoreSnapshot();
  console.log('restored', S.movements.arrivals.size, 'of', before, 'plans', S.tfmsPlanByAircraft.size, 'of', plans);
  assert.strictEqual(S.movements.arrivals.size, before); assert.strictEqual(S.tfmsPlanByAircraft.size, plans);
  const arr2 = await S.buildBoard('arrivals');
  assert.strictEqual(arr2.length, arrPre.length);
  console.log('ALL OK');
  process.exit(0);
})().catch(e => { console.error('TEST FAIL', e); process.exit(1); });
