// Leg / stale-row / actual-time regression checks (FA comparison 2026-10-16).
// Run: node scripts/test-legs.js  (needs DATABASE_URL; any scratch Postgres, or a pg stub)
process.env.SKYWAY_TEST = '1';
process.env.OSKY_ID = process.env.OSKY_ID || 'x';
process.env.OSKY_SECRET = process.env.OSKY_SECRET || 'x';
process.env.PORT = process.env.PORT || '18767';
const S = require('../server.js');
const assert = require('assert');
function iso(min) { return new Date(Date.now() + min * 60000).toISOString().replace(/\.\d+Z$/, 'Z'); }
function msg(o) {
  const spec = o.type ? `<nxcm:flightAircraftSpecs equipmentQualifier="L">${o.type}</nxcm:flightAircraftSpecs>` : '';
  const eta = o.eta ? `<nxcm:ncsmTrackData><nxcm:eta etaType="${o.etaType || 'ESTIMATED'}" timeValue="${o.eta}"/>${o.depFix ? `<nxcm:departureFixAndTime arrTime="${o.depFix}" fixName="X"/>` : ''}</nxcm:ncsmTrackData>` : '';
  const act = o.actDep ? `<nxcm:ncsmFlightTimeData><nxcm:etd etdType="ACTUAL" timeValue="${o.actDep}"/></nxcm:ncsmFlightTimeData>` : '';
  return `<fdm:fltdMessage acid="${o.acid}" arrArpt="${o.arr}" depArpt="${o.dep}" flightRef="${o.ref || '1'}" msgType="${o.msgType}" sourceTimeStamp="${o.src || iso(-0.1)}"><fdm:${o.msgType}><nxcm:qualifiedAircraftId><nxce:aircraftId>${o.acid}</nxce:aircraftId><nxce:igtd>${o.etd}</nxce:igtd><nxce:departurePoint><nxce:airport>${o.dep}</nxce:airport></nxce:departurePoint><nxce:arrivalPoint><nxce:airport>${o.arr}</nxce:airport></nxce:arrivalPoint></nxcm:qualifiedAircraftId>${spec}${eta}${act}</fdm:${o.msgType}></fdm:fltdMessage>`;
}
function send(list) {
  const xml = `<?xml version="1.0"?><ds:x xmlns:ds="urn:x"><fdm:fltdOutput>${list.map(msg).join('')}</fdm:fltdOutput></ds:x>`;
  S.handleSwimMsg({ getXmlContent: () => xml, getBinaryAttachment: () => null }, 'tfms');
}
const A = () => S.movements.arrivals;
const T = v => v ? new Date(v).toISOString().replace(/\.\d+Z$/, 'Z') : v;
const row = id => A().get(S.identAliases.get(id) || id);
(async () => {
  await new Promise(r => setTimeout(r, 800));
  // 1) N176QS: filed 15:00→15:54, ETA passes while still on the ground at VNY → inferred landed;
  //    then TFMS track (airborne, new ETA) must un-land it and overwrite the stale times.
  send([{ acid: 'EJA176', dep: 'VNY', arr: 'SFO', msgType: 'flightPlanInformation', etd: iso(-125), eta: iso(-71), type: 'GL7T', ref: '176A' }]);
  S.pruneLandedArrivals();
  assert(row('N176QS').onGround && row('N176QS').landInferred, '176 should be inferred landed (ETA 71 min past)');
  send([{ acid: 'EJA176', dep: 'VNY', arr: 'SFO', msgType: 'trackInformation', etd: iso(-125), eta: iso(36), depFix: iso(-14), ref: '176A' }]);
  let r = row('N176QS');
  assert(!r.onGround && !r.arrived, '176 not un-landed by live track');
  assert.strictEqual(T(r.arriveISO), iso(36), '176 ETA not updated from track');
  send([{ acid: 'EJA176', dep: 'VNY', arr: 'SFO', msgType: 'departureInformation', etd: iso(-125), actDep: iso(-16), ref: '176A' }]);
  assert.strictEqual(row('N176QS').departISO && T(row('N176QS').departISO), iso(-16), '176 actual departure not applied');
  send([{ acid: 'EJA176', dep: 'VNY', arr: 'SFO', msgType: 'flightPlanAmendmentInformation', etd: iso(-125), ref: '176A', type: 'GL7T' }]);
  r = row('N176QS');
  assert.strictEqual(r.departISO && T(row('N176QS').departISO), iso(-16), 'filed amendment reverted actual departure');
  assert.strictEqual(T(r.arriveISO), iso(36), 'filed amendment reverted live ETA');
  console.log('N176QS', r.depart, '→', r.arrive, 'onGround', !!r.onGround);

  // 2) N454QS: landed this afternoon, out SFO→HHR, back HHR→SFO tonight — two legs, never merged.
  send([{ acid: 'EJA454', dep: 'TRM', arr: 'SFO', msgType: 'arrivalInformation', etd: iso(-150), eta: iso(-110), etaType: 'ACTUAL', ref: '454A', type: 'E55P' }]);
  send([{ acid: 'EJA454', dep: 'SFO', arr: 'HHR', msgType: 'FlightTimes', etd: iso(30), eta: iso(88), ref: '454B', type: 'E55P' }]);
  send([{ acid: 'EJA454', dep: 'HHR', arr: 'SFO', msgType: 'flightPlanInformation', etd: iso(270), eta: iso(330), ref: '454C', type: 'E55P' }]);
  r = row('N454QS');
  assert(r.arrived && r.from === 'KTRM', 'landed leg damaged: ' + JSON.stringify([r.from, r.depart, r.arrive]));
  let q = A().get((S.identAliases.get('N454QS') || 'N454QS') + '~NEXT');
  assert(q && q.from === 'KHHR' && T(q.arriveISO) === iso(330) && !q.arrived, 'next leg not queued');
  let board = await S.buildBoard('arrivals');
  const rows454 = board.filter(f => /N454QS/.test(f.ident));
  assert.strictEqual(rows454.length, 2, 'expected landed + scheduled N454QS rows');
  rows454.forEach(f => assert(!(Date.parse(f.arriveISO) < Date.parse(f.departISO)), 'arrival before departure on ' + f.from));
  // HHR→SFO amend while queued updates the queued row only
  send([{ acid: 'EJA454', dep: 'HHR', arr: 'SFO', msgType: 'FlightTimes', etd: iso(278), eta: iso(342), ref: '454C' }]);
  assert.strictEqual(A().get((S.identAliases.get('N454QS') || 'N454QS') + '~NEXT').arriveISO && T(A().get((S.identAliases.get('N454QS') || 'N454QS') + '~NEXT').arriveISO), iso(342));
  assert(row('N454QS').arrived, 'base touched by next-leg amend');
  // Wheels-off SFO→HHR: landed row leaves, queued leg becomes the aircraft row.
  send([{ acid: 'EJA454', dep: 'SFO', arr: 'HHR', msgType: 'trackInformation', etd: iso(-2), eta: iso(56), ref: '454B' }]);
  S.pruneLandedArrivals();
  r = row('N454QS');
  assert(r && r.from === 'KHHR' && !r.arrived && T(r.arriveISO) === iso(342), 'queued leg not promoted: ' + JSON.stringify(r && [r.from, r.arrive, r.arrived]));
  assert(!A().has((S.identAliases.get('N454QS') || 'N454QS') + '~NEXT'), 'queued row left behind');
  console.log('N454QS legs OK', r.from, r.depart, '→', r.arrive);

  // 3) Mixed legacy row (landed 15:12 with tonight's 21:30 ETD) is split on repair.
  A().set('N999QS', { ident: 'N999QS', callsign: 'EJA999', reg: 'N999QS', type: 'E55P', from: 'KHHR', to: 'KSFO', arrived: true, onGround: true, landedAt: Date.now() - 100 * 60000, arriveISO: iso(-100), departISO: iso(200), depart: 'x', source: 'swim', timeSource: 'swim' });
  assert.strictEqual(S.repairMixedArrivalLegs(), 1);
  assert(A().get('N999QS~NEXT') && A().get('N999QS~NEXT').from === 'KHHR' && A().get('N999QS').from === '', 'mixed row not split');

  // 4) Stale ground rows: landed 20h ago with nothing filed → dropped; with a future SFO departure → kept.
  A().set('N421SC', { ident: 'N421SC', reg: 'N421SC', type: 'F2TH', from: 'KTEB', to: 'KSFO', onGround: true, landInferred: true, landedAt: Date.now() - 20 * 3600000, arriveISO: iso(-20 * 60), departISO: iso(-26 * 60), source: 'swim', timeSource: 'swim' });
  A().set('N6FE', { ident: 'N6FE', reg: 'N6FE', type: 'GLEX', from: 'KMEM', to: 'KSFO', onGround: true, landInferred: true, landedAt: Date.now() - 20 * 3600000, arriveISO: iso(-20 * 60), departISO: iso(-23 * 60), source: 'swim', timeSource: 'swim' });
  S.movements.departures.set('N6FE', { ident: 'N6FE', reg: 'N6FE', from: 'KSFO', to: 'KMEM', departISO: iso(14 * 60), departed: false, source: 'swim', timeSource: 'swim' });
  // N191QS-style bleed: landed yesterday, carries a later SFO departure time that already passed.
  A().set('N191QS', { ident: 'N191QS', reg: 'N191QS', callsign: 'EJA191', type: 'GL7T', from: 'KSFO', to: 'KSFO', arrived: true, onGround: true, landedAt: Date.now() - 23 * 3600000, arriveISO: iso(-23 * 60), departISO: iso(-21 * 60), source: 'swim', timeSource: 'swim' });
  S.movements.departures.set('N191QS', { ident: 'N191QS', reg: 'N191QS', from: 'KSFO', to: 'KIAD', departISO: iso(16 * 60), departed: false, source: 'swim', timeSource: 'swim' });
  S.pruneLandedArrivals();
  assert(!A().has('N421SC'), 'stale N421SC kept');
  assert(A().has('N6FE'), 'overnighter with a filed departure dropped');
  assert(!A().has('N191QS'), 'N191QS bleed row kept');

  // 5) ETA ≤30 min past, no track: still en route (landPending), not LANDED; track ended → landed.
  send([{ acid: 'N12AB', dep: 'SBA', arr: 'SFO', msgType: 'flightPlanInformation', etd: iso(-70), eta: iso(-10), type: 'C56X' }]);
  S.pruneLandedArrivals();
  board = await S.buildBoard('arrivals');
  let p = board.find(f => f.ident === 'N12AB');
  assert(p && p.landPending && !p.onGround, 'N12AB should be pending, not landed');
  A().get('N12AB').lastTrackAt = Date.now() - 8 * 60000;
  S.pruneLandedArrivals();
  assert(A().get('N12AB').onGround, 'track ended + ETA passed should land it');
  // Live track keeps a late flight airborne even 45 min past its filed ETA.
  send([{ acid: 'N34CD', dep: 'SBA', arr: 'SFO', msgType: 'flightPlanInformation', etd: iso(-100), eta: iso(-45), type: 'C56X' }]);
  A().get('N34CD').lastTrackAt = Date.now() - 60000;
  S.pruneLandedArrivals();
  assert(!A().get('N34CD').onGround, 'live track flight landed');

  // 6) CRJ2 on Part 91 / charter vs airline.
  assert(S.isGaBizTraffic('N269ZR', '', 'CRJ2'), 'N269ZR CRJ2 rejected');
  assert(!S.isGaBizTraffic('SKW5300', 'N123SY', 'CRJ2'), 'SKW CRJ2 accepted');
  assert(!S.isGaBizTraffic('ASH6000', '', 'CRJ7'), 'Mesa CRJ7 accepted');
  assert(!S.isGaBizTraffic('UAL1', '', 'B738'));
  // 7) KSFO-origin plan never writes an arrival row (N191QS from=KSFO).
  send([{ acid: 'N77LL', dep: 'SFO', arr: 'SFO', msgType: 'flightPlanInformation', etd: iso(30), eta: iso(90), type: 'C172' }]);
  assert(!A().has('N77LL'), 'SFO→SFO plan wrote an arrival row');
  console.log('ALL LEG TESTS OK');
  process.exit(0);
})().catch(e => { console.error('TEST FAIL', e); process.exit(1); });
