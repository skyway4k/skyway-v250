// DB-less check of TIME ON BASE matching: node scripts/test-onbase.js
process.env.SKYWAY_TEST = '1'; process.env.SKYWAY_TEST_NODB = '1'; process.env.DATABASE_URL = process.env.DATABASE_URL || 'postgres://x:x@127.0.0.1:1/x'; process.env.OSKY_ID = process.env.OSKY_ID || 'x'; process.env.OSKY_SECRET = process.env.OSKY_SECRET || 'x';
process.env.PORT = process.env.PORT || '18797';
const S = require('../server.js'); const assert = require('assert');
const iso = m => new Date(Date.now() + m * 60000).toISOString();
(async () => {
  await new Promise(r => setTimeout(r, 800));
  S.movements.arrivals.clear(); S.movements.departures.clear();
  // tail↔callsign: arrival keyed by tail, departure keyed by callsign
  S.identAliases.set('KOW883', 'N883TR'); S.identAliases.set('N883TR', 'N883TR');
  S.movements.arrivals.set('N883TR', { ident: 'N883TR', callsign: 'KOW883', arrived: true, arriveISO: iso(-40), landedAt: Date.now() - 40 * 60000 });
  S.movements.departures.set('KOW883', { ident: 'KOW883', callsign: 'KOW883', from: 'KSFO', to: 'KTEB', departISO: iso(135), depart: 'x' });
  S.movements.departures.set('KOW883B', { ident: 'N883TR', callsign: 'KOW883', from: 'KSFO', to: 'KLAS', departISO: iso(600), depart: 'y' }); // later leg
  // fractional: arrival EJA183 / departure N183QS (inferred)
  S.movements.arrivals.set('N183QS', { ident: 'N183QS', callsign: 'EJA183', arriveISO: iso(60) });
  S.movements.departures.set('EJA183', { ident: 'EJA183', callsign: 'EJA183', from: 'KSFO', to: 'KVNY', departISO: iso(160) });
  // departure BEFORE arrival is not the next leg
  S.movements.arrivals.set('N111AA', { ident: 'N111AA', arriveISO: iso(90) });
  S.movements.departures.set('N111AA', { ident: 'N111AA', from: 'KSFO', to: 'KSJC', departISO: iso(30) });
  // overdue: landed, ETD passed, not departed
  S.movements.arrivals.set('N222BB', { ident: 'N222BB', arrived: true, arriveISO: iso(-120), landedAt: Date.now() - 120 * 60000 });
  S.movements.departures.set('N222BB', { ident: 'N222BB', from: 'KSFO', to: 'KBUR', departISO: iso(-12) });
  const idx = S.buildDepartureIndex();
  const nd = k => S.nextDepartureFor(S.movements.arrivals.get(k), k, idx);
  assert.strictEqual(nd('N883TR').to, 'KTEB', 'earliest next ETD after arrival');
  assert.strictEqual(nd('N183QS').to, 'KVNY', 'frac callsign↔tail');
  assert.strictEqual(nd('N111AA'), null, 'ETD before ETA must not match');
  assert.strictEqual(nd('N222BB').to, 'KBUR', 'overdue still matched');
  // Stale: ETD 5h ago, never departed -> skipped in favour of the next leg (or none)
  S.movements.arrivals.set('N333CC', { ident: 'N333CC', arrived: true, arriveISO: iso(-400), landedAt: Date.now() - 400 * 60000 });
  S.movements.departures.set('N333CC', { ident: 'N333CC', from: 'KSFO', to: 'KSTS', departISO: iso(-300) });
  S.movements.arrivals.set('N444DD', { ident: 'N444DD', arrived: true, arriveISO: iso(-400), landedAt: Date.now() - 400 * 60000 });
  S.movements.departures.set('N444DD', { ident: 'N444DD', from: 'KSFO', to: 'KSTS', departISO: iso(-300) });
  S.movements.departures.set('N444DD-2', { ident: 'N444DD', from: 'KSFO', to: 'KRNO', departISO: iso(90) });
  { const idx2 = S.buildDepartureIndex();
    assert.strictEqual(S.nextDepartureFor(S.movements.arrivals.get('N333CC'), 'N333CC', idx2), null, 'stale ETD not shown as overdue');
    assert.strictEqual(S.nextDepartureFor(S.movements.arrivals.get('N444DD'), 'N444DD', idx2).to, 'KRNO', 'stale leg skipped for next leg'); }
  console.log('ON BASE matcher OK'); process.exit(0);
})().catch(e => { console.error('FAIL', e); process.exit(1); });
