// Local preview for Skyway v2 without Postgres/SWIM: serves public/v2 and /api/v2/board from
// JSON fixtures captured from the live /api/arrivals, /api/departures and /status.
// Usage: node scripts/v2-dev.js <fixtureDir> [port]   (fixtures: arrivals.json departures.json status.json)
var http = require('http'), fs = require('fs'), path = require('path'), url = require('url');
var dir = process.argv[2] || '/tmp/v2fx', port = +(process.argv[3] || 8799);
function j(n) { try { return JSON.parse(fs.readFileSync(path.join(dir, n), 'utf8')); } catch (e) { return n === 'status.json' ? {} : []; } }
var st = j('status.json');
var v2 = require('../lib/v2board')({
  buildBoard: async function (k) { return j(k + '.json'); },
  boardWindowInfo: function () { return st.boardWindow || { mode: 'until-midnight-PT', label: 'through midnight PT' }; },
  status: function () { return Object.assign({}, st, { rampEdit: { enabled: process.env.EDIT === '1' } }); },
  fuelPrice: 7.85, LADD_MASK: 'BLOCKED', PUBLIC_DIR: path.join(__dirname, '..', 'public'),
  sendJSON: function (res, s, o) { res.writeHead(s, { 'Content-Type': 'application/json' }); res.end(JSON.stringify(o)); },
  touchActivity: function () { }
});
http.createServer(async function (req, res) {
  var p = url.parse(req.url, true);
  if (await v2.handle(req, res, p.pathname, p.query)) return;
  if (p.pathname === '/adsb/states') { res.writeHead(200, { 'Content-Type': 'application/json' }); res.end(JSON.stringify({ states: [], source: 'fixture' })); return; }
  res.writeHead(404); res.end('nf');
}).listen(port, function () { console.log('v2 dev on ' + port); });
