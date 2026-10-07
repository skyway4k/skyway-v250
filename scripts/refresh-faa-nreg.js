#!/usr/bin/env node
/**
 * Rebuild data/faa-nreg.json.gz from the official FAA Releasable Aircraft zip
 * (MASTER.txt + ACFTREF.txt) into the compact { N123AB: { m, t? } } schema
 * consumed by server.js ensureFaaDb / lookupFaaRegistry.
 *
 * Usage:
 *   node scripts/refresh-faa-nreg.js
 *   node scripts/refresh-faa-nreg.js --zip /path/to/ReleasableAircraft.zip
 *   node scripts/refresh-faa-nreg.js --from-dir /path/with/MASTER.txt+ACFTREF.txt
 *   FAA_DB_URL=... node scripts/refresh-faa-nreg.js
 *
 * Writes atomically via *.tmp then rename. Safe to run while the server is up
 * (Node re-reads the gz on next ensureFaaDb force / process restart).
 */
'use strict';

var fs = require('fs');
var path = require('path');
var zlib = require('zlib');
var { execFileSync } = require('child_process');
var readline = require('readline');
var os = require('os');

var ROOT = path.join(__dirname, '..');
var OUT = path.join(ROOT, 'data', 'faa-nreg.json.gz');
var FAA_DB_URL = process.env.FAA_DB_URL || 'https://registry.faa.gov/database/ReleasableAircraft.zip';
/* registry.faa.gov 403s some bot-ish UAs; a normal browser UA downloads fine. */
var UA = process.env.FAA_DB_UA || 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36';

// Keep in sync with server.js ICAO_TO_LAYMAN / FAA_MODEL_TO_ICAO / helpers.
var ICAO_TO_LAYMAN = {
  C56X: 'Citation Excel', C68A: 'Citation Latitude', C680: 'Citation Sovereign', C700: 'Citation Longitude',
  C750: 'Citation X', C25A: 'Citation CJ2', C25B: 'Citation CJ3', C25C: 'Citation CJ4', C525: 'Citation CJ1',
  C510: 'Citation Mustang', C550: 'Citation II', C560: 'Citation V', E55P: 'Phenom 300', E50P: 'Phenom 100',
  E545: 'Legacy 450', E550: 'Praetor 600', CL30: 'Challenger 300', CL35: 'Challenger 350', CL60: 'Challenger 600',
  GLF5: 'Gulfstream G550', GLF4: 'Gulfstream G450', GLF6: 'Gulfstream G650', GL5T: 'Global 5500', GL7T: 'Global 7500',
  GLEX: 'Global Express', GA5C: 'Gulfstream G500', GA6C: 'Gulfstream G600', GA7C: 'Gulfstream G700',
  G280: 'Gulfstream G280', FA7X: 'Falcon 7X', FA8X: 'Falcon 8X', F2TH: 'Falcon 2000', HDJT: 'HondaJet',
  PC12: 'PC-12', PC24: 'PC-24', SF50: 'Vision Jet', LJ45: 'Learjet 45', LJ75: 'Learjet 75',
  BE20: 'King Air 200', B350: 'King Air 350', C172: 'Skyhawk', C182: 'Skylane', C206: 'Stationair', C208: 'Caravan'
};
var FAA_MODEL_TO_ICAO = {
  'EMB-505': 'E55P', EMB505: 'E55P', 'EMB-500': 'E50P', EMB500: 'E50P',
  'EMB-545': 'E545', EMB545: 'E545', 'EMB-550': 'E550', EMB550: 'E550',
  '680A': 'C68A', '680': 'C680', '700': 'C700', '750': 'C750', '560XL': 'C56X',
  '525C': 'C25C', '525B': 'C25B', '525A': 'C25A', '525': 'C525', '510': 'C510',
  '500': 'C500', '550': 'C550', '560': 'C560', '650': 'C650',
  'GV-SP': 'GLF5', G550: 'GLF5', 'GIV-X': 'GLF4', G450: 'GLF4',
  GVI: 'GLF6', G650: 'GLF6', G650ER: 'GLF6', G280: 'G280', G200: 'G200', G150: 'G150',
  G500: 'GA5C', G600: 'GA6C', G700: 'GA7C', G800: 'GA8C', G400: 'GA4C',
  'BD-100-1A10': 'CL30', 'BD-700-1A10': 'GLEX', 'BD-700-2A12': 'GL7T',
  'PC-12/47E': 'PC12', PC1247E: 'PC12', 'MYSTERE-FALCON 50': 'FA50', MYSTEREFALCON50: 'FA50',
  'CL-600-2B16': 'CL60', 'CL-600-2B19': 'CRJ2',
  F2TH: 'F2TH', FA7X: 'FA7X', FA8X: 'FA8X', FA50: 'FA50', F900: 'F900',
  'PC-12': 'PC12', 'PC-24': 'PC24', 'TBM 700': 'TBM7', 'TBM 850': 'TBM8', 'TBM 900': 'TBM9',
  '172S': 'C172', '172R': 'C172', '182T': 'C182', '206H': 'C206', '208B': 'C208',
  SR20: 'SR20', SR22: 'SR22', SR22T: 'SR22', SF50: 'SF50',
  EA500: 'EA50', 'HA-420': 'HDJT', LJ45: 'LJ45', LJ75: 'LJ75', LJ60: 'LJ60',
  B300: 'B350', B200: 'BE20', C90GTx: 'BE9L', C90A: 'BE9L'
};

function isNRegIdent(s) {
  return /^N[0-9][A-Z0-9]*$/.test(s);
}

function faaIcaoFromModel(rawModel) {
  if (!rawModel) return '';
  var raw = String(rawModel).trim();
  var paren = raw.match(/\(([^)]+)\)/);
  var candidates = [];
  if (paren) candidates.push(paren[1].trim());
  candidates.push(raw.replace(/\s*\([^)]*\)\s*/g, ' ').trim());
  candidates.push(raw);
  for (var i = 0; i < candidates.length; i++) {
    var c = candidates[i];
    if (!c) continue;
    var hit = FAA_MODEL_TO_ICAO[c] || FAA_MODEL_TO_ICAO[c.toUpperCase()] || FAA_MODEL_TO_ICAO[c.replace(/\s+/g, '')];
    if (hit) return hit;
    var compact = c.toUpperCase().replace(/[^A-Z0-9-]/g, '');
    if (FAA_MODEL_TO_ICAO[compact]) return FAA_MODEL_TO_ICAO[compact];
  }
  return '';
}

function faaLaymanModel(make, rawModel) {
  if (!rawModel) return '';
  var raw = String(rawModel).trim();
  var paren = raw.match(/\(([^)]+)\)/);
  if (paren && paren[1].trim().length >= 2) {
    var p = paren[1].trim();
    if (p.length <= 24 && !/variant/i.test(p)) return p;
  }
  if (/^EMB-/i.test(raw)) return raw.toUpperCase();
  if (/^\d{2,4}[A-Z]?$/i.test(raw) && make) {
    var mk = String(make).split(/\s+/)[0];
    if (mk && mk.length <= 12) return mk.charAt(0) + mk.slice(1).toLowerCase() + ' ' + raw;
  }
  return raw;
}

function parseArgs(argv) {
  var zip = null;
  var fromDir = null;
  for (var i = 2; i < argv.length; i++) {
    if (argv[i] === '--zip' && argv[i + 1]) { zip = argv[++i]; continue; }
    if (argv[i] === '--from-dir' && argv[i + 1]) { fromDir = argv[++i]; continue; }
    if (argv[i] === '--out' && argv[i + 1]) { OUT = path.resolve(argv[++i]); continue; }
    if (argv[i] === '--help' || argv[i] === '-h') {
      console.log('Usage: node scripts/refresh-faa-nreg.js [--zip path | --from-dir path] [--out path]');
      process.exit(0);
    }
  }
  return { zip: zip, fromDir: fromDir };
}

function splitCsvLine(line) {
  // FAA files are simple CSV without embedded quotes in the columns we need.
  // Prefer a lightweight split; fall back to quote-aware if needed.
  if (line.indexOf('"') < 0) return line.split(',');
  var out = [];
  var cur = '';
  var inQ = false;
  for (var i = 0; i < line.length; i++) {
    var ch = line.charAt(i);
    if (ch === '"') { inQ = !inQ; continue; }
    if (ch === ',' && !inQ) { out.push(cur); cur = ''; continue; }
    cur += ch;
  }
  out.push(cur);
  return out;
}

function readCsvLines(filePath, onRow) {
  return new Promise(function (resolve, reject) {
    var stream = fs.createReadStream(filePath, { encoding: 'utf8' });
    var rl = readline.createInterface({ input: stream, crlfDelay: Infinity });
    var first = true;
    rl.on('line', function (line) {
      if (!line) return;
      if (first) {
        first = false;
        // strip UTF-8 BOM if present on header
        if (line.charCodeAt(0) === 0xFEFF) line = line.slice(1);
        return;
      }
      onRow(splitCsvLine(line));
    });
    rl.on('close', resolve);
    rl.on('error', reject);
    stream.on('error', reject);
  });
}

async function downloadZip(dest) {
  console.log('[faa-nreg] downloading ' + FAA_DB_URL + ' …');
  var res = await fetch(FAA_DB_URL, { headers: { 'User-Agent': UA, Accept: 'application/zip,*/*' } });
  if (!res.ok) throw new Error('download HTTP ' + res.status);
  var buf = Buffer.from(await res.arrayBuffer());
  fs.writeFileSync(dest, buf);
  console.log('[faa-nreg] downloaded ' + Math.round(buf.length / 1048576) + ' MB → ' + dest);
  return dest;
}

async function buildFromZip(zipPath) {
  var work = fs.mkdtempSync(path.join(os.tmpdir(), 'skyway-faa-'));
  try {
    console.log('[faa-nreg] extracting MASTER.txt + ACFTREF.txt …');
    execFileSync('unzip', ['-o', '-j', zipPath, 'MASTER.txt', 'ACFTREF.txt', '-d', work], { stdio: 'inherit', timeout: 180000 });
    var refPath = path.join(work, 'ACFTREF.txt');
    var masterPath = path.join(work, 'MASTER.txt');
    if (!fs.existsSync(refPath) || !fs.existsSync(masterPath)) {
      throw new Error('zip missing MASTER.txt or ACFTREF.txt');
    }

    var refMap = new Map();
    await readCsvLines(refPath, function (rp) {
      if (rp.length < 3) return;
      var code = String(rp[0] || '').trim();
      if (!code) return;
      refMap.set(code, { make: String(rp[1] || '').trim(), model: String(rp[2] || '').trim() });
    });
    console.log('[faa-nreg] ACFTREF codes: ' + refMap.size);

    var obj = Object.create(null);
    var count = 0;
    var withT = 0;
    await readCsvLines(masterPath, function (mp) {
      if (mp.length < 3) return;
      var nNum = String(mp[0] || '').trim().toUpperCase().replace(/[^A-Z0-9]/g, '');
      if (!nNum) return;
      var nKey = nNum.charAt(0) === 'N' ? nNum : ('N' + nNum);
      if (!isNRegIdent(nKey)) return;
      var mfrCode = String(mp[2] || '').trim();
      var ref = refMap.get(mfrCode) || {};
      var rawModel = ref.model || '';
      var make = ref.make || '';
      if (!rawModel && !make) return;
      var icao = faaIcaoFromModel(rawModel);
      var layman = (icao && ICAO_TO_LAYMAN[icao]) || faaLaymanModel(make, rawModel) || rawModel;
      if (icao) {
        obj[nKey] = { t: icao, m: layman };
        withT++;
      } else {
        obj[nKey] = { m: layman };
      }
      count++;
    });
    console.log('[faa-nreg] N-numbers: ' + count + ' (with ICAO t=: ' + withT + ')');

    fs.mkdirSync(path.dirname(OUT), { recursive: true });
    var json = JSON.stringify(obj);
    var gz = zlib.gzipSync(Buffer.from(json, 'utf8'), { level: 9 });
    var tmp = OUT + '.tmp.' + process.pid;
    fs.writeFileSync(tmp, gz);
    fs.renameSync(tmp, OUT);
    console.log('[faa-nreg] wrote ' + OUT + ' (' + Math.round(gz.length / 1024) + ' KB)');
    return { count: count, withT: withT, out: OUT, bytes: gz.length };
  } finally {
    try {
      fs.rmSync(work, { recursive: true, force: true });
    } catch (e) { /* ignore */ }
  }
}

async function buildFromDir(dir) {
  var work = path.resolve(dir);
  var refPath = path.join(work, 'ACFTREF.txt');
  var masterPath = path.join(work, 'MASTER.txt');
  // Reuse zip pipeline by temporarily pointing work at dir without delete
  console.log('[faa-nreg] using MASTER/ACFTREF in ' + work);
  var refMap = new Map();
  await readCsvLines(refPath, function (rp) {
    if (rp.length < 3) return;
    var code = String(rp[0] || '').trim();
    if (!code) return;
    refMap.set(code, { make: String(rp[1] || '').trim(), model: String(rp[2] || '').trim() });
  });
  console.log('[faa-nreg] ACFTREF codes: ' + refMap.size);
  var obj = Object.create(null);
  var count = 0;
  var withT = 0;
  await readCsvLines(masterPath, function (mp) {
    if (mp.length < 3) return;
    var nNum = String(mp[0] || '').trim().toUpperCase().replace(/[^A-Z0-9]/g, '');
    if (!nNum) return;
    var nKey = nNum.charAt(0) === 'N' ? nNum : ('N' + nNum);
    if (!isNRegIdent(nKey)) return;
    var mfrCode = String(mp[2] || '').trim();
    var ref = refMap.get(mfrCode) || {};
    var rawModel = ref.model || '';
    var make = ref.make || '';
    if (!rawModel && !make) return;
    var icao = faaIcaoFromModel(rawModel);
    var layman = (icao && ICAO_TO_LAYMAN[icao]) || faaLaymanModel(make, rawModel) || rawModel;
    if (icao) {
      obj[nKey] = { t: icao, m: layman };
      withT++;
    } else {
      obj[nKey] = { m: layman };
    }
    count++;
  });
  console.log('[faa-nreg] N-numbers: ' + count + ' (with ICAO t=: ' + withT + ')');
  fs.mkdirSync(path.dirname(OUT), { recursive: true });
  var json = JSON.stringify(obj);
  var gz = zlib.gzipSync(Buffer.from(json, 'utf8'), { level: 9 });
  var tmp = OUT + '.tmp.' + process.pid;
  fs.writeFileSync(tmp, gz);
  fs.renameSync(tmp, OUT);
  console.log('[faa-nreg] wrote ' + OUT + ' (' + Math.round(gz.length / 1024) + ' KB)');
  return { count: count, withT: withT, out: OUT, bytes: gz.length };
}

async function main() {
  var args = parseArgs(process.argv);
  if (args.fromDir) {
    var result = await buildFromDir(args.fromDir);
    console.log('[faa-nreg] OK', JSON.stringify(result));
    return;
  }
  var zipPath = args.zip;
  var cleanupZip = false;
  if (!zipPath) {
    zipPath = path.join(os.tmpdir(), 'ReleasableAircraft.' + process.pid + '.zip');
    await downloadZip(zipPath);
    cleanupZip = true;
  } else {
    zipPath = path.resolve(zipPath);
    if (!fs.existsSync(zipPath)) throw new Error('zip not found: ' + zipPath);
    console.log('[faa-nreg] using existing zip ' + zipPath);
  }
  try {
    var result = await buildFromZip(zipPath);
    console.log('[faa-nreg] OK', JSON.stringify(result));
  } finally {
    if (cleanupZip) {
      try { fs.unlinkSync(zipPath); } catch (e) { /* ignore */ }
    }
  }
}

main().catch(function (e) {
  console.error('[faa-nreg] FAILED:', e && e.message ? e.message : e);
  process.exit(1);
});
