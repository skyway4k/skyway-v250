#!/usr/bin/env python3
"""Rebuild data/faa-nreg.json.gz from the official FAA ReleasableAircraft.zip."""
from __future__ import annotations
import csv, gzip, json, re, subprocess, sys, tempfile, urllib.request, zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "data" / "faa-nreg.json.gz"
URL = "https://registry.faa.gov/database/ReleasableAircraft.zip"
# registry.faa.gov returns 403 for some bot-ish UAs; browser UA downloads OK.
UA = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"

# Keep in sync with server.js ICAO_TO_LAYMAN / FAA_MODEL_TO_ICAO
ICAO_TO_LAYMAN = {
  "C56X":"Citation Excel","C68A":"Citation Latitude","C680":"Citation Sovereign","C700":"Citation Longitude",
  "C750":"Citation X","C25A":"Citation CJ2","C25B":"Citation CJ3","C25C":"Citation CJ4","C525":"Citation CJ1",
  "C510":"Citation Mustang","C550":"Citation II","C560":"Citation V","E55P":"Phenom 300","E50P":"Phenom 100",
  "E545":"Legacy 450","E550":"Praetor 600","CL30":"Challenger 300","CL35":"Challenger 350","CL60":"Challenger 600",
  "GLF5":"Gulfstream G550","GLF4":"Gulfstream G450","GLF6":"Gulfstream G650","GL5T":"Global 5500","GL7T":"Global 7500",
  "GLEX":"Global Express","GA5C":"Gulfstream G500","GA6C":"Gulfstream G600","GA7C":"Gulfstream G700",
  "G280":"Gulfstream G280","FA7X":"Falcon 7X","FA8X":"Falcon 8X","F2TH":"Falcon 2000","HDJT":"HondaJet",
  "PC12":"PC-12","PC24":"PC-24","SF50":"Vision Jet","LJ45":"Learjet 45","LJ75":"Learjet 75",
  "BE20":"King Air 200","B350":"King Air 350","C172":"Skyhawk","C182":"Skylane","C206":"Stationair","C208":"Caravan"
}
FAA_MODEL_TO_ICAO = {
  "EMB-505":"E55P","EMB505":"E55P","EMB-500":"E50P","EMB500":"E50P",
  "EMB-545":"E545","EMB545":"E545","EMB-550":"E550","EMB550":"E550",
  "680A":"C68A","680":"C680","700":"C700","750":"C750","560XL":"C56X",
  "525C":"C25C","525B":"C25B","525A":"C25A","525":"C525","510":"C510",
  "500":"C500","550":"C550","560":"C560","650":"C650",
  "GV-SP":"GLF5","G550":"GLF5","GIV-X":"GLF4","G450":"GLF4",
  "GVI":"GLF6","G650":"GLF6","G650ER":"GLF6","G280":"G280","G200":"G200","G150":"G150",
  "G500":"GA5C","G600":"GA6C","G700":"GA7C","G800":"GA8C","G400":"GA4C",
  "BD-100-1A10":"CL30","BD-700-1A10":"GLEX","BD-700-2A12":"GL7T","PC-12/47E":"PC12","PC1247E":"PC12",
  "MYSTERE-FALCON 50":"FA50","MYSTEREFALCON50":"FA50","CL-600-2B16":"CL60","CL-600-2B19":"CRJ2",
  "F2TH":"F2TH","FA7X":"FA7X","FA8X":"FA8X","FA50":"FA50","F900":"F900",
  "PC-12":"PC12","PC-24":"PC24","TBM 700":"TBM7","TBM 850":"TBM8","TBM 900":"TBM9",
  "172S":"C172","172R":"C172","182T":"C182","206H":"C206","208B":"C208",
  "SR20":"SR20","SR22":"SR22","SR22T":"SR22","SF50":"SF50",
  "EA500":"EA50","HA-420":"HDJT","LJ45":"LJ45","LJ75":"LJ75","LJ60":"LJ60",
  "B300":"B350","B200":"BE20","C90GTx":"BE9L","C90A":"BE9L"
}

def icao_from_model(raw_model: str) -> str:
    if not raw_model: return ""
    raw = str(raw_model).strip()
    paren = re.search(r"\(([^)]+)\)", raw)
    candidates = []
    if paren: candidates.append(paren.group(1).strip())
    candidates.append(re.sub(r"\s*\([^)]*\)\s*", " ", raw).strip())
    candidates.append(raw)
    for c in candidates:
        if not c: continue
        hit = FAA_MODEL_TO_ICAO.get(c) or FAA_MODEL_TO_ICAO.get(c.upper()) or FAA_MODEL_TO_ICAO.get(re.sub(r"\s+", "", c))
        if hit: return hit
        compact = re.sub(r"[^A-Z0-9-]", "", c.upper())
        if compact in FAA_MODEL_TO_ICAO: return FAA_MODEL_TO_ICAO[compact]
    return ""

def layman(make: str, model: str, icao: str) -> str:
    if icao and icao in ICAO_TO_LAYMAN: return ICAO_TO_LAYMAN[icao]
    if not model: return ""
    raw = model.strip()
    paren = re.search(r"\(([^)]+)\)", raw)
    if paren:
        p = paren.group(1).strip()
        if len(p) <= 24 and not re.search(r"variant", p, re.I): return p
    if re.match(r"^EMB-", raw, re.I): return raw.upper()
    if re.match(r"^\d{2,4}[A-Z]?$", raw, re.I) and make:
        mk = make.split()[0]
        if mk and len(mk) <= 12: return mk[0] + mk[1:].lower() + " " + raw
    if make and raw:
        mk = make.split()[0]
        if mk.upper() in ("CESSNA", "PIPER", "BEECH", "CIRRUS") and re.match(r"^\d", raw):
            return mk[0] + mk[1:].lower() + " " + raw
        if mk.upper() == "CIRRUS" and raw.upper().startswith("SR"):
            return raw.upper()
    return raw

def build(master: Path, acftref: Path) -> dict:
    ref = {}
    with acftref.open(newline="", encoding="latin-1") as f:
        r = csv.reader(f); next(r)
        for row in r:
            if not row or not row[0].strip(): continue
            ref[row[0].strip()] = (row[1].strip(), row[2].strip())
    out = {}
    with master.open(newline="", encoding="latin-1") as f:
        r = csv.reader(f); next(r)
        for row in r:
            if not row: continue
            nnum = row[0].strip()
            if not nnum: continue
            code = row[2].strip() if len(row) > 2 else ""
            make, model = ref.get(code, ("", ""))
            icao = icao_from_model(model) or icao_from_model((make + " " + model).strip())
            m = layman(make, model, icao)
            if not m and not model: continue
            entry = {}
            if icao: entry["t"] = icao
            entry["m"] = m or model
            nkey = nnum.upper().replace(" ", "")
            if not nkey.startswith("N"): nkey = "N" + nkey
            if not re.match(r"^N[0-9][A-Z0-9]*$", nkey): continue
            out[nkey] = entry
    return out

def write_out(data: dict) -> None:
    import os
    OUT.parent.mkdir(parents=True, exist_ok=True)
    tmp = OUT.with_name(OUT.name + f".tmp.{os.getpid()}")
    with gzip.open(tmp, "wt", encoding="utf-8", compresslevel=9) as f:
        json.dump(data, f, separators=(",", ":"))
    tmp.replace(OUT)
    print("Wrote", OUT, "n=", len(data), "with_t=", sum(1 for v in data.values() if "t" in v))

def main() -> int:
    import argparse
    ap = argparse.ArgumentParser(description="Rebuild data/faa-nreg.json.gz from FAA releasable DB")
    ap.add_argument("--zip", type=Path, help="Existing ReleasableAircraft.zip")
    ap.add_argument("--from-dir", type=Path, help="Dir containing MASTER.txt + ACFTREF.txt")
    args = ap.parse_args()

    if args.from_dir:
        data = build(args.from_dir / "MASTER.txt", args.from_dir / "ACFTREF.txt")
        write_out(data)
        return 0

    with tempfile.TemporaryDirectory(prefix="faa-nreg-") as td:
        td_path = Path(td)
        if args.zip:
            zip_path = args.zip
            print("Using zip", zip_path)
        else:
            zip_path = td_path / "ReleasableAircraft.zip"
            print("Downloading", URL)
            req = urllib.request.Request(URL, headers={"User-Agent": UA})
            with urllib.request.urlopen(req, timeout=180) as resp, zip_path.open("wb") as f:
                while True:
                    chunk = resp.read(1024 * 1024)
                    if not chunk: break
                    f.write(chunk)
            print("Downloaded", zip_path.stat().st_size, "bytes")
        with zipfile.ZipFile(zip_path) as zf:
            zf.extract("MASTER.txt", td)
            zf.extract("ACFTREF.txt", td)
        data = build(td_path / "MASTER.txt", td_path / "ACFTREF.txt")
        write_out(data)
    return 0

if __name__ == "__main__":
    sys.exit(main())
