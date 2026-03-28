#!/usr/bin/env python3
"""
fetch_bis.py — BIS Entity List diff pipeline for HARPY

Source: eCFR.gov — 15 CFR Part 744, Supplement No. 4 (Entity List)
The eCFR publishes the full regulatory text as XML; this script parses the
embedded HTML table, diffs against a stored baseline, and emits new
designations as signals.

First run: establishes baseline, writes empty signals file.
Subsequent runs: emit only entries added since last baseline.

Usage:
  python fetch_bis.py             # normal run
  python fetch_bis.py --probe     # print first 5 rows parsed and exit
"""

import argparse
import hashlib
import json
import re
import sys
import urllib.request
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from utils import country_to_iso2, profile_score

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

ECFR_URL_TEMPLATE = (
    "https://www.ecfr.gov/api/versioner/v1/full/{date}/title-15.xml"
    "?chapter=VII&subchapter=C&part=744"
    "&appendix=Supplement+No.+4+to+Part+744"
)

DATA_DIR = Path(__file__).parent.parent / "data"
BASELINE = DATA_DIR / "bis_baseline.json"
OUTPUT   = DATA_DIR / "bis_signals.json"

HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; harpy/1.0)"}

# ---------------------------------------------------------------------------
# FR citation date extraction
# ---------------------------------------------------------------------------

_MONTH_NUMS = {
    "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
    "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
}


def extract_most_recent_date(fr_cite):
    """
    Return YYYY-MM-DD of the most recent date in an FR citation string.
    Handles:
      - "76 FR 71869, 11/21/11."       MM/DD/YY (two-digit year)
      - "89 FR 96836, 12/5/2024."      MM/DD/YYYY
      - "89 FR 96836, Dec. 5, 2024"    Month D, YYYY
    Multiple citations are separated by "; " or line breaks.
    """
    if not fr_cite:
        return None

    all_dates = []

    # Pattern 1: M/D/YY or M/D/YYYY
    for m, d, y in re.findall(r"\b(\d{1,2})/(\d{1,2})/(\d{2,4})\b", fr_cite):
        y = int(y)
        if y < 100:
            y = 1900 + y if y >= 97 else 2000 + y
        try:
            all_dates.append(f"{y:04d}-{int(m):02d}-{int(d):02d}")
        except ValueError:
            pass

    # Pattern 2: Mon[.] D[,] YYYY  or  Month D, YYYY
    for mon, day, year in re.findall(
        r"\b([A-Za-z]{3,9})\.?\s+(\d{1,2}),?\s+(\d{4})\b", fr_cite
    ):
        mn = _MONTH_NUMS.get(mon[:3].lower())
        if mn:
            all_dates.append(f"{int(year):04d}-{mn:02d}-{int(day):02d}")

    return max(all_dates) if all_dates else None


# ---------------------------------------------------------------------------
# XML helpers
# ---------------------------------------------------------------------------

def get_all_text(element):
    """Recursively collect all text content from an XML element."""
    parts = []
    if element.text:
        parts.append(element.text)
    for child in element:
        parts.append(get_all_text(child))
        if child.tail:
            parts.append(child.tail)
    return " ".join(p for p in parts if p.strip())


# ---------------------------------------------------------------------------
# eCFR fetch + parse
# ---------------------------------------------------------------------------

def find_ecfr_date():
    """Return the most recent eCFR date string for title 15."""
    url = "https://www.ecfr.gov/api/versioner/v1/titles.json"
    req = urllib.request.Request(url, headers=HEADERS)
    with urllib.request.urlopen(req, timeout=15) as resp:
        data = json.loads(resp.read())
    for title in data.get("titles", []):
        if title.get("number") == 15:
            return title.get("up_to_date_as_of")
    return datetime.now(timezone.utc).date().isoformat()


def fetch_ecfr_xml(date_str):
    """Download the Supplement 4 to Part 744 XML from eCFR."""
    url = ECFR_URL_TEMPLATE.format(date=date_str)
    print(f"  eCFR URL: {url}")
    req = urllib.request.Request(url, headers=HEADERS)
    with urllib.request.urlopen(req, timeout=60) as resp:
        return resp.read()


def parse_entities(xml_bytes):
    """
    Parse the eCFR XML and return a list of entity dicts:
      name, country, address, license_req, fr_cite, signal_date
    """
    root = ET.fromstring(xml_bytes)
    entities = []
    current_country = ""

    for tbody in root.iter("TBODY"):
        for tr in tbody.findall("TR"):
            tds = tr.findall("TD")
            if len(tds) < 2:
                continue

            # Country — carry forward when blank
            country_raw = get_all_text(tds[0]).strip()
            # Non-breaking space or whitespace-only = continuation
            if country_raw and country_raw.replace("\xa0", "").strip():
                current_country = country_raw.strip()

            # Entity cell: "Name, Address, City, Country."
            entity_text = get_all_text(tds[1]).strip()
            if not entity_text or not entity_text.replace("\xa0", "").strip():
                continue

            # Name = everything before the first comma
            name = entity_text.split(",")[0].strip()
            if not name:
                continue

            # Address = the rest after the name
            address = entity_text[len(name):].lstrip(", ").strip()

            license_req = (
                get_all_text(tds[2]).strip() if len(tds) > 2 else ""
            )
            fr_cite = (
                get_all_text(tds[4]).strip() if len(tds) > 4 else ""
            )
            signal_date = extract_most_recent_date(fr_cite)

            entities.append({
                "name":        name,
                "country":     current_country,
                "address":     address,
                "license_req": license_req,
                "fr_cite":     fr_cite,
                "signal_date": signal_date,
            })

    return entities


# ---------------------------------------------------------------------------
# Fingerprint
# ---------------------------------------------------------------------------

def fingerprint(name, country):
    """16-char hex key: sha256 of normalised (name, country)."""
    key = f"{name.strip().lower()}|{country.strip().lower()}"
    return hashlib.sha256(key.encode("utf-8")).hexdigest()[:16]


# ---------------------------------------------------------------------------
# Entity → signal record
# ---------------------------------------------------------------------------

def entity_to_signal(ent, date_discovered):
    iso         = country_to_iso2(ent["country"])
    country_lbl = ent["country"] or "Unknown"
    title       = f"{country_lbl} — {ent['name']}"

    parts = []
    if ent["license_req"]:
        parts.append(f"License: {ent['license_req']}")
    if ent["address"]:
        parts.append(ent["address"][:200])   # cap long addresses
    if ent["fr_cite"]:
        parts.append(f"FR: {ent['fr_cite']}")
    description = ". ".join(parts) if parts else None

    return {
        "iso":         iso,
        "source":      "bis",
        "signal_date": ent["signal_date"] or date_discovered,
        "title":       title,
        "value_usd":   None,
        "description": description,
        "raw_score":   profile_score(iso),
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="BIS Entity List diff pipeline")
    parser.add_argument(
        "--probe",
        action="store_true",
        help="Parse entities and print first 5, then exit without writing files",
    )
    args = parser.parse_args()

    today = datetime.now(timezone.utc).date().isoformat()

    print("=== BIS Entity List pipeline ===")
    print(f"  Date: {today}")

    # ── Load baseline ──────────────────────────────────────────────────────
    if BASELINE.exists():
        baseline_data = json.loads(BASELINE.read_text())
        baseline = set(baseline_data.get("fingerprints", []))
        first_run = False
        print(f"  Baseline: {len(baseline)} known entities")
    else:
        baseline = set()
        first_run = True
        print("  Baseline: none (first run — will establish baseline)")

    # ── Fetch XML from eCFR ───────────────────────────────────────────────
    try:
        ecfr_date = find_ecfr_date()
        print(f"  eCFR date: {ecfr_date}")
        xml_bytes = fetch_ecfr_xml(ecfr_date)
        print(f"  XML size: {len(xml_bytes):,} bytes")
    except Exception as e:
        print(f"ERROR fetching eCFR: {e}", file=sys.stderr)
        sys.exit(1)

    # ── Parse ─────────────────────────────────────────────────────────────
    try:
        entities = parse_entities(xml_bytes)
    except Exception as e:
        print(f"ERROR parsing XML: {e}", file=sys.stderr)
        sys.exit(1)

    print(f"  Entities parsed: {len(entities)}")

    # ── Probe mode ────────────────────────────────────────────────────────
    if args.probe:
        print("\n--- First 5 entities ---")
        for i, e in enumerate(entities[:5]):
            print(f"  [{i}] {e}")
        sys.exit(0)

    # ── Build fingerprint set ─────────────────────────────────────────────
    current_fps = {}
    for ent in entities:
        fp = fingerprint(ent["name"], ent["country"])
        current_fps[fp] = ent

    new_fps = set(current_fps.keys()) - baseline

    if first_run:
        print(f"  First run — baseline established: {len(current_fps)} entities")
        signals = []
    else:
        removed = len(baseline - set(current_fps.keys()))
        print(f"  New entries:     {len(new_fps)}")
        print(f"  Removed entries: {removed}  (not emitted — deletions not a signal)")
        signals = [
            entity_to_signal(current_fps[fp], today)
            for fp in sorted(new_fps)
        ]

    # ── Write signals ─────────────────────────────────────────────────────
    output = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "sources":      ["bis"],
        "signals":      signals,
    }
    OUTPUT.write_text(json.dumps(output, indent=2))
    print(f"  Wrote {len(signals)} signals → {OUTPUT}")

    # ── Update baseline ───────────────────────────────────────────────────
    updated = baseline | set(current_fps.keys())
    BASELINE.write_text(json.dumps({
        "updated_at":   datetime.now(timezone.utc).isoformat(),
        "count":        len(updated),
        "fingerprints": sorted(updated),
    }, indent=2))
    print(f"  Baseline saved: {len(updated)} entities")

    if first_run:
        print("\n  Run again to detect new additions.")


if __name__ == "__main__":
    main()
