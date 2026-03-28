#!/usr/bin/env python3
"""
fetch_sam.py — SAM.gov procurement signal pipeline for HARPY

Single API call, last 45 days, limit=10.
Filters to defense/state/USAID agencies.
Writes data/sam_signals.json.
"""

import json
import os
import re
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
import urllib.request
import urllib.parse

sys.path.insert(0, str(Path(__file__).parent))
from utils import COUNTRY_NAME_TO_ISO2, ALPHA3_TO_ALPHA2, profile_score

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

SAM_API_BASE = "https://api.sam.gov/opportunities/v2/search"
LOOKBACK_DAYS = 45
BATCH_SIZE = 10

AGENCY_KEYWORDS = [
    "DEFENSE, DEPARTMENT OF",
    "ARMY, DEPARTMENT OF",
    "NAVY, DEPARTMENT OF",
    "AIR FORCE, DEPARTMENT OF",
    "STATE, DEPARTMENT OF",
    "AGENCY FOR INTERNATIONAL DEVELOPMENT",
]

_COUNTRY_NAMES_SORTED = sorted(COUNTRY_NAME_TO_ISO2.keys(), key=len, reverse=True)
_COUNTRY_PATTERN = re.compile(
    r"\b(" + "|".join(re.escape(n) for n in _COUNTRY_NAMES_SORTED) + r")\b",
    re.IGNORECASE,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def load_api_key():
    key = os.environ.get("SAM_API_KEY", "")
    if not key:
        env_path = Path(__file__).parent.parent / ".env"
        try:
            with open(env_path) as f:
                for line in f:
                    if line.startswith("SAM_API_KEY="):
                        key = line.strip().split("=", 1)[1]
                        break
        except FileNotFoundError:
            pass
    return key


MAINTENANCE_PATTERNS = re.compile(
    r"\b(corrugated|roofing|flooring|painting|janitorial|landscaping|"
    r"elevator|hvac|plumbing|electrical repair|window|carpet|custodial)\b",
    re.IGNORECASE,
)


def is_agency_match(record):
    path = (record.get("fullParentPathName") or "").upper()
    return any(kw in path for kw in AGENCY_KEYWORDS)


def is_maintenance(record):
    title = record.get("title") or ""
    return bool(MAINTENANCE_PATTERNS.search(title))


def extract_country(record):
    pop = record.get("placeOfPerformance") or {}
    country_obj = pop.get("country") or {}
    code = (country_obj.get("code") or "").strip().upper()

    if code:
        if len(code) == 3:
            iso2 = ALPHA3_TO_ALPHA2.get(code)
            if iso2:
                return iso2
        elif len(code) == 2:
            return code

    # Fallback: scan title for country name
    title = record.get("title") or ""
    m = _COUNTRY_PATTERN.search(title)
    if m:
        return COUNTRY_NAME_TO_ISO2.get(m.group(1).lower())

    return None


def build_description(record):
    award = record.get("award") or {}
    parts = []
    notice_type = (record.get("type") or record.get("baseType") or "").strip()
    psc = (record.get("classificationCode") or "").strip()
    awardee = ((award.get("awardee") or {}).get("name") or "").strip()
    if notice_type:
        parts.append(notice_type)
    if psc:
        parts.append(psc)
    if awardee:
        parts.append(awardee)
    return " · ".join(parts) if parts else None


def to_signal(record):
    award = record.get("award") or {}
    value = award.get("amount")
    try:
        value = float(value) if value is not None else None
    except (TypeError, ValueError):
        value = None

    iso = extract_country(record)
    return {
        "iso": iso,
        "source": "sam",
        "signal_date": record.get("postedDate"),
        "title": record.get("title"),
        "value_usd": value,
        "description": build_description(record),
        "raw_score": profile_score(iso),
        "page_url": record.get("uiLink"),
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    out_path = Path(__file__).parent.parent / "data" / "sam_signals.json"

    key = load_api_key()
    if not key:
        print("ERROR: SAM_API_KEY not set", file=sys.stderr)
        out_path.write_text(json.dumps({
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "sources": ["sam"],
            "error": "missing SAM_API_KEY",
            "signals": [],
        }, indent=2))
        sys.exit(0)

    today = datetime.now(timezone.utc)
    from_date = (today - timedelta(days=LOOKBACK_DAYS)).strftime("%m/%d/%Y")
    to_date = today.strftime("%m/%d/%Y")

    params = {
        "api_key": key,
        "limit": BATCH_SIZE,
        "offset": 0,
        "postedFrom": from_date,
        "postedTo": to_date,
    }
    url = SAM_API_BASE + "?" + urllib.parse.urlencode(params)

    try:
        req = urllib.request.Request(url)
        with urllib.request.urlopen(req, timeout=30) as resp:
            data = json.loads(resp.read())
    except Exception as e:
        print(f"ERROR: API call failed: {e}", file=sys.stderr)
        out_path.write_text(json.dumps({
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "sources": ["sam"],
            "error": str(e),
            "signals": [],
        }, indent=2))
        sys.exit(0)

    batch = data.get("opportunitiesData") or []
    signals = [to_signal(r) for r in batch if is_agency_match(r) and not is_maintenance(r)]

    output = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "sources": ["sam"],
        "signals": signals,
    }
    out_path.write_text(json.dumps(output, indent=2))
    print(f"Wrote {len(signals)} signals to {out_path}")


if __name__ == "__main__":
    main()
