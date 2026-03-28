#!/usr/bin/env python3
"""
fetch_federalregister.py — Federal Register signal pipeline for HARPY

Fetches recent documents from agencies that generate apparatus actions:
State Dept, DoD, BIS (export controls), OFAC (sanctions), Treasury, USAID.

No API key required.
Writes data/federalregister_signals.json.
"""

import argparse
import http.client
import json
import re
import ssl
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from utils import COUNTRY_NAME_TO_ISO2, profile_score, append_and_write, write_error

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

FR_HOST = "www.federalregister.gov"
FR_PATH = "/api/v1/documents.json"
LOOKBACK_DAYS          = 30
LOOKBACK_DAYS_BACKFILL = 365
PER_PAGE = 100

# Correct agency slugs from /api/v1/agencies.json
AGENCIES = [
    "state-department",
    "defense-department",
    "industry-and-security-bureau",
    "foreign-assets-control-office",
    "treasury-department",
    "agency-for-international-development",
    "army-department",
    "navy-department",
    "air-force-department",
]

FIELDS = [
    "title",
    "abstract",
    "document_number",
    "html_url",
    "publication_date",
    "type",
    "action",
]

INCLUDE_TYPES = {"Rule", "Proposed Rule", "Notice", "Presidential Document"}

NOISE_PATTERNS = re.compile(
    r"\b(meeting|sunshine act|comment period|vacancy|nomination|"
    r"privacy act|records management|information collection|"
    r"pay scale|federal holiday|office hours|"
    r"viticultural area|winegrowing|alcohol|tobacco|"
    r"flood insurance|flood plain|flood map|"
    r"small business|disadvantaged business|"
    r"environmental impact|environmental assessment)\b",
    re.IGNORECASE,
)

_COUNTRY_NAMES_SORTED = sorted(COUNTRY_NAME_TO_ISO2.keys(), key=len, reverse=True)
_COUNTRY_PATTERN = re.compile(
    r"\b(" + "|".join(re.escape(n) for n in _COUNTRY_NAMES_SORTED) + r")\b",
    re.IGNORECASE,
)


def extract_country(title, abstract):
    text = f"{title or ''} {abstract or ''}"
    m = _COUNTRY_PATTERN.search(text)
    if m:
        return COUNTRY_NAME_TO_ISO2.get(m.group(1).lower())
    return None


# ---------------------------------------------------------------------------
# Fetching — use http.client directly; urllib re-encodes %5B%5D in query strings
# ---------------------------------------------------------------------------

def _build_path(from_date_str, page):
    # Build query string with percent-encoded brackets for nested params
    parts = [
        f"conditions%5Bpublication_date%5D%5Bgte%5D={from_date_str}",
        f"per_page={PER_PAGE}",
        f"page={page}",
        "order=newest",
    ]
    for agency in AGENCIES:
        parts.append(f"conditions%5Bagencies%5D%5B%5D={agency}")
    for field in FIELDS:
        parts.append(f"fields%5B%5D={field}")
    return FR_PATH + "?" + "&".join(parts)


def _get(path):
    ctx = ssl.create_default_context()
    conn = http.client.HTTPSConnection(FR_HOST, context=ctx, timeout=30)
    conn.request("GET", path, headers={"User-Agent": "harpy/1.0"})
    resp = conn.getresponse()
    if resp.status != 200:
        body = resp.read(512).decode("utf-8", errors="replace")
        raise RuntimeError(f"HTTP {resp.status}: {body}")
    return json.loads(resp.read())


def fetch_all(from_date_str):
    results = []
    page = 1
    while True:
        data = _get(_build_path(from_date_str, page))
        batch = data.get("results") or []
        results.extend(batch)
        total = data.get("count", 0)
        if not batch or len(results) >= total or len(results) >= 500:
            break
        page += 1
    return results


# ---------------------------------------------------------------------------
# Signal conversion
# ---------------------------------------------------------------------------

def is_noise(doc):
    text = f"{doc.get('title') or ''} {doc.get('abstract') or ''}"
    return bool(NOISE_PATTERNS.search(text))


def to_signal(doc):
    title = (doc.get("title") or "").strip()
    abstract = (doc.get("abstract") or "").strip()
    doc_type = (doc.get("type") or "").strip()
    doc_number = (doc.get("document_number") or "").strip()

    action = (doc.get("action") or "").strip()
    lead = action or doc_type
    desc_parts = []
    if lead:
        desc_parts.append(lead)
    if abstract:
        desc_parts.append(abstract)
    description = " · ".join(desc_parts) if desc_parts else None

    iso = extract_country(title, abstract)
    return {
        "document_number": doc_number or None,
        "iso": iso,
        "source": "federalregister",
        "signal_date": doc.get("publication_date"),
        "title": title,
        "value_usd": None,
        "description": description,
        "raw_score": profile_score(iso),
        "page_url": doc.get("html_url"),
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--backfill", action="store_true",
                        help=f"Fetch {LOOKBACK_DAYS_BACKFILL} days instead of {LOOKBACK_DAYS}")
    args = parser.parse_args()

    out_path = Path(__file__).parent.parent / "data" / "federalregister_signals.json"
    today = datetime.now(timezone.utc)
    days = LOOKBACK_DAYS_BACKFILL if args.backfill else LOOKBACK_DAYS
    from_date = (today - timedelta(days=days)).strftime("%Y-%m-%d")

    print(f"Fetching Federal Register documents since {from_date} ({'backfill' if args.backfill else 'daily'})…")

    try:
        docs = fetch_all(from_date)
    except Exception as e:
        print(f"ERROR: fetch failed: {e}", file=sys.stderr)
        write_error(out_path, "federalregister", str(e))
        sys.exit(0)

    print(f"  Retrieved {len(docs)} raw documents")

    new_signals = []
    for doc in docs:
        if (doc.get("type") or "").strip() not in INCLUDE_TYPES:
            continue
        if is_noise(doc):
            continue
        new_signals.append(to_signal(doc))

    added = append_and_write(out_path, "federalregister", new_signals, lambda s: s.get("document_number"))
    print(f"[federalregister] {added} new signal(s) written → {out_path}")


if __name__ == "__main__":
    main()
