#!/usr/bin/env python3
"""
build_signals.py — Aggregation and enrichment layer for HARPY.

Reads all data/*_signals.json source files, then:
  1. Merges all records
  2. Filters: drops iso null/US/XX, drops records before DATE_CUTOFF
  3. Deduplicates: cn_number (DSCA) or iso|signal_date|description composite key
  4. Enriches each signal with precomputed fields:
       profile      — nested {name, score, factors, rationale} from country profile
       layer        — source taxonomy layer (military, influence, regulatory, ...)
       quality      — source quality weight (0.0–1.0)
       dollar_mod   — log-scaled dollar bonus (DSCA/SAM only)
       is_fr_policy — True if federalregister signal matches policy regex
  5. Sorts by signal_date descending
  6. Writes data/signals.json

Fields stripped from source records: raw_score, weight (both unused downstream).
"""

import json
import glob
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from utils import (
    load_profile, SOURCE_LAYER_MAP, source_quality, dollar_modifier, FR_POLICY_RE,
)

DATA_DIR    = os.path.join(os.path.dirname(__file__), "..", "data")
OUTPUT_FILE = os.path.join(DATA_DIR, "signals.json")

DATE_CUTOFF = "2025-01-01"   # signals before this date are excluded from output

STRIP_FIELDS = {"raw_score", "weight"}


def build_profile_block(iso: str):
    """Return nested profile dict for iso, or None if no profile exists."""
    if not iso:
        return None
    p = load_profile(iso)
    if p is None:
        return None
    return {
        "name":      p.get("name"),
        "score":     p.get("structural_interest_score"),
        "factors":   p.get("key_structural_interests"),
        "rationale": p.get("score_rationale"),
    }


def enrich(sig: dict) -> dict:
    """Add precomputed fields; strip dead fields. Returns new dict."""
    out = {k: v for k, v in sig.items() if k not in STRIP_FIELDS}

    iso    = sig.get("iso") or ""
    source = sig.get("source") or ""
    title  = sig.get("title") or ""

    out["profile"]      = build_profile_block(iso)
    out["layer"]        = SOURCE_LAYER_MAP.get(source.lower())
    out["quality"]      = source_quality(source, title)
    out["dollar_mod"]   = dollar_modifier(sig.get("value_usd"), source)
    out["is_fr_policy"] = bool(FR_POLICY_RE.search(title)) if source.lower() == "federalregister" else None

    return out


def dedup_key(sig: dict):
    cn = sig.get("cn_number")
    if cn:
        return f"cn:{cn}"
    return f"{sig.get('iso')}|{sig.get('signal_date')}|{sig.get('description')}"


def main():
    pattern      = os.path.join(DATA_DIR, "*_signals.json")
    source_files = sorted(glob.glob(pattern))

    raw_signals      = []
    sources_found    = []
    counts_per_source = {}

    for path in source_files:
        if os.path.abspath(path) == os.path.abspath(OUTPUT_FILE):
            continue

        filename    = os.path.basename(path)
        source_name = filename.replace("_signals.json", "")

        try:
            with open(path) as f:
                data = json.load(f)
        except Exception as e:
            print(f"  ERROR reading {filename}: {e}")
            continue

        signals = data.get("signals", [])
        signals = [s for s in signals if s.get("title")]

        raw_signals.extend(signals)
        sources_found.append(source_name)
        counts_per_source[source_name] = len(signals)
        print(f"  Found: {filename}  ({len(signals)} records)")

    # Filter
    filtered = [
        s for s in raw_signals
        if s.get("iso")
        and s.get("iso") not in ("US", "XX")
        and (s.get("signal_date") or "") >= DATE_CUTOFF
    ]
    print(f"\n  After filter (iso valid, date >= {DATE_CUTOFF}): {len(filtered)} / {len(raw_signals)}")

    # Deduplicate
    seen = set()
    deduped = []
    for s in filtered:
        key = dedup_key(s)
        if key in seen:
            continue
        seen.add(key)
        deduped.append(s)
    print(f"  After dedup: {len(deduped)}")

    # Enrich
    enriched = [enrich(s) for s in deduped]

    # Sort newest first
    enriched.sort(key=lambda s: s.get("signal_date") or "", reverse=True)

    output = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "sources":      sources_found,
        "signals":      enriched,
    }

    with open(OUTPUT_FILE, "w") as f:
        json.dump(output, f, indent=2)

    print(f"\nTotal records: {len(enriched)}")
    print(f"Sources: {sources_found}")
    print("Records per source (pre-filter):")
    for src, count in counts_per_source.items():
        print(f"  {src}: {count}")

    print("\nFirst 3 records:")
    for i, rec in enumerate(enriched[:3]):
        print(f"  [{i}] {json.dumps(rec)}")

    print(f"\nWrote {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
