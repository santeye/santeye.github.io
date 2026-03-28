#!/usr/bin/env python3
"""
utils.py — Shared utilities for HARPY fetch scripts.

Canonical country maps, profile lookups, source taxonomy,
and common signal file I/O patterns.

Import from any fetch script:
    from utils import country_to_iso2, profile_score, append_and_write, write_error
"""

import json
import math
import re
from datetime import datetime, timezone
from pathlib import Path

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

REPO_ROOT    = Path(__file__).parent.parent
PROFILES_DIR = REPO_ROOT / "data" / "profiles"

# ---------------------------------------------------------------------------
# Country name → ISO alpha-2  (canonical, lowercase keys)
#
# Base: fetch_ofac.py (most complete general-purpose map).
# Additions from BIS: "turkiye", "dprk", "uk", "new zealand", "maldives",
#   DPRK/ROK formal names, China formal name.
# Additions from SAM/FARA: "singapore".
# DSCA uppercase map stays in fetch_dsca.py — it parses PDF filenames,
# not free-text, and uses a different key convention.
# ---------------------------------------------------------------------------

COUNTRY_NAME_TO_ISO2 = {
    "afghanistan": "AF", "albania": "AL", "algeria": "DZ", "angola": "AO",
    "armenia": "AM", "australia": "AU", "austria": "AT", "azerbaijan": "AZ",
    "bahrain": "BH", "bangladesh": "BD", "belarus": "BY", "belgium": "BE",
    "belize": "BZ", "benin": "BJ", "bolivia": "BO",
    "bosnia": "BA", "bosnia and herzegovina": "BA",
    "botswana": "BW", "brazil": "BR", "bulgaria": "BG", "burkina faso": "BF",
    "burma": "MM", "burundi": "BI", "cambodia": "KH", "cameroon": "CM",
    "canada": "CA", "central african republic": "CF", "chad": "TD",
    "chile": "CL", "china": "CN", "colombia": "CO", "comoros": "KM",
    "congo": "CG", "democratic republic of congo": "CD",
    "democratic republic of the congo": "CD", "drc": "CD",
    "costa rica": "CR", "croatia": "HR", "cuba": "CU", "cyprus": "CY",
    "czech republic": "CZ", "denmark": "DK", "djibouti": "DJ",
    "dominican republic": "DO", "ecuador": "EC", "egypt": "EG",
    "el salvador": "SV", "equatorial guinea": "GQ", "eritrea": "ER",
    "estonia": "EE", "ethiopia": "ET", "finland": "FI", "france": "FR",
    "gabon": "GA", "gambia": "GM", "georgia": "GE", "germany": "DE",
    "ghana": "GH", "greece": "GR", "guatemala": "GT", "guinea": "GN",
    "guinea-bissau": "GW", "haiti": "HT", "honduras": "HN", "hong kong": "HK",
    "hungary": "HU", "india": "IN", "indonesia": "ID", "iran": "IR",
    "iraq": "IQ", "ireland": "IE", "israel": "IL", "italy": "IT",
    "ivory coast": "CI", "cote d'ivoire": "CI", "jamaica": "JM",
    "japan": "JP", "jordan": "JO", "kazakhstan": "KZ", "kenya": "KE",
    "kosovo": "XK", "kuwait": "KW", "kyrgyzstan": "KG", "laos": "LA",
    "latvia": "LV", "lebanon": "LB", "liberia": "LR", "libya": "LY",
    "lithuania": "LT", "luxembourg": "LU", "madagascar": "MG", "malawi": "MW",
    "malaysia": "MY", "maldives": "MV", "mali": "ML", "mauritania": "MR",
    "mauritius": "MU", "mexico": "MX", "moldova": "MD", "mongolia": "MN",
    "montenegro": "ME", "morocco": "MA", "mozambique": "MZ", "myanmar": "MM",
    "namibia": "NA", "nepal": "NP", "netherlands": "NL", "new zealand": "NZ",
    "nicaragua": "NI", "niger": "NE", "nigeria": "NG",
    "north korea": "KP", "north macedonia": "MK",
    "norway": "NO", "oman": "OM", "pakistan": "PK", "panama": "PA",
    "papua new guinea": "PG", "paraguay": "PY", "peru": "PE",
    "philippines": "PH", "poland": "PL", "portugal": "PT", "qatar": "QA",
    "romania": "RO", "russia": "RU", "russian federation": "RU",
    "rwanda": "RW", "saudi arabia": "SA", "senegal": "SN", "serbia": "RS",
    "sierra leone": "SL", "singapore": "SG", "slovakia": "SK",
    "slovenia": "SI", "somalia": "SO", "south africa": "ZA",
    "south korea": "KR", "south sudan": "SS", "spain": "ES",
    "sri lanka": "LK", "sudan": "SD", "sweden": "SE", "switzerland": "CH",
    "syria": "SY", "taiwan": "TW", "tajikistan": "TJ", "tanzania": "TZ",
    "thailand": "TH", "timor-leste": "TL", "east timor": "TL", "togo": "TG",
    "trinidad and tobago": "TT",
    "tunisia": "TN", "turkey": "TR", "turkiye": "TR", "turkmenistan": "TM",
    "uganda": "UG", "ukraine": "UA", "united arab emirates": "AE", "uae": "AE",
    "united kingdom": "GB", "uk": "GB",
    "united states": "US", "usa": "US",
    "uruguay": "UY", "uzbekistan": "UZ", "venezuela": "VE", "vietnam": "VN",
    "viet nam": "VN", "west bank": "PS", "gaza": "PS", "palestine": "PS",
    "yemen": "YE", "zambia": "ZM", "zimbabwe": "ZW",
    # OFAC formal alternates
    "korea, north": "KP", "korea, south": "KR",
    "iran, islamic republic of": "IR",
    "syrian arab republic": "SY",
    "libyan arab jamahiriya": "LY",
    "lao people's democratic republic": "LA",
    # BIS/formal DPRK + ROK names
    "dprk": "KP",
    "democratic people's republic of korea": "KP",
    "republic of korea": "KR",
    # BIS China formal variant
    "china, people's republic of": "CN",
}


def country_to_iso2(name: str):
    """Normalize a free-text country name to ISO alpha-2, or None."""
    if not name:
        return None
    return COUNTRY_NAME_TO_ISO2.get(name.strip().lower())


# ---------------------------------------------------------------------------
# ISO alpha-3 → alpha-2  (for SAM.gov API and similar sources)
# ---------------------------------------------------------------------------

ALPHA3_TO_ALPHA2 = {
    "AFG": "AF", "ALB": "AL", "DZA": "DZ", "AGO": "AO", "ARM": "AM",
    "AUS": "AU", "AUT": "AT", "AZE": "AZ", "BHR": "BH", "BGD": "BD",
    "BLR": "BY", "BEL": "BE", "BLZ": "BZ", "BEN": "BJ", "BIH": "BA",
    "BWA": "BW", "BRA": "BR", "BGR": "BG", "BFA": "BF", "BDI": "BI",
    "KHM": "KH", "CMR": "CM", "CAN": "CA", "CAF": "CF", "TCD": "TD",
    "CHL": "CL", "CHN": "CN", "COL": "CO", "COM": "KM", "COG": "CG",
    "COD": "CD", "CRI": "CR", "HRV": "HR", "CUB": "CU", "CYP": "CY",
    "CZE": "CZ", "DNK": "DK", "DJI": "DJ", "DOM": "DO", "ECU": "EC",
    "EGY": "EG", "SLV": "SV", "ERI": "ER", "EST": "EE", "ETH": "ET",
    "FIN": "FI", "FRA": "FR", "GAB": "GA", "GMB": "GM", "GEO": "GE",
    "DEU": "DE", "GHA": "GH", "GRC": "GR", "GTM": "GT", "GIN": "GN",
    "GNB": "GW", "HTI": "HT", "HND": "HN", "HUN": "HU", "IND": "IN",
    "IDN": "ID", "IRN": "IR", "IRQ": "IQ", "IRL": "IE", "ISR": "IL",
    "ITA": "IT", "CIV": "CI", "JAM": "JM", "JPN": "JP", "JOR": "JO",
    "KAZ": "KZ", "KEN": "KE", "XKX": "XK", "KWT": "KW", "KGZ": "KG",
    "LAO": "LA", "LVA": "LV", "LBN": "LB", "LBR": "LR", "LBY": "LY",
    "LTU": "LT", "MDG": "MG", "MWI": "MW", "MYS": "MY", "MDV": "MV",
    "MLI": "ML", "MRT": "MR", "MEX": "MX", "MDA": "MD", "MNG": "MN",
    "MNE": "ME", "MAR": "MA", "MOZ": "MZ", "MMR": "MM", "NAM": "NA",
    "NPL": "NP", "NLD": "NL", "NIC": "NI", "NER": "NE", "NGA": "NG",
    "PRK": "KP", "MKD": "MK", "NOR": "NO", "OMN": "OM", "PAK": "PK",
    "PAN": "PA", "PNG": "PG", "PRY": "PY", "PER": "PE", "PHL": "PH",
    "POL": "PL", "PRT": "PT", "QAT": "QA", "ROU": "RO", "RUS": "RU",
    "RWA": "RW", "SAU": "SA", "SEN": "SN", "SRB": "RS", "SLE": "SL",
    "SGP": "SG", "SVK": "SK", "SVN": "SI", "SOM": "SO", "ZAF": "ZA",
    "KOR": "KR", "SSD": "SS", "ESP": "ES", "LKA": "LK", "SDN": "SD",
    "SWE": "SE", "CHE": "CH", "SYR": "SY", "TWN": "TW", "TJK": "TJ",
    "TZA": "TZ", "THA": "TH", "TLS": "TL", "TGO": "TG", "TUN": "TN",
    "TUR": "TR", "TKM": "TM", "UGA": "UG", "UKR": "UA", "ARE": "AE",
    "GBR": "GB", "USA": "US", "URY": "UY", "UZB": "UZ", "VEN": "VE",
    "VNM": "VN", "PSE": "PS", "YEM": "YE", "ZMB": "ZM", "ZWE": "ZW",
    "SWZ": "SZ", "MUS": "MU", "CPV": "CV", "GNQ": "GQ", "STP": "ST",
    "SYC": "SC", "LSO": "LS", "KOS": "XK",
}


# ---------------------------------------------------------------------------
# Source taxonomy
#
# SOURCE_LAYER_MAP and SOURCE_QUALITY are the authoritative Python versions
# of the constants currently duplicated in index.html. When index.html is
# refactored, it should read precomputed layer/quality fields from
# signals.json rather than recomputing them at runtime.
# ---------------------------------------------------------------------------

SOURCE_LAYER_MAP = {
    "dsca":            "military",
    "anchor_budget":   "military",
    "fara":            "influence",
    "lda":             "influence",
    "federalregister": "regulatory",
    "ofac":            "regulatory",
    "bis":             "regulatory",
    "sam":             "procurement",
    "imf":             "financial",
    "cftc":            "financial",
}

ALL_LAYERS = [
    "military", "influence", "regulatory",
    "procurement", "financial", "legislative", "adversarial",
]

# Quality weights mirror sigQuality() in index.html.
# federalregister is handled separately in source_quality() via FR_POLICY_RE.
SOURCE_QUALITY = {
    "dsca":            1.0,
    "ofac":            0.9,
    "fara":            0.9,
    "bis":             0.85,
    "anchor_budget":   0.75,
    "sam":             0.7,
    "cftc":            0.55,
    "imf":             0.5,
    "lda":             0.5,
}

FR_POLICY_RE = re.compile(
    r"sanction|licen[sc]e|designation|emergency|\brule\b|regulation"
    r"|export.control|\barms\b|defense|security",
    re.IGNORECASE,
)


def source_quality(source: str, title: str = "") -> float:
    """Quality weight for a signal. federalregister splits on policy regex."""
    src = (source or "").lower()
    if src == "federalregister":
        return 0.8 if FR_POLICY_RE.search(title or "") else 0.2
    return SOURCE_QUALITY.get(src, 0.5)


def dollar_modifier(value_usd, source: str) -> float:
    """
    Log-scaled dollar boost for DSCA and SAM signals only.
    $1M → 1.0, $100M → 1.15, $1B → 1.225, $10B → 1.3
    """
    src = (source or "").lower()
    if src not in ("dsca", "sam") or not value_usd:
        return 1.0
    t = math.log10(value_usd / 1e6) / 4
    return 1.0 + 0.3 * max(0.0, min(1.0, t))


# ---------------------------------------------------------------------------
# Profile lookups
# ---------------------------------------------------------------------------

def load_profile(iso2: str):
    """Return profile dict for iso2, or None if missing/unreadable."""
    if not iso2:
        return None
    p = PROFILES_DIR / f"{iso2}.json"
    try:
        return json.loads(p.read_text())
    except Exception:
        return None


def profile_score(iso2: str):
    """Return structural_interest_score as float, or None."""
    profile = load_profile(iso2)
    if profile is None:
        return None
    score = profile.get("structural_interest_score")
    return float(score) if score is not None else None


# ---------------------------------------------------------------------------
# Signal file I/O
# ---------------------------------------------------------------------------

def load_existing(path: Path, source: str) -> dict:
    """
    Load a signals envelope from path.
    Returns an empty envelope on missing or corrupt file.
    """
    if path.exists():
        try:
            return json.loads(path.read_text())
        except Exception:
            pass
    return {"generated_at": None, "sources": [source], "signals": []}


def append_and_write(path: Path, source: str, new_signals: list, dedup_key_fn) -> int:
    """
    Merge new_signals into the existing signals file at path.

    dedup_key_fn(signal) -> hashable key (or None to skip dedup for that record).

    Sorts all signals by signal_date ascending before writing.
    Returns count of records actually added.
    """
    existing  = load_existing(path, source)
    known_keys = {
        dedup_key_fn(s)
        for s in existing.get("signals", [])
        if dedup_key_fn(s) is not None
    }

    added = []
    for sig in new_signals:
        key = dedup_key_fn(sig)
        if key is not None and key in known_keys:
            continue
        added.append(sig)
        if key is not None:
            known_keys.add(key)

    all_signals = existing.get("signals", []) + added
    all_signals.sort(key=lambda s: s.get("signal_date") or "")

    path.write_text(json.dumps({
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "sources":      [source],
        "signals":      all_signals,
    }, indent=2))

    return len(added)


def write_error(path: Path, source: str, error: str):
    """
    Preserve existing signals and write an error field into the envelope.
    Used when a fetch run fails partway through.
    """
    try:
        existing = json.loads(path.read_text()) if path.exists() else {}
    except Exception:
        existing = {}
    existing.update({
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "sources":      [source],
        "error":        error,
    })
    existing.setdefault("signals", [])
    path.write_text(json.dumps(existing, indent=2))
