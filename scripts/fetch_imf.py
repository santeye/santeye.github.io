#!/usr/bin/env python3
"""
fetch_imf.py — IMF program monitoring pipeline for HARPY

Three signal types, in priority order:
  1. imf_delay       — program review significantly overdue (program off-track)
  2. imf_approval    — new IMF program approved (SBA, EFF, ECF, RCF-equivalent)
  3. imf_disbursement — large tranche actually disbursed (≥ SDR 50M)

Sources:
  MONA Reviews.xlsx     → delay detection
  MONA Description.xlsx → new program approvals + total access amounts
  MONA Purchases.xlsx   → actual disbursement records
  SDR rate              → rms_five.aspx (fallback: 1.35 USD/SDR)

MONA data lag: ~90-120 days behind real time. Disbursement lookback is
extended to LOOKBACK_DAYS + SDR_LAG_DAYS to compensate.

First run: LOOKBACK_DAYS = 90 days (delays emit all currently overdue programs).
"""

from __future__ import annotations

import hashlib
import io
import json
import re
import sys
import time
import xml.etree.ElementTree as ET
import zipfile
from datetime import date, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from utils import profile_score

try:
    from curl_cffi import requests as cffi_requests
    from bs4 import BeautifulSoup
    _HAS_CFFI = True
except ImportError:
    _HAS_CFFI = False

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

LOOKBACK_DAYS = 90          # standard lookback for approvals / disbursements
MONA_LAG_DAYS = 120         # MONA data typically 90-120 days behind real time
DELAY_THRESHOLD_DAYS = 90   # reviews overdue by this many days are flagged
DISBURSE_MIN_SDR = 50.0     # minimum SDR millions for disbursement signals

MONA_BASE = "https://www.imf.org/external/np/pdr/mona/"
SDR_RATE_URL = "https://www.imf.org/external/np/fin/data/rms_five.aspx"
SDR_FALLBACK = 1.35         # approximate USD/SDR when live fetch fails

# Arrangement type labels (MONA codes → human-readable)
ARRANGEMENT_LABELS = {
    "SBA":      "Stand-By Arrangement",
    "EFF":      "Extended Fund Facility",
    "ECF":      "Extended Credit Facility",
    "ECF-EFF":  "ECF/EFF Combined",
    "SCF":      "Standby Credit Facility",
    "PRGF":     "Poverty Reduction and Growth Facility",
    "PRGF-EFF": "PRGF/EFF Combined",
    "FCL":      "Flexible Credit Line",
    "PLL":      "Precautionary and Liquidity Line",
    "PCL":      "Precautionary Credit Line",
    "PCI":      "Policy Coordination Instrument",
    "PSI":      "Policy Support Instrument",
    "ESF":      "Exogenous Shocks Facility",
    "SBA-ESF":  "SBA/ESF Combined",
    "SBA-SCF":  "SBA/SCF Combined",
    "SLL":      "Standby Liquidity Line",
}

# ---------------------------------------------------------------------------
# MONA country name → ISO alpha-2
# Builds on IMF DataMapper labels + MONA naming conventions
# ---------------------------------------------------------------------------

IMF_COUNTRY_MAP = {
    # Standard names
    "AFGHANISTAN": "AF", "AFGHANISTAN,ISLAMIC REPUBLIC OF": "AF",
    "ALBANIA": "AL", "ALGERIA": "DZ", "ANGOLA": "AO",
    "ARGENTINA": "AR", "ARMENIA": "AM", "AUSTRALIA": "AU",
    "AUSTRIA": "AT", "AZERBAIJAN": "AZ",
    "BAHRAIN": "BH", "BANGLADESH": "BD", "BARBADOS": "BB",
    "BELARUS": "BY", "BELGIUM": "BE", "BELIZE": "BZ",
    "BENIN": "BJ", "BHUTAN": "BT",
    "BOLIVIA": "BO", "BOSNIA AND HERZEGOVINA": "BA",
    "BOTSWANA": "BW", "BRAZIL": "BR", "BULGARIA": "BG",
    "BURKINA FASO": "BF", "BURUNDI": "BI",
    "CABO VERDE": "CV", "CAPE VERDE": "CV",
    "CAMBODIA": "KH", "CAMEROON": "CM", "CANADA": "CA",
    "CENTRAL AFRICAN REPUBLIC": "CF", "CHAD": "TD",
    "CHILE": "CL", "CHINA": "CN", "COLOMBIA": "CO",
    "COMOROS": "KM", "CONGO": "CG",
    "CONGO, DEM. REP. OF THE": "CD", "CONGO, DEMOCRATIC REPUBLIC OF THE": "CD",
    "COSTA RICA": "CR", "CROATIA": "HR", "CUBA": "CU",
    "CYPRUS": "CY", "CZECH REPUBLIC": "CZ",
    "DENMARK": "DK", "DJIBOUTI": "DJ", "DOMINICA": "DM",
    "DOMINICAN REPUBLIC": "DO",
    "ECUADOR": "EC", "EGYPT": "EG", "EGYPT, ARAB REPUBLIC OF": "EG",
    "EL SALVADOR": "SV", "ERITREA": "ER", "ESWATINI": "SZ",
    "ETHIOPIA": "ET",
    "FIJI": "FJ", "FINLAND": "FI", "FRANCE": "FR",
    "GABON": "GA", "GAMBIA": "GM", "GAMBIA, THE": "GM",
    "GEORGIA": "GE", "GERMANY": "DE", "GHANA": "GH",
    "GREECE": "GR", "GRENADA": "GD", "GUATEMALA": "GT",
    "GUINEA": "GN", "GUINEA-BISSAU": "GW", "GUYANA": "GY",
    "HAITI": "HT", "HONDURAS": "HN", "HUNGARY": "HU",
    "INDIA": "IN", "INDONESIA": "ID", "IRAN": "IR",
    "IRAN, ISLAMIC REPUBLIC OF": "IR", "IRAQ": "IQ",
    "IRELAND": "IE", "ISRAEL": "IL", "ITALY": "IT",
    "JAMAICA": "JM", "JAPAN": "JP", "JORDAN": "JO",
    "KAZAKHSTAN": "KZ", "KENYA": "KE", "KIRIBATI": "KI",
    "KOREA": "KR", "KOREA, REPUBLIC OF": "KR",
    "KOSOVO": "XK", "KUWAIT": "KW", "KYRGYZ REPUBLIC": "KG",
    "LAO P.D.R.": "LA", "LAOS": "LA",
    "LATVIA": "LV", "LEBANON": "LB", "LESOTHO": "LS",
    "LIBERIA": "LR", "LIBYA": "LY", "LITHUANIA": "LT",
    "LUXEMBOURG": "LU",
    "MADAGASCAR": "MG", "MALAWI": "MW", "MALAYSIA": "MY",
    "MALDIVES": "MV", "MALI": "ML", "MALTA": "MT",
    "MARSHALL ISLANDS": "MH", "MAURITANIA": "MR",
    "MAURITIUS": "MU", "MEXICO": "MX", "MICRONESIA": "FM",
    "MICRONESIA, FED. STATES OF": "FM",
    "MOLDOVA": "MD", "MONGOLIA": "MN", "MONTENEGRO": "ME",
    "MOROCCO": "MA", "MOZAMBIQUE": "MZ", "MYANMAR": "MM",
    "NAMIBIA": "NA", "NEPAL": "NP", "NETHERLANDS": "NL",
    "NEW ZEALAND": "NZ", "NICARAGUA": "NI", "NIGER": "NE",
    "NIGERIA": "NG", "NORTH MACEDONIA": "MK", "NORWAY": "NO",
    "OMAN": "OM",
    "PAKISTAN": "PK", "PALAU": "PW", "PANAMA": "PA",
    "PAPUA NEW GUINEA": "PG", "PARAGUAY": "PY", "PERU": "PE",
    "PHILIPPINES": "PH", "POLAND": "PL", "PORTUGAL": "PT",
    "QATAR": "QA",
    "ROMANIA": "RO", "RUSSIA": "RU", "RUSSIAN FEDERATION": "RU",
    "RWANDA": "RW",
    "SAMOA": "WS", "SAO TOME AND PRINCIPE": "ST",
    "SAUDI ARABIA": "SA", "SENEGAL": "SN", "SERBIA": "RS",
    "SEYCHELLES": "SC", "SIERRA LEONE": "SL",
    "SLOVAK REPUBLIC": "SK", "SLOVENIA": "SI",
    "SOLOMON ISLANDS": "SB", "SOMALIA": "SO",
    "SOUTH AFRICA": "ZA", "SOUTH SUDAN": "SS",
    "SOUTH SUDAN, REPUBLIC OF": "SS",
    "SPAIN": "ES", "SRI LANKA": "LK", "SUDAN": "SD",
    "SURINAME": "SR", "SWEDEN": "SE", "SWITZERLAND": "CH",
    "SYRIA": "SY",
    "TAIWAN": "TW", "TAJIKISTAN": "TJ", "TANZANIA": "TZ",
    "TANZANIA, UNITED REPUBLIC OF": "TZ",
    "THAILAND": "TH", "TIMOR-LESTE": "TL", "TOGO": "TG",
    "TONGA": "TO", "TRINIDAD AND TOBAGO": "TT", "TUNISIA": "TN",
    "TURKEY": "TR", "TURKIYE, REPUBLIC OF": "TR",
    "TURKMENISTAN": "TM", "TUVALU": "TV",
    "UGANDA": "UG", "UKRAINE": "UA",
    "UNITED ARAB EMIRATES": "AE",
    "UNITED KINGDOM": "GB", "UNITED STATES": "US",
    "URUGUAY": "UY", "UZBEKISTAN": "UZ",
    "VANUATU": "VU", "VENEZUELA": "VE",
    "VIETNAM": "VN", "VIET NAM": "VN",
    "WEST BANK AND GAZA": "PS",
    "YEMEN": "YE", "ZAMBIA": "ZM", "ZIMBABWE": "ZW",
    # Common variants with trailing spaces (MONA quirk)
    "COTE D'IVOIRE": "CI", "CÔTE D'IVOIRE": "CI",
    "CONGO, REPUBLIC OF ": "CG",
    "NORTH MACEDONIA ": "MK",
    "UNITED STATES VIRGIN ISLANDS ": "VI",
    "GAMBIA, THE ": "GM",
}

def _normalize_mona_name(raw: str) -> str:
    """Strip trailing whitespace/punctuation MONA sometimes includes."""
    return raw.strip().rstrip(" ,")

def country_iso(raw_name: str) -> str | None:
    """Return ISO alpha-2 from a MONA country name string."""
    name = _normalize_mona_name(raw_name).upper()
    if name in IMF_COUNTRY_MAP:
        return IMF_COUNTRY_MAP[name]
    # Partial match: strip common suffixes
    for suffix in [",ISLAMIC REPUBLIC OF", ", ISLAMIC REPUBLIC OF",
                   ", ARAB REPUBLIC OF", ", REPUBLIC OF", ", THE"]:
        if name.endswith(suffix.upper()):
            base = name[: -len(suffix)].strip()
            if base in IMF_COUNTRY_MAP:
                return IMF_COUNTRY_MAP[base]
    return None

# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------

def _session():
    if _HAS_CFFI:
        return cffi_requests.Session()
    raise RuntimeError("curl_cffi not available — required for IMF access")


def _fetch(session, url: str, timeout: int = 30) -> bytes:
    """Fetch URL with browser impersonation. Returns raw bytes."""
    resp = session.get(url, impersonate="chrome120", timeout=timeout)
    if resp.status_code != 200:
        raise IOError(f"HTTP {resp.status_code} from {url}")
    return resp.content

# ---------------------------------------------------------------------------
# SDR/USD exchange rate
# ---------------------------------------------------------------------------

def fetch_sdr_rate(session) -> tuple[float, str]:
    """
    Returns (usd_per_sdr, source_note).
    Parses the "Currency units per SDR" table from rms_five.aspx:
    looks for the U.S. dollar row and reads the most-recent-day value.
    """
    try:
        html = _fetch(session, SDR_RATE_URL).decode("latin-1")
        soup = BeautifulSoup(html, "html.parser")
        # The "Currency units per SDR" data is in one of the tables on the page.
        # Scan all tables; in each, use find_all("tr") (recursive, handles <tbody>)
        # and look for the U.S. dollar row.
        for table in soup.find_all("table"):
            rows = table.find_all("tr")
            # Check title row for "Currency units per SDR"
            if not rows:
                continue
            title = rows[0].get_text(" ", strip=True)
            # Must start with (not just contain) "Currency units per SDR"
            # to exclude the outer wrapper table whose title reads
            # "SDRs per Currency unit and Currency units per SDR..."
            if not title.startswith("Currency units per SDR"):
                continue
            # Data rows start at index 2 (index 1 is the date header)
            for row in rows[2:]:
                cells = [td.get_text(strip=True) for td in row.find_all(["td", "th"])]
                if not cells:
                    continue
                if "u.s. dollar" in cells[0].lower():
                    raw = cells[1].replace(",", "")
                    rate = float(raw)
                    return rate, "live"
        raise ValueError("U.S. dollar row not found in SDR table")
    except Exception as exc:
        print(f"  WARN: SDR rate fetch failed: {exc} — using fallback {SDR_FALLBACK}")
    return SDR_FALLBACK, "fallback"

# ---------------------------------------------------------------------------
# XLSX parser (stdlib only — no openpyxl required)
# ---------------------------------------------------------------------------

NS = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"

def _parse_xlsx(content: bytes) -> tuple[list[str], list[dict]]:
    """
    Parse an xlsx file from raw bytes.
    Returns (header_list, list_of_row_dicts).
    All cell values are strings; dates remain as Excel serial strings.
    """
    zf = zipfile.ZipFile(io.BytesIO(content))

    # Load shared strings
    shared: list[str] = []
    with zf.open("xl/sharedStrings.xml") as f:
        tree = ET.parse(f)
        for si in tree.findall(f".//{NS}si"):
            t = "".join(
                x.text or ""
                for x in si.iter(f"{NS}t")
            )
            shared.append(t)

    # Load sheet1
    with zf.open("xl/worksheets/sheet1.xml") as f:
        tree = ET.parse(f)

    rows_el = tree.findall(f".//{NS}row")

    def cell_val(c) -> str:
        typ = c.get("t", "")
        v = c.find(f"{NS}v")
        if v is None:
            return ""
        if typ == "s":
            try:
                return shared[int(v.text)]
            except (ValueError, IndexError):
                return ""
        return v.text or ""

    if not rows_el:
        return [], []

    header = [cell_val(c) for c in rows_el[0].findall(f"{NS}c")]

    records = []
    for row in rows_el[1:]:
        cells = [cell_val(c) for c in row.findall(f"{NS}c")]
        rec = {}
        for i, h in enumerate(header):
            rec[h.strip()] = cells[i].strip() if i < len(cells) else ""
        records.append(rec)

    return header, records


def excel_date(serial: str) -> str | None:
    """Convert Excel date serial to ISO date string, or None."""
    if not serial:
        return None
    try:
        n = int(float(serial))
        d = date(1899, 12, 30) + timedelta(days=n)
        return d.isoformat() if d.year >= 1900 else None
    except (ValueError, TypeError):
        return None

# ---------------------------------------------------------------------------
# Signal ID (deduplication)
# ---------------------------------------------------------------------------

def make_imf_id(*parts) -> str:
    """Stable 12-char hex ID for deduplication."""
    key = "|".join(str(p) for p in parts)
    return hashlib.md5(key.encode()).hexdigest()[:12]

# ---------------------------------------------------------------------------
# Signal builders
# ---------------------------------------------------------------------------

def build_signal(
    *,
    iso: str | None,
    imf_signal_type: str,
    signal_date: str,
    title: str,
    value_usd: float | None,
    description: str,
    arrangement_type: str,
    sdr_amount: float | None,
    arr_number: str,
    page_url: str | None,
    imf_id: str,
) -> dict:
    return {
        "iso": iso,
        "source": "imf",
        "signal_date": signal_date,
        "title": title,
        "value_usd": round(value_usd, 2) if value_usd is not None else None,
        "description": description,
        "raw_score": profile_score(iso),
        "imf_signal_type": imf_signal_type,
        "arrangement_type": arrangement_type,
        "sdr_amount": round(sdr_amount, 2) if sdr_amount is not None else None,
        "arr_number": arr_number,
        "page_url": page_url,
        "imf_id": imf_id,
    }

def country_label(iso2: str | None, raw_name: str) -> str:
    """Return readable country label, preferring profile name."""
    if iso2:
        p = Path(__file__).parent.parent / "data" / "profiles" / f"{iso2}.json"
        try:
            return json.loads(p.read_text()).get("name") or raw_name.title()
        except Exception:
            pass
    return raw_name.title()

# ---------------------------------------------------------------------------
# MONA signal extractors
# ---------------------------------------------------------------------------

def extract_delays(
    records: list[dict],
    today: date,
    threshold_days: int,
) -> list[dict]:
    """
    From MONA Reviews.xlsx records, emit one signal per program
    that has the most-overdue unresolved review (excluding R0 initial approvals).
    """
    min_end = (today - timedelta(days=180)).isoformat()

    # Per-arrangement: track worst (most overdue) unresolved review
    worst: dict[str, dict] = {}

    for rec in records:
        review_type = rec.get("Review Type", "").strip()
        if not review_type or review_type.upper() == "R0":
            continue  # skip initial-approval rows

        # Use revised date if available, else original
        sched = (
            excel_date(rec.get("Program Review Revised Date", ""))
            or excel_date(rec.get("Program Review Date", ""))
        )
        if not sched:
            continue
        if sched >= today.isoformat():
            continue  # not yet due

        completed = (
            excel_date(rec.get("Program Review Completed Date", ""))
        )
        if completed:
            continue  # already done

        # Program must still be active (not expired too long ago)
        end = (
            excel_date(rec.get("Revised End Date", ""))
            or excel_date(rec.get("Initial End Date", ""))
        )
        if not end or end < min_end:
            continue

        days_overdue = (today - date.fromisoformat(sched)).days
        if days_overdue < threshold_days:
            continue

        arr_num = rec.get("Arrangement Number", "")
        existing = worst.get(arr_num)
        if existing is None or days_overdue > existing["days"]:
            worst[arr_num] = {
                "arr_num": arr_num,
                "country": rec.get("Country Name", "").strip(),
                "arr_type": rec.get("Arrangement Type", "").strip(),
                "sched_date": sched,
                "end_date": end,
                "review_type": review_type,
                "days": days_overdue,
            }

    return list(worst.values())


def extract_approvals(
    desc_records: list[dict],
    today: date,
    lookback_days: int,
) -> list[dict]:
    """
    From MONA Description.xlsx, find new program approvals (R0 rows)
    within the lookback window.
    One signal per arrangement number.
    """
    cutoff = (today - timedelta(days=lookback_days + MONA_LAG_DAYS)).isoformat()
    seen: set[str] = set()
    results = []

    for rec in desc_records:
        review_type = rec.get("Review Type", "").strip().upper()
        if review_type != "R0":
            continue
        arr_num = rec.get("Arrangement Number", "")
        if arr_num in seen:
            continue
        board_date = excel_date(rec.get("Board Action Date", ""))
        if not board_date or board_date < cutoff:
            continue
        seen.add(arr_num)

        sdr_raw = rec.get("Totalaccess", "")
        try:
            sdr_amt = float(sdr_raw)
        except (ValueError, TypeError):
            sdr_amt = None

        results.append({
            "arr_num": arr_num,
            "country": rec.get("Country Name", "").strip(),
            "arr_type": rec.get("Arrangement Type", "").strip(),
            "board_date": board_date,
            "sdr_amount": sdr_amt,
        })

    return results


def extract_disbursements(
    purch_records: list[dict],
    today: date,
    lookback_days: int,
    min_sdr: float,
) -> list[dict]:
    """
    From MONA Purchases.xlsx, find actual disbursements within the
    extended lookback window (accounting for MONA's data lag).
    """
    cutoff = (today - timedelta(days=lookback_days + MONA_LAG_DAYS)).isoformat()
    results = []

    for rec in purch_records:
        actual_date = excel_date(rec.get("Actual Date", ""))
        if not actual_date or actual_date < cutoff:
            continue
        if actual_date > today.isoformat():
            continue

        sdr_raw = rec.get("Actual Amount", "")
        try:
            sdr_amt = float(sdr_raw)
        except (ValueError, TypeError):
            continue
        if sdr_amt < min_sdr:
            continue

        results.append({
            "arr_num": rec.get("Arrangement Number", ""),
            "country": rec.get("Country Name", "").strip(),
            "arr_type": rec.get("Arrangement Type", "").strip(),
            "actual_date": actual_date,
            "sdr_amount": sdr_amt,
            "basis": rec.get("Actual Basis", "").strip(),
            "review_type": rec.get("Review Type", "").strip(),
        })

    return results

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    out_path = Path(__file__).parent.parent / "data" / "imf_signals.json"
    today = date.today()

    print(f"fetch_imf.py — {today.isoformat()}")
    print(f"  Lookback: {LOOKBACK_DAYS}d (disbursements/approvals extended by {MONA_LAG_DAYS}d MONA lag)")
    print(f"  Delay threshold: {DELAY_THRESHOLD_DAYS}d overdue")

    sess = _session()

    # --- SDR rate ---
    sdr_rate, rate_source = fetch_sdr_rate(sess)
    print(f"  SDR/USD rate: {sdr_rate} ({rate_source})")
    time.sleep(0.5)

    # --- Download MONA files ---
    print("  Fetching MONA Reviews.xlsx ...")
    rev_bytes = _fetch(sess, MONA_BASE + "ArrangementsData/Reviews.xlsx", timeout=60)
    time.sleep(0.5)

    print("  Fetching MONA Description.xlsx ...")
    desc_bytes = _fetch(sess, MONA_BASE + "ArrangementsData/Description.xlsx", timeout=60)
    time.sleep(0.5)

    print("  Fetching MONA Purchases.xlsx ...")
    purch_bytes = _fetch(sess, MONA_BASE + "ArrangementsData/Purchases.xlsx", timeout=60)

    # --- Parse ---
    print("  Parsing xlsx files ...")
    _, rev_records = _parse_xlsx(rev_bytes)
    _, desc_records = _parse_xlsx(desc_bytes)
    _, purch_records = _parse_xlsx(purch_bytes)
    print(f"    Reviews: {len(rev_records)} rows, Description: {len(desc_records)} rows, Purchases: {len(purch_records)} rows")

    # --- Extract signals ---
    delay_data = extract_delays(rev_records, today, DELAY_THRESHOLD_DAYS)
    approval_data = extract_approvals(desc_records, today, LOOKBACK_DAYS)
    disburse_data = extract_disbursements(purch_records, today, LOOKBACK_DAYS, DISBURSE_MIN_SDR)

    print(f"  Raw extracts — delays: {len(delay_data)}, approvals: {len(approval_data)}, disbursements: {len(disburse_data)}")

    # --- Build signals ---
    # Individual arrangement URLs are not programmatically derivable from the
    # arr_number alone — MONA uses JavaScript-rendered pages with no stable
    # per-arrangement deep link. The MONA index page is the best available URL.
    # For approval signals, a dated press release exists at
    # https://www.imf.org/en/News/Articles/... but its URL requires a separate
    # search by country + board_date, which is not in scope here.
    mona_page = "https://www.imf.org/external/np/pdr/mona/index.aspx"
    signals: list[dict] = []

    # 1. DELAYS (highest priority)
    for d in sorted(delay_data, key=lambda x: x["days"], reverse=True):
        iso2 = country_iso(d["country"])
        label = country_label(iso2, d["country"])
        arr_label = ARRANGEMENT_LABELS.get(d["arr_type"], d["arr_type"])
        months_over = round(d["days"] / 30.4)
        title = f"{label} — {arr_label} review delayed {months_over} months"
        desc = (
            f"{arr_label} scheduled review ({d['review_type']}) overdue by {d['days']} days. "
            f"Arrangement expires {d['end_date']}. "
            "Review delay signals potential conditionality failure or political impasse."
        )
        imf_id = make_imf_id("delay", d["arr_num"], d["review_type"], d["sched_date"])
        signals.append(build_signal(
            iso=iso2,
            imf_signal_type="delay",
            signal_date=d["sched_date"],
            title=title,
            value_usd=None,
            description=desc,
            arrangement_type=d["arr_type"],
            sdr_amount=None,
            arr_number=d["arr_num"],
            page_url=mona_page,
            imf_id=imf_id,
        ))

    # 2. NEW APPROVALS
    for a in sorted(approval_data, key=lambda x: x["board_date"], reverse=True):
        iso2 = country_iso(a["country"])
        label = country_label(iso2, a["country"])
        arr_label = ARRANGEMENT_LABELS.get(a["arr_type"], a["arr_type"])
        sdr = a["sdr_amount"]
        usd = round(sdr * sdr_rate / 1e6, 2) * 1e6 if sdr else None  # keep as dollars
        usd_val = sdr * sdr_rate * 1_000_000 if sdr else None  # SDR millions → USD
        sdr_str = f"SDR {sdr:.1f}M" if sdr else "amount undisclosed"
        title = f"{label} — {arr_label} approved ({sdr_str})"
        desc = (
            f"IMF Executive Board approved new {arr_label} for {label}. "
            f"Total access: {sdr_str}"
            + (f" (~${sdr * sdr_rate / 1000:.1f}B)" if sdr and sdr * sdr_rate >= 1000 else
               f" (~${sdr * sdr_rate:.0f}M)" if sdr else "")
            + ". Board approval date per MONA."
        )
        imf_id = make_imf_id("approval", a["arr_num"], a["board_date"])
        signals.append(build_signal(
            iso=iso2,
            imf_signal_type="approval",
            signal_date=a["board_date"],
            title=title,
            value_usd=usd_val,
            description=desc,
            arrangement_type=a["arr_type"],
            sdr_amount=sdr,
            arr_number=a["arr_num"],
            page_url=mona_page,
            imf_id=imf_id,
        ))

    # 3. DISBURSEMENTS
    for d in sorted(disburse_data, key=lambda x: x["actual_date"], reverse=True):
        iso2 = country_iso(d["country"])
        label = country_label(iso2, d["country"])
        arr_label = ARRANGEMENT_LABELS.get(d["arr_type"], d["arr_type"])
        sdr = d["sdr_amount"]
        usd_val = sdr * sdr_rate * 1_000_000  # SDR millions → USD
        sdr_str = f"SDR {sdr:.1f}M"
        review = d["review_type"].strip()
        basis_short = d["basis"][:80] if d["basis"] else arr_label
        title = f"{label} — {arr_label} disbursement {sdr_str}"
        desc = (
            f"Actual disbursement of {sdr_str} (~${sdr * sdr_rate:.0f}M USD) "
            f"under {arr_label}. "
            + (f"Review: {review}. " if review and review.upper() != "R0" else "Initial tranche. ")
            + (f"Basis: {basis_short}." if basis_short and basis_short != arr_label else "")
        )
        imf_id = make_imf_id("disbursement", d["arr_num"], d["actual_date"], str(sdr))
        signals.append(build_signal(
            iso=iso2,
            imf_signal_type="disbursement",
            signal_date=d["actual_date"],
            title=title,
            value_usd=round(usd_val, 2),
            description=desc.strip(),
            arrangement_type=d["arr_type"],
            sdr_amount=sdr,
            arr_number=d["arr_num"],
            page_url=mona_page,
            imf_id=imf_id,
        ))

    # --- Write output ---
    output = {
        "generated_at": f"{today.isoformat()}T00:00:00+00:00",
        "sources": ["imf"],
        "sdr_usd_rate": sdr_rate,
        "sdr_rate_source": rate_source,
        "signals": signals,
    }

    out_path.write_text(json.dumps(output, indent=2))

    # --- Summary ---
    by_type: dict[str, int] = {}
    for s in signals:
        t = s["imf_signal_type"]
        by_type[t] = by_type.get(t, 0) + 1

    print(f"\n  Signals written: {len(signals)} → {out_path.name}")
    for t, n in sorted(by_type.items()):
        print(f"    {t}: {n}")

    # Print sample
    print("\n  Sample signals:")
    for s in signals[:5]:
        print(f"    [{s['imf_signal_type']:14s}] {s['signal_date']}  {s['title'][:65]}")


if __name__ == "__main__":
    main()
