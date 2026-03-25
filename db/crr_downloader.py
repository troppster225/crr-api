"""
CRR Auction Results Downloader
================================
Downloads CRR auction zip files from ERCOT's MIS portal and stores
only the MarketResults CSV from each zip (discards the rest).

ERCOT MIS portal:  https://mis.ercot.com/misapp/GetReports.do
Authentication:    TLS client certificate (no SOAP / WS-Security needed for public reports)

Report IDs:
    11201  —  Monthly CRR Auction Results
    11203  —  Annual CRR Auction Sequence Results

Environment variables required:
    CERT_PEM     path to client_cert.pem
    CERT_KEY     path to client_key.pem
    MIMIC_DUNS   DUNS number for download URL (e.g. 0218539729999)

Usage:
    # Download all monthly + annual reports for calendar year 2026
    python crr_downloader.py --year 2026

    # Download only monthly reports for 2025
    python crr_downloader.py --type monthly --year 2025

    # Dry-run (list what would be downloaded without downloading)
    python crr_downloader.py --year 2026 --dry-run
"""

import argparse
import io
import json
import logging
import os
import re
import sys
import time
import zipfile
from datetime import date
from pathlib import Path
from typing import Optional

import requests

# ─────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────
LIST_URL     = "https://mis.ercot.com/misapp/GetReports.do?reportTypeId={report_type_id}&lang=en_US"
DOWNLOAD_URL = "https://mis.ercot.com/misdownload/servlets/mirDownload?mimic_duns={duns}&doclookupId={doc_id}"

REPORT_MONTHLY  = 11201
REPORT_ANNUAL   = 11203

DEFAULT_DOWNLOAD_DIR = Path(__file__).parent / "downloads"
STATE_FILE           = Path(__file__).parent / "downloads" / ".download_state.json"

REQUEST_DELAY_SEC = 1.0
MAX_RETRIES       = 3

logging.basicConfig(
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    level=logging.INFO,
)
log = logging.getLogger(__name__)


# ─────────────────────────────────────────────
# ERCOT MIS HTML PARSING
# ─────────────────────────────────────────────

# Matches filename like: rpt.00011201.0000000000000000.20260122.080059537.FEB2026MonthlyCRRAuctionResults.zip
FILENAME_RE  = re.compile(r'(rpt\.\d+\.[^\s<"]+\.zip)', re.IGNORECASE)
# Matches doclookupId=1234567890 in href attributes
DOC_ID_RE    = re.compile(r'doclookupId=(\d+)')


def parse_report_list_html(html: str) -> list[dict]:
    """
    Parse the MIS GetReports HTML page and return a list of dicts:
        [{"filename": "rpt.00011201...FEB2026Monthly...zip", "doc_id": "1185205369"}, ...]

    The HTML structure pairs filenames and download links in adjacent table cells:
        <td class='labelOptional_ind'>rpt.00011201....FEB2026...zip</td>
        ...
        <td><a href='/misdownload/servlets/mirDownload?mimic_duns=...&doclookupId=1185205369'>zip</a></td>

    Strategy: find all filenames and all doc_ids in document order, then zip them.
    This is reliable because the MIS HTML consistently lists them in matching order.
    """
    filenames = FILENAME_RE.findall(html)
    doc_ids   = DOC_ID_RE.findall(html)

    if not filenames or not doc_ids:
        log.warning("No reports found in HTML — page may have changed format or auth failed")
        return []

    if len(filenames) != len(doc_ids):
        log.warning(
            "Filename count (%d) != doc_id count (%d) — using min()",
            len(filenames), len(doc_ids)
        )

    return [
        {"filename": fn, "doc_id": did}
        for fn, did in zip(filenames, doc_ids)
    ]


# ─────────────────────────────────────────────
# AUCTION METADATA PARSING  (from filename)
# ─────────────────────────────────────────────

MONTHLY_PAT = re.compile(r"(?P<mon>[A-Z]{3})(?P<yr>\d{4})Monthly", re.IGNORECASE)
ANNUAL_PAT  = re.compile(r"(?P<yr>\d{4})(?P<half>1st6|2nd6)AnnualAuctionSeq(?P<seq>\d)", re.IGNORECASE)
MONTH_ABBR  = {
    "JAN": 1, "FEB": 2, "MAR": 3, "APR": 4, "MAY": 5, "JUN": 6,
    "JUL": 7, "AUG": 8, "SEP": 9, "OCT": 10, "NOV": 11, "DEC": 12,
}


def _days_in_month(year: int, month: int) -> int:
    import calendar
    return calendar.monthrange(year, month)[1]


def parse_report_metadata(filename: str) -> Optional[dict]:
    """
    Extract structured metadata from the zip filename.
    Returns dict with keys: auction_kind, delivery_start, delivery_end,
                             annual_period, sequence_num
    Returns None if filename doesn't match either pattern.
    """
    m = MONTHLY_PAT.search(filename)
    if m:
        yr = int(m.group("yr"))
        mo = MONTH_ABBR.get(m.group("mon").upper())
        if not mo:
            return None
        return {
            "auction_kind":   "MONTHLY",
            "delivery_start": date(yr, mo, 1),
            "delivery_end":   date(yr, mo, _days_in_month(yr, mo)),
            "annual_period":  None,
            "sequence_num":   None,
        }

    m = ANNUAL_PAT.search(filename)
    if m:
        yr   = int(m.group("yr"))
        half = m.group("half").lower()
        seq  = int(m.group("seq"))
        if half == "1st6":
            first, last = date(yr, 1, 1), date(yr, 6, 30)
        else:
            first, last = date(yr, 7, 1), date(yr, 12, 31)
        return {
            "auction_kind":   "ANNUAL",
            "delivery_start": first,
            "delivery_end":   last,
            "annual_period":  f"{yr}.{half}",
            "sequence_num":   seq,
        }

    return None


def filename_in_year(filename: str, year: int) -> bool:
    """Return True if this auction's delivery period overlaps the given calendar year."""
    meta = parse_report_metadata(filename)
    if not meta:
        return False
    return (
        meta["delivery_start"].year <= year <= meta["delivery_end"].year
    )


# ─────────────────────────────────────────────
# DOWNLOAD STATE  (avoid re-downloading)
# ─────────────────────────────────────────────

def load_state(state_file: Path) -> set:
    if state_file.exists():
        with open(state_file) as f:
            return set(json.load(f).get("downloaded", []))
    return set()


def save_state(state_file: Path, downloaded: set):
    state_file.parent.mkdir(parents=True, exist_ok=True)
    with open(state_file, "w") as f:
        json.dump({"downloaded": sorted(downloaded)}, f, indent=2)


# ─────────────────────────────────────────────
# CORE DOWNLOAD LOGIC
# ─────────────────────────────────────────────

def fetch_report_list(report_type_id: int, cert: tuple) -> list[dict]:
    """Fetch and parse the HTML report list for a given report type."""
    url = LIST_URL.format(report_type_id=report_type_id)
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = requests.get(url, cert=cert, timeout=30, allow_redirects=True)
            r.raise_for_status()
            return parse_report_list_html(r.text)
        except Exception as e:
            if attempt == MAX_RETRIES:
                raise
            log.warning("Attempt %d failed fetching report list: %s — retrying…", attempt, e)
            time.sleep(REQUEST_DELAY_SEC * attempt)
    return []


def download_and_extract_csv(doc_id: str, mimic_duns: str, cert: tuple) -> Optional[bytes]:
    """
    Download the zip for a given doclookupId and extract only the MarketResults CSV.
    Returns the CSV bytes, or None on failure.
    """
    url = DOWNLOAD_URL.format(duns=mimic_duns, doc_id=doc_id)
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = requests.get(url, cert=cert, timeout=120, stream=False)
            r.raise_for_status()
            break
        except Exception as e:
            if attempt == MAX_RETRIES:
                log.error("Failed downloading doc_id=%s after %d attempts: %s", doc_id, attempt, e)
                return None
            log.warning("Download attempt %d failed: %s — retrying…", attempt, e)
            time.sleep(REQUEST_DELAY_SEC * attempt)

    # Extract the MarketResults CSV from the in-memory zip
    try:
        zf = zipfile.ZipFile(io.BytesIO(r.content))
    except zipfile.BadZipFile as e:
        log.error("doc_id=%s: response is not a valid ZIP (%s)", doc_id, e)
        return None

    csv_candidates = [
        n for n in zf.namelist()
        if "marketresult" in n.lower() and n.lower().endswith(".csv")
    ]
    if not csv_candidates:
        log.warning("doc_id=%s: no MarketResults CSV found in ZIP. Contents: %s", doc_id, zf.namelist())
        return None

    if len(csv_candidates) > 1:
        log.warning("doc_id=%s: multiple MarketResults CSVs found, using first: %s", doc_id, csv_candidates)

    return zf.read(csv_candidates[0])


def run_download(
    cert:         tuple,
    mimic_duns:   str,
    download_dir: Path,
    report_types: list[int],
    year:         Optional[int]       = None,
    dry_run:      bool                = False,
) -> list[Path]:
    """
    Download all available CRR auction results, filtered by year if provided.
    Returns list of saved CSV file paths.
    """
    download_dir.mkdir(parents=True, exist_ok=True)
    downloaded_ids = load_state(STATE_FILE)
    saved_paths    = []

    for rtype in report_types:
        label = "monthly" if rtype == REPORT_MONTHLY else "annual"
        log.info("Fetching %s report list (type %d)…", label, rtype)

        try:
            entries = fetch_report_list(rtype, cert)
        except Exception as e:
            log.error("Failed to fetch %s report list: %s", label, e)
            continue

        log.info("  Found %d total entries", len(entries))

        # Filter by year if specified
        if year is not None:
            entries = [e for e in entries if filename_in_year(e["filename"], year)]
            log.info("  %d entries match year %d", len(entries), year)

        for entry in entries:
            filename = entry["filename"]
            doc_id   = entry["doc_id"]

            # Output path: same name as zip but with .csv extension
            csv_name = re.sub(r'\.zip$', '.csv', filename, flags=re.IGNORECASE)
            dest     = download_dir / csv_name

            if doc_id in downloaded_ids:
                log.info("  [skip] Already downloaded: %s", csv_name)
                saved_paths.append(dest)
                continue

            if dest.exists():
                log.info("  [skip] File exists on disk: %s", csv_name)
                downloaded_ids.add(doc_id)
                save_state(STATE_FILE, downloaded_ids)
                saved_paths.append(dest)
                continue

            meta = parse_report_metadata(filename)
            if not meta:
                log.warning("  [skip] Unrecognised filename: %s", filename)
                continue

            if dry_run:
                log.info("  [dry-run] Would download: %s  (doc_id=%s, %s)",
                         csv_name, doc_id, meta["auction_kind"])
                continue

            log.info("  Downloading: %s  (doc_id=%s)", csv_name, doc_id)
            csv_bytes = download_and_extract_csv(doc_id, mimic_duns, cert)
            if csv_bytes is None:
                continue

            dest.write_bytes(csv_bytes)
            size_kb = len(csv_bytes) / 1024
            log.info("    Saved → %s  (%.1f KB)", csv_name, size_kb)

            downloaded_ids.add(doc_id)
            save_state(STATE_FILE, downloaded_ids)
            saved_paths.append(dest)

            time.sleep(REQUEST_DELAY_SEC)

    return saved_paths


# ─────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Download ERCOT CRR auction MarketResults CSVs")
    parser.add_argument("--year",  type=int,
                        help="Filter to reports whose delivery period overlaps this calendar year")
    parser.add_argument("--type",  choices=["monthly", "annual", "all"], default="all",
                        help="Which report types to download (default: all)")
    parser.add_argument("--out",   default=str(DEFAULT_DOWNLOAD_DIR),
                        help="Destination directory (default: ./downloads/)")
    parser.add_argument("--dry-run", action="store_true",
                        help="List what would be downloaded without downloading")
    args = parser.parse_args()

    # Auth from environment
    cert_pem    = os.environ.get("CERT_PEM")
    cert_key    = os.environ.get("CERT_KEY")
    mimic_duns  = os.environ.get("MIMIC_DUNS", "0218539729999")

    if not cert_pem or not cert_key:
        sys.exit(
            "ERROR: Set CERT_PEM and CERT_KEY environment variables.\n"
            "  export CERT_PEM=/path/to/client_cert.pem\n"
            "  export CERT_KEY=/path/to/client_key.pem"
        )

    cert = (cert_pem, cert_key)

    rtype_map = {
        "monthly": [REPORT_MONTHLY],
        "annual":  [REPORT_ANNUAL],
        "all":     [REPORT_MONTHLY, REPORT_ANNUAL],
    }
    report_types = rtype_map[args.type]
    download_dir = Path(args.out)

    paths = run_download(
        cert         = cert,
        mimic_duns   = mimic_duns,
        download_dir = download_dir,
        report_types = report_types,
        year         = args.year,
        dry_run      = args.dry_run,
    )

    if not args.dry_run:
        log.info("\nDone. %d CSV(s) ready in %s", len(paths), download_dir)
        log.info("Next step: python etl_loader.py")


if __name__ == "__main__":
    main()
