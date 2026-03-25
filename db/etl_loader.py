"""
CRR ETL Loader
==============
Reads downloaded ERCOT CRR auction files (zip or pre-extracted csv), parses the
MarketResults data, maps source/sink nodes to load zones, calculates Column1 hours
+ Column2 values, and loads everything into PostgreSQL.

Usage:
    # Load everything in the downloads folder (zip or csv)
    python etl_loader.py

    # Load a specific file (zip or csv)
    python etl_loader.py --file downloads/rpt.00011201...FEB2026.csv

    # Load your node-zone mapping CSV
    python etl_loader.py --load-zone-map ercot_node_zone_map.csv

    # Recalculate Column1/Column2 for all rows (after fixing holiday table)
    python etl_loader.py --recalc-values

Connection string:
    Set DATABASE_URL environment variable:
        export DATABASE_URL="postgresql://user:password@your-vps-ip:5432/crr_db"
"""

import os
import re
import sys
import zipfile
import logging
import calendar
import argparse
from datetime import date, timedelta, datetime
from pathlib import Path
from typing import Optional

import pandas as pd
import psycopg2
import psycopg2.extras

logging.basicConfig(
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    level=logging.INFO,
)
log = logging.getLogger(__name__)

DEFAULT_DOWNLOAD_DIR = Path(__file__).parent / "downloads"

MONTH_ABBR = {
    "JAN": 1, "FEB": 2, "MAR": 3, "APR": 4, "MAY": 5, "JUN": 6,
    "JUL": 7, "AUG": 8, "SEP": 9, "OCT": 10, "NOV": 11, "DEC": 12,
}

# ─────────────────────────────────────────────
# DATABASE CONNECTION
# ─────────────────────────────────────────────

def get_conn(dsn: str = None):
    dsn = dsn or os.environ.get("DATABASE_URL")
    if not dsn:
        sys.exit("ERROR: Set DATABASE_URL environment variable.\n"
                 "  e.g. export DATABASE_URL='postgresql://user:pass@host:5432/crr_db'")
    return psycopg2.connect(dsn)


# ─────────────────────────────────────────────
# NERC HOLIDAY CALCULATION
# ─────────────────────────────────────────────

def _nerc_holidays_for_year(year: int) -> set:
    """Return ERCOT NERC holiday dates for a year."""
    def last_mon(y, m):
        d = date(y, m, calendar.monthrange(y, m)[1])
        while d.weekday() != 0: d -= timedelta(1)
        return d

    def nth_weekday(y, m, wd, n):
        d, count = date(y, m, 1), 0
        while True:
            if d.weekday() == wd:
                count += 1
                if count == n: return d
            d += timedelta(1)

    def observe(d):
        if d.weekday() == 5: return d - timedelta(1)
        if d.weekday() == 6: return d + timedelta(1)
        return d

    return {
        observe(date(year, 1,  1)),         # New Year's Day
        last_mon(year, 5),                  # Memorial Day
        observe(date(year, 7,  4)),         # Independence Day
        nth_weekday(year, 9, 0, 1),         # Labor Day
        nth_weekday(year, 11, 3, 4),        # Thanksgiving
        observe(date(year, 12, 25)),        # Christmas
    }


_holiday_cache: dict[int, set] = {}

def is_ercot_holiday(d: date) -> bool:
    if d.year not in _holiday_cache:
        _holiday_cache[d.year] = _nerc_holidays_for_year(d.year)
    return d in _holiday_cache[d.year]


def peak_hours_for_period(start: date, end_inclusive: date) -> dict:
    """
    Calculate ERCOT peak hours for a date range [start, end_inclusive].
    Returns {'PeakWD': int, 'PeakWE': int, 'Off-peak': int}
    """
    wdpeak = wepeak = offpeak = 0
    d = start
    while d <= end_inclusive:
        if d.weekday() >= 5 or is_ercot_holiday(d):
            wepeak  += 16
            offpeak += 8
        else:
            wdpeak  += 16
            offpeak += 8
        d += timedelta(1)
    return {"PeakWD": wdpeak, "PeakWE": wepeak, "Off-peak": offpeak}


# Cache so we don't recompute the same contract period many times
_hours_cache: dict = {}

def get_contract_hours(start: date, end: date, tou: str) -> int:
    key = (start, end, tou)
    if key not in _hours_cache:
        _hours_cache[key] = peak_hours_for_period(start, end)
    return _hours_cache[key].get(tou, 0)


# ─────────────────────────────────────────────
# FILENAME → AUCTION METADATA PARSER
# ─────────────────────────────────────────────

MONTHLY_PAT = re.compile(r"(?P<mon>[A-Z]{3})(?P<yr>\d{4})Monthly", re.I)
ANNUAL_PAT  = re.compile(r"(?P<yr>\d{4})(?P<half>1st6|2nd6)AnnualAuctionSeq(?P<seq>\d)", re.I)

def parse_filename_meta(filename: str) -> dict:
    name = Path(filename).stem

    # Extract auction run date from filename (format: .YYYYMMDD.)
    run_date = None
    dm = re.search(r"\.(\d{8})\.", name)
    if dm:
        try:
            run_date = datetime.strptime(dm.group(1), "%Y%m%d").date()
        except ValueError:
            pass

    m = MONTHLY_PAT.search(name)
    if m:
        yr, mo = int(m.group("yr")), MONTH_ABBR.get(m.group("mon").upper())
        first  = date(yr, mo, 1)
        last   = date(yr, mo, calendar.monthrange(yr, mo)[1])
        return dict(report_type_id=11201, auction_kind="MONTHLY",
                    auction_name=name, auction_run_date=run_date,
                    delivery_start=first, delivery_end=last,
                    annual_period=None, sequence_num=None)

    m = ANNUAL_PAT.search(name)
    if m:
        yr, half, seq = int(m.group("yr")), m.group("half").lower(), int(m.group("seq"))
        first = date(yr, 1, 1) if half == "1st6" else date(yr, 7, 1)
        last  = date(yr, 6, 30) if half == "1st6" else date(yr, 12, 31)
        return dict(report_type_id=11203, auction_kind="ANNUAL",
                    auction_name=name, auction_run_date=run_date,
                    delivery_start=first, delivery_end=last,
                    annual_period=f"{yr}.{half}", sequence_num=seq)

    raise ValueError(f"Cannot parse auction metadata from filename: {filename}")


# ─────────────────────────────────────────────
# ZIP → DATAFRAME
# ─────────────────────────────────────────────

def _normalise_market_results_df(df: pd.DataFrame) -> pd.DataFrame:
    """Normalise column names and types for a MarketResults DataFrame."""
    df["StartDate"] = pd.to_datetime(df["StartDate"]).dt.date
    df["EndDate"]   = pd.to_datetime(df["EndDate"]).dt.date

    df = df.rename(columns={
        "CRR_ID":            "crr_id",
        "OriginalCRR_ID":    "original_crr_id",
        "AccountHolder":     "account_holder",
        "HedgeType":         "hedge_type",
        "BidType":           "bid_type",
        "CRRType":           "crr_type",
        "Source":            "source_node",
        "Sink":              "sink_node",
        "StartDate":         "start_date",
        "EndDate":           "end_date",
        "TimeOfUse":         "time_of_use",
        "Bid24Hour":         "bid_24hour",
        "MW":                "mw",
        "ShadowPricePerMWH": "shadow_price_mwh",
    })

    df["bid_24hour"] = df["bid_24hour"].map({"Yes": True, "No": False, True: True, False: False})
    df["crr_id"]     = df["crr_id"].astype("Int64")
    df["original_crr_id"] = df["original_crr_id"].astype("Int64")
    return df


def read_market_results_from_zip(zip_path: Path) -> pd.DataFrame:
    """Extract and parse the MarketResults CSV from an ERCOT auction zip."""
    with zipfile.ZipFile(zip_path) as zf:
        csv_files = [n for n in zf.namelist() if "MarketResults" in n and n.endswith(".csv")]
        if not csv_files:
            raise ValueError(f"No MarketResults CSV in {zip_path.name}")
        with zf.open(csv_files[0]) as f:
            df = pd.read_csv(f)
    return _normalise_market_results_df(df)


def read_market_results_from_csv(csv_path: Path) -> pd.DataFrame:
    """Parse a pre-extracted MarketResults CSV file (output of crr_downloader.py)."""
    df = pd.read_csv(csv_path, low_memory=False)
    df.columns = [c.strip() for c in df.columns]
    return _normalise_market_results_df(df)


def load_market_results_file(path: Path) -> pd.DataFrame:
    """
    Load a MarketResults file regardless of whether it's a .zip or a .csv.
    crr_downloader.py saves pre-extracted CSVs; legacy local files may be zips.
    """
    if path.suffix.lower() == ".zip":
        return read_market_results_from_zip(path)
    else:
        return read_market_results_from_csv(path)


# ─────────────────────────────────────────────
# ZONE MAPPING LOOKUP
# ─────────────────────────────────────────────

def load_zone_map_from_db(conn) -> dict:
    """Return {node_name: load_zone} from the database."""
    with conn.cursor() as cur:
        cur.execute("SELECT node_name, load_zone FROM node_zone_map WHERE load_zone IS NOT NULL")
        return {row[0]: row[1] for row in cur.fetchall()}


def load_zone_map_from_csv(csv_path: Path) -> pd.DataFrame:
    """
    Read your ERCOT node-zone mapping CSV.
    Expected columns (flexible — will try common names):
        node / node_name / SettlementPoint / BusName  → node identifier
        zone / load_zone / ZoneName / WeatherZone     → load zone
    """
    df = pd.read_csv(csv_path)

    # Normalise column names
    col_map = {}
    for c in df.columns:
        lc = c.lower().replace(" ", "_").replace("-", "_")
        if lc in ("node", "node_name", "settlementpoint", "busname", "resource_node"):
            col_map[c] = "node_name"
        elif lc in ("zone", "load_zone", "zonename", "weatherzone", "lz"):
            col_map[c] = "load_zone"

    df = df.rename(columns=col_map)
    if "node_name" not in df.columns or "load_zone" not in df.columns:
        raise ValueError(
            f"Could not find node/zone columns in {csv_path}. "
            f"Columns found: {list(df.columns)}"
        )
    return df[["node_name", "load_zone"]].dropna(subset=["node_name"])


# ─────────────────────────────────────────────
# DATABASE UPSERTS
# ─────────────────────────────────────────────

def upsert_auction_file(conn, meta: dict, filename: str) -> int:
    """Insert or update auction_file row. Returns the auction_file.id."""
    combined = {**meta, "doc_id": meta.get("doc_id"), "filename": filename}
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO auction_file
                (ercot_doc_id, report_type_id, auction_kind, auction_name,
                 auction_run_date, delivery_start, delivery_end,
                 annual_period_label, sequence_num, source_filename)
            VALUES (%(doc_id)s, %(report_type_id)s, %(auction_kind)s, %(auction_name)s,
                    %(auction_run_date)s, %(delivery_start)s, %(delivery_end)s,
                    %(annual_period)s, %(sequence_num)s, %(filename)s)
            ON CONFLICT (source_filename) DO UPDATE
                SET loaded_at = NULL
            RETURNING id
        """, combined)
        return cur.fetchone()[0]


def upsert_zone_map(conn, df: pd.DataFrame):
    """Bulk-upsert node → zone mapping rows."""
    rows = list(df[["node_name", "load_zone"]].itertuples(index=False, name=None))
    sql  = """
        INSERT INTO node_zone_map (node_name, load_zone, source)
        VALUES %s
        ON CONFLICT (node_name) DO UPDATE
            SET load_zone = EXCLUDED.load_zone,
                source    = EXCLUDED.source
    """
    data = [(n, z, "mapping_csv") for n, z in rows]
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, sql, data)
    conn.commit()
    log.info(f"  Zone map: upserted {len(data)} node→zone rows")


def _to_python(v):
    """Convert a pandas/numpy scalar to a Python-native type for psycopg2."""
    if v is None:
        return None
    try:
        if pd.isna(v):
            return None
    except (TypeError, ValueError):
        pass
    if hasattr(v, "item"):           # numpy scalar → Python scalar
        return v.item()
    return v


def bulk_insert_contracts(conn, df: pd.DataFrame, auction_file_id: int, zone_map: dict):
    """
    Insert contract rows into crr_market_result.
    Skips rows already present (by crr_id + time_of_use + auction_file_id).
    Returns count of inserted rows.
    """
    # Resolve zones
    df = df.copy()
    df["source_zone"] = df["source_node"].map(zone_map)
    df["sink_zone"]   = df["sink_node"].map(zone_map)
    df["auction_file_id"] = auction_file_id

    columns = [
        "auction_file_id", "crr_id", "original_crr_id", "account_holder",
        "hedge_type", "bid_type", "crr_type",
        "source_node", "sink_node", "start_date", "end_date",
        "time_of_use", "bid_24hour", "mw", "shadow_price_mwh",
        "source_zone", "sink_zone",
    ]

    rows = []
    for row in df[columns].itertuples(index=False, name=None):
        rows.append(tuple(_to_python(v) for v in row))

    sql = f"""
        INSERT INTO crr_market_result ({', '.join(columns)})
        VALUES %s
        ON CONFLICT (crr_id, time_of_use, auction_file_id) DO NOTHING
    """
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, sql, rows, page_size=2000)
        inserted = cur.rowcount
    conn.commit()
    return inserted


def calculate_and_store_values(conn, auction_file_id: int):
    """
    Calculate Column1 (hours) and Column2 (value $) for all contracts in this auction,
    then upsert into crr_contract_value.
    """
    with conn.cursor() as cur:
        cur.execute("""
            SELECT id, start_date, end_date, time_of_use, mw, shadow_price_mwh
            FROM crr_market_result
            WHERE auction_file_id = %s
        """, (auction_file_id,))
        rows = cur.fetchall()

    value_rows = []
    for row_id, start, end, tou, mw, price in rows:
        hours = get_contract_hours(start, end, tou)
        value = float(price) * hours * float(mw)
        value_rows.append((row_id, hours, round(value, 4)))

    sql = """
        INSERT INTO crr_contract_value (id, column1_hours, column2_value, refreshed_at)
        VALUES %s
        ON CONFLICT (id) DO UPDATE
            SET column1_hours = EXCLUDED.column1_hours,
                column2_value = EXCLUDED.column2_value,
                refreshed_at  = NOW()
    """
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, sql, value_rows, page_size=2000)
    conn.commit()
    return len(value_rows)


def mark_loaded(conn, auction_file_id: int):
    with conn.cursor() as cur:
        cur.execute("UPDATE auction_file SET loaded_at = NOW() WHERE id = %s", (auction_file_id,))
    conn.commit()


# ─────────────────────────────────────────────
# HOLIDAY SEEDING
# ─────────────────────────────────────────────

HOLIDAY_NAMES = {
    1:  "New Year's Day",
    5:  "Memorial Day",
    7:  "Independence Day",
    9:  "Labor Day",
    11: "Thanksgiving",
    12: "Christmas",
}

def seed_nerc_holidays(conn, years: list[int]):
    """Populate the nerc_holiday table for the given years."""
    rows = []
    for y in years:
        for d in _nerc_holidays_for_year(y):
            name = HOLIDAY_NAMES.get(d.month, "Holiday")
            rows.append((d, name, "WEPEAK"))
    sql = """
        INSERT INTO nerc_holiday (holiday_date, holiday_name, peak_treatment)
        VALUES %s
        ON CONFLICT (holiday_date) DO NOTHING
    """
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, sql, rows)
    conn.commit()
    log.info(f"  Seeded NERC holidays for years {years}")


# ─────────────────────────────────────────────
# MAIN LOAD PIPELINE
# ─────────────────────────────────────────────

def load_crr_file(conn, crr_path: Path, zone_map: dict, force: bool = False) -> bool:
    """
    Full ETL for a single CRR file (either a .zip or a pre-extracted .csv).
    Returns True if loaded successfully, False if skipped.
    """
    filename = crr_path.name
    log.info(f"\nProcessing: {filename}")

    # Parse metadata from filename
    try:
        meta = parse_filename_meta(filename)
    except ValueError as e:
        log.warning(f"  Skipping — {e}")
        return False

    # Check if already loaded
    with conn.cursor() as cur:
        cur.execute(
            "SELECT id, loaded_at FROM auction_file WHERE source_filename = %s",
            (filename,)
        )
        existing = cur.fetchone()

    if existing and existing[1] and not force:
        log.info(f"  Already loaded (auction_file.id={existing[0]}) — use --force to reload")
        return False

    # Read MarketResults data (zip or csv)
    try:
        df = load_market_results_file(crr_path)
    except Exception as e:
        log.error(f"  Failed to read file: {e}")
        return False

    log.info(f"  {meta['auction_kind']}  {meta['delivery_start']}→{meta['delivery_end']}"
             + (f"  Seq{meta['sequence_num']}" if meta['sequence_num'] else ""))
    log.info(f"  {len(df):,} contract rows")

    # Upsert auction_file record
    af_id = upsert_auction_file(conn, meta, filename)
    log.info(f"  auction_file.id = {af_id}")

    # Insert contracts
    n_inserted = bulk_insert_contracts(conn, df, af_id, zone_map)
    log.info(f"  Inserted {n_inserted:,} new rows (skipped {len(df)-n_inserted:,} dupes)")

    # Calculate Column1 / Column2
    n_values = calculate_and_store_values(conn, af_id)
    log.info(f"  Calculated values for {n_values:,} rows")

    mark_loaded(conn, af_id)
    return True


def run_etl(
    conn,
    download_dir: Path,
    zone_map: dict,
    specific_file: Path = None,
    force: bool = False,
):
    # Seed NERC holidays for a range of years
    seed_nerc_holidays(conn, list(range(2022, 2032)))

    if specific_file:
        crr_files = [specific_file]
    else:
        # Prefer CSVs (output of crr_downloader.py); also pick up legacy ZIPs.
        # De-duplicate: if both rpt.xxx.csv and rpt.xxx.zip exist, use the CSV.
        csv_files = sorted(download_dir.glob("rpt.*.csv"))
        zip_files = sorted(download_dir.glob("rpt.*.zip"))
        csv_stems  = {p.stem for p in csv_files}
        extra_zips = [p for p in zip_files if p.stem not in csv_stems]
        crr_files  = csv_files + extra_zips
        log.info(
            f"Found {len(csv_files)} CSV(s) and {len(extra_zips)} additional ZIP(s)"
            f" in {download_dir}"
        )

    loaded = skipped = failed = 0
    for fp in crr_files:
        try:
            ok = load_crr_file(conn, fp, zone_map, force=force)
            if ok: loaded += 1
            else:  skipped += 1
        except Exception as e:
            log.error(f"  ERROR on {fp.name}: {e}")
            failed += 1

    log.info(f"\nETL complete — loaded: {loaded}  skipped: {skipped}  failed: {failed}")


# ─────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Load ERCOT CRR auction data into PostgreSQL")
    parser.add_argument("--dsn", default=os.getenv("DATABASE_URL"),
                        help="PostgreSQL connection string (or set DATABASE_URL)")
    parser.add_argument("--dir",  default=str(DEFAULT_DOWNLOAD_DIR),
                        help="Directory containing downloaded zip files")
    parser.add_argument("--file", help="Load a specific file (zip or csv) instead of the whole directory")
    parser.add_argument("--load-zone-map", metavar="CSV",
                        help="CSV file mapping nodes to load zones — upserts into node_zone_map table")
    parser.add_argument("--recalc-values", action="store_true",
                        help="Recalculate Column1/Column2 for all existing rows")
    parser.add_argument("--force", action="store_true",
                        help="Re-load files even if already marked as loaded")
    args = parser.parse_args()

    conn = get_conn(args.dsn)

    # Handle zone map load
    if args.load_zone_map:
        log.info(f"Loading zone map from {args.load_zone_map}…")
        zone_df = load_zone_map_from_csv(Path(args.load_zone_map))
        upsert_zone_map(conn, zone_df)

    # Build in-memory zone map from DB
    zone_map = load_zone_map_from_db(conn)
    log.info(f"Zone map: {len(zone_map)} nodes loaded from database")

    if args.recalc_values:
        log.info("Recalculating all Column1/Column2 values…")
        with conn.cursor() as cur:
            cur.execute("SELECT DISTINCT auction_file_id FROM crr_market_result")
            af_ids = [r[0] for r in cur.fetchall()]
        for af_id in af_ids:
            n = calculate_and_store_values(conn, af_id)
            log.info(f"  auction_file_id={af_id}: {n} rows updated")
        return

    # Run ETL
    specific = Path(args.file) if args.file else None
    run_etl(
        conn=conn,
        download_dir=Path(args.dir),
        zone_map=zone_map,
        specific_file=specific,
        force=args.force,
    )
    conn.close()


if __name__ == "__main__":
    main()
