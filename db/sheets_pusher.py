"""
CRR Google Sheets Pusher
=========================
Queries the crr PostgreSQL database and pushes CRR pool data
to the designated Google Sheet.

Tabs written:
    CRR Pool      — Zone × delivery-month matrix (values in USD)
    Peak Detail   — Zone × month × peak-type breakdown
    Last Updated  — Run timestamp + summary stats

Environment variables required:
    DATABASE_URL              postgresql://user:pass@host:5432/crr
    GOOGLE_SERVICE_ACCOUNT    path to service_account.json
    CRR_SPREADSHEET_ID        Google Sheet ID

Usage:
    python sheets_pusher.py
    python sheets_pusher.py --months 6   # how many forward months to show (default 12)
    python sheets_pusher.py --dry-run    # print data without writing to sheet
"""

import json
import logging
import os
import sys
import argparse
from datetime import date, timedelta
from pathlib import Path
from typing import Optional

import psycopg2
import psycopg2.extras
from google.oauth2 import service_account
from googleapiclient.discovery import build

logging.basicConfig(
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    level=logging.INFO,
)
log = logging.getLogger(__name__)

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

# ─────────────────────────────────────────────
# DB CONNECTION
# ─────────────────────────────────────────────

def get_conn(dsn: str = None):
    dsn = dsn or os.environ.get("DATABASE_URL")
    if not dsn:
        sys.exit("ERROR: Set DATABASE_URL environment variable.")
    return psycopg2.connect(dsn)


# ─────────────────────────────────────────────
# GOOGLE SHEETS AUTH
# ─────────────────────────────────────────────

def get_sheets_service(service_account_path: str):
    """Build an authenticated Google Sheets API service object."""
    creds = service_account.Credentials.from_service_account_file(
        service_account_path, scopes=SCOPES
    )
    return build("sheets", "v4", credentials=creds, cache_discovery=False)


# ─────────────────────────────────────────────
# DATA QUERIES
# ─────────────────────────────────────────────

def query_pool_by_zone_month(conn, start_month: date, end_month: date) -> list[dict]:
    """
    Pull aggregated CRR pool value by sink_zone + delivery_month.
    Returns list of dicts: {sink_zone, delivery_month, total_mw, total_value_usd}
    """
    sql = """
        SELECT
            sink_zone,
            delivery_month,
            SUM(total_mw)         AS total_mw,
            SUM(total_value_usd)  AS total_value_usd
        FROM v_crr_pool_by_zone_month
        WHERE delivery_month >= %(start)s
          AND delivery_month <= %(end)s
          AND sink_zone IS NOT NULL
        GROUP BY sink_zone, delivery_month
        ORDER BY sink_zone, delivery_month
    """
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(sql, {"start": start_month, "end": end_month})
        return cur.fetchall()


def query_peak_detail(conn, start_month: date, end_month: date) -> list[dict]:
    """
    Pull CRR pool breakdown by sink_zone + delivery_month + time_of_use.
    Returns list of dicts with those columns plus total_mw and total_value_usd.
    """
    sql = """
        SELECT
            sink_zone,
            delivery_month,
            time_of_use,
            SUM(total_mw)        AS total_mw,
            SUM(total_value_usd) AS total_value_usd
        FROM v_crr_pool_by_zone_month
        WHERE delivery_month >= %(start)s
          AND delivery_month <= %(end)s
          AND sink_zone IS NOT NULL
        GROUP BY sink_zone, delivery_month, time_of_use
        ORDER BY sink_zone, delivery_month, time_of_use
    """
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(sql, {"start": start_month, "end": end_month})
        return cur.fetchall()


def query_summary_stats(conn) -> dict:
    """Return basic summary stats for the Last Updated tab."""
    sql = """
        SELECT
            COUNT(*)                              AS contract_count,
            MIN(af.delivery_start)               AS earliest_delivery,
            MAX(af.delivery_end)                 AS latest_delivery,
            MAX(af.loaded_at)                    AS last_loaded
        FROM crr_market_result mr
        JOIN auction_file af ON af.id = mr.auction_file_id
    """
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(sql)
        return dict(cur.fetchone())


# ─────────────────────────────────────────────
# SHEET BUILDING HELPERS
# ─────────────────────────────────────────────

def _month_label(d: date) -> str:
    """e.g. 2026-02-01 → 'Feb 2026'"""
    return d.strftime("%b %Y")


def build_pool_matrix(rows: list[dict], months: list[date]) -> list[list]:
    """
    Build a 2D list for the CRR Pool tab:
        Row 0: header  [Zone, Feb 2026, Mar 2026, ...]
        Row N: [LZ_HOUSTON, $val, $val, ...]
    """
    # Index by (zone, month) → value
    lookup: dict[tuple, float] = {}
    zones = set()
    for r in rows:
        key = (r["sink_zone"], r["delivery_month"])
        lookup[key] = float(r["total_value_usd"] or 0)
        zones.add(r["sink_zone"])

    sorted_zones = sorted(zones)
    header = ["Zone"] + [_month_label(m) for m in months]
    table  = [header]
    for zone in sorted_zones:
        row = [zone]
        for m in months:
            val = lookup.get((zone, m), 0)
            row.append(round(val, 2) if val else 0)
        table.append(row)
    return table


def build_peak_detail_table(rows: list[dict]) -> list[list]:
    """
    Build a flat table for the Peak Detail tab:
        Zone | Month | Peak Type | MW | Value USD
    """
    header = ["Zone", "Month", "Peak Type", "MW", "Value (USD)"]
    table  = [header]
    for r in rows:
        table.append([
            r["sink_zone"],
            _month_label(r["delivery_month"]),
            r["time_of_use"],
            round(float(r["total_mw"] or 0), 2),
            round(float(r["total_value_usd"] or 0), 2),
        ])
    return table


def build_last_updated_table(stats: dict) -> list[list]:
    now = date.today()
    return [
        ["Field", "Value"],
        ["Run date",           str(now)],
        ["Contract count",     stats.get("contract_count", "—")],
        ["Earliest delivery",  str(stats.get("earliest_delivery", "—"))],
        ["Latest delivery",    str(stats.get("latest_delivery", "—"))],
        ["Last ETL load",      str(stats.get("last_loaded", "—"))],
    ]


# ─────────────────────────────────────────────
# SHEETS WRITE HELPERS
# ─────────────────────────────────────────────

def clear_and_write(service, spreadsheet_id: str, tab_name: str, data: list[list]):
    """Clear a tab and write new data to it (creates the tab if it doesn't exist)."""
    sheets = service.spreadsheets()

    # Ensure tab exists
    meta = sheets.get(spreadsheetId=spreadsheet_id).execute()
    existing_titles = {s["properties"]["title"] for s in meta["sheets"]}
    if tab_name not in existing_titles:
        sheets.batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={"requests": [{"addSheet": {"properties": {"title": tab_name}}}]},
        ).execute()
        log.info(f"  Created tab: {tab_name}")

    range_name = f"'{tab_name}'!A1"

    # Clear existing content
    sheets.values().clear(
        spreadsheetId=spreadsheet_id,
        range=range_name,
    ).execute()

    # Write new data
    if data:
        sheets.values().update(
            spreadsheetId=spreadsheet_id,
            range=range_name,
            valueInputOption="USER_ENTERED",
            body={"values": data},
        ).execute()

    log.info(f"  Wrote {len(data)} rows to tab '{tab_name}'")


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────

def run(
    spreadsheet_id: str,
    service_account_path: str,
    dsn: str,
    num_months: int = 12,
    dry_run: bool = False,
):
    conn = get_conn(dsn)

    # Date window: current month through (num_months - 1) forward months
    today      = date.today()
    start_month = today.replace(day=1)
    months = []
    m = start_month
    for _ in range(num_months):
        months.append(m)
        # Advance to next month
        if m.month == 12:
            m = date(m.year + 1, 1, 1)
        else:
            m = date(m.year, m.month + 1, 1)
    end_month = months[-1]

    log.info(f"Querying data for {_month_label(start_month)} → {_month_label(end_month)}")

    pool_rows   = query_pool_by_zone_month(conn, start_month, end_month)
    detail_rows = query_peak_detail(conn, start_month, end_month)
    stats       = query_summary_stats(conn)
    conn.close()

    log.info(f"  Pool rows: {len(pool_rows)}, Detail rows: {len(detail_rows)}")

    pool_table   = build_pool_matrix(pool_rows, months)
    detail_table = build_peak_detail_table(detail_rows)
    meta_table   = build_last_updated_table(stats)

    if dry_run:
        log.info("\n[dry-run] CRR Pool matrix (first 5 rows):")
        for row in pool_table[:5]:
            log.info(f"  {row}")
        log.info("\n[dry-run] Peak Detail (first 5 rows):")
        for row in detail_table[:5]:
            log.info(f"  {row}")
        log.info("\n[dry-run] Last Updated:")
        for row in meta_table:
            log.info(f"  {row}")
        return

    log.info(f"Connecting to Google Sheets (spreadsheet_id={spreadsheet_id})")
    service = get_sheets_service(service_account_path)

    clear_and_write(service, spreadsheet_id, "CRR Pool",     pool_table)
    clear_and_write(service, spreadsheet_id, "Peak Detail",  detail_table)
    clear_and_write(service, spreadsheet_id, "Last Updated", meta_table)

    log.info("\nSheets push complete.")


def main():
    parser = argparse.ArgumentParser(description="Push CRR pool data to Google Sheets")
    parser.add_argument("--spreadsheet-id",
                        default=os.environ.get("CRR_SPREADSHEET_ID"),
                        help="Google Sheet ID (or set CRR_SPREADSHEET_ID)")
    parser.add_argument("--service-account",
                        default=os.environ.get("GOOGLE_SERVICE_ACCOUNT"),
                        help="Path to service_account.json (or set GOOGLE_SERVICE_ACCOUNT)")
    parser.add_argument("--dsn",
                        default=os.environ.get("DATABASE_URL"),
                        help="PostgreSQL connection string (or set DATABASE_URL)")
    parser.add_argument("--months", type=int, default=12,
                        help="Number of forward months to include (default: 12)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print data without writing to Google Sheets")
    args = parser.parse_args()

    if not args.spreadsheet_id:
        sys.exit("ERROR: Provide --spreadsheet-id or set CRR_SPREADSHEET_ID")
    if not args.service_account:
        sys.exit("ERROR: Provide --service-account or set GOOGLE_SERVICE_ACCOUNT")

    run(
        spreadsheet_id     = args.spreadsheet_id,
        service_account_path = args.service_account,
        dsn                = args.dsn,
        num_months         = args.months,
        dry_run            = args.dry_run,
    )


if __name__ == "__main__":
    main()
