# CRR Database Pipeline — Setup Guide

## Prerequisites
- Python 3.11+
- PostgreSQL 14+ on your VPS (already installed)
- ERCOT API key (free registration at https://apiportal.ercot.com/)

---

## 1. Install Python dependencies
```bash
pip install -r requirements.txt
```

---

## 2. Create the database
```bash
# On your VPS
psql -U postgres -c "CREATE DATABASE crr_db;"
psql -U postgres -d crr_db -f schema.sql
```

---

## 3. Set environment variables
```bash
export DATABASE_URL="postgresql://postgres:yourpassword@your-vps-ip:5432/crr_db"
export ERCOT_API_KEY="your-ercot-subscription-key"
```

---

## 4. Load your node-zone mapping file
You have a CSV that maps individual nodes to load zones. Run:
```bash
python etl_loader.py --load-zone-map /path/to/your_node_zone_map.csv
```

The loader accepts any CSV with columns like:
- `node_name` / `node` / `SettlementPoint` / `BusName`
- `load_zone` / `zone` / `ZoneName`

---

## 5. Download auction files from ERCOT API
```bash
# Download everything covering 2026
python ercot_downloader.py --year 2026

# Or just the annual sequences
python ercot_downloader.py --type annual --year 2026

# Download specific monthly auction
python ercot_downloader.py --type monthly --month 2026-04
```

Files are saved to `./downloads/` and tracked in `.download_state.json`
so re-running never re-downloads what you already have.

---

## 6. Load downloaded files into PostgreSQL
```bash
# Load everything in the downloads folder
python etl_loader.py

# Load a specific file
python etl_loader.py --file downloads/rpt.00011201...APR2026.zip
```

---

## 7. Query the CRR pool

**From psql:**
```sql
-- How big is the LZ_NORTH CRR pot next month?
SELECT * FROM (
  SELECT sink_zone, delivery_month,
         SUM(total_value_usd) AS pool_usd,
         SUM(total_mw)        AS pool_mw
  FROM v_crr_pool_by_zone_month
  WHERE delivery_month = DATE_TRUNC('month', CURRENT_DATE + INTERVAL '1 month')
  GROUP BY 1,2
) x ORDER BY pool_usd DESC;
```

**From Python (crr_analysis.py):**
The existing `crr_analysis.py` still works for one-off local analysis.
The database is for the ongoing automated pipeline.

---

## What data covers "next month"?

As of any date, here's what's available for estimating the next month's CRR pool:

| Auction type | When it runs | Covers |
|---|---|---|
| Annual Seq6 | ~2 yrs before delivery | All 6 months of that annual period |
| Annual Seq5–1 | Every 6 months, closer to delivery | Same 6 months, more refined |
| Monthly | ~3–4 weeks before delivery | Only that specific month |

For **April 2026** (as of March 24, 2026):
- Annual Seq1–6 (2026 1st6): ✅ all available
- April 2026 monthly: ✅ ran ~March 19, 2026, should be downloadable now

For **May 2026** (as of March 24, 2026):
- Annual Seq1–6: ✅ available
- May 2026 monthly: ⏳ runs ~April 17, 2026 — not yet available

**Recommendation:** Download monthly data the day it's published (~3 weeks before the delivery month). Annual data is stable and only needs to be downloaded once per sequence.

---

## Keeping the database current (cron job)

Add to crontab on your VPS:
```cron
# Check for new ERCOT auction files every Monday at 8am
0 8 * * 1 cd /path/to/crr-api/db && python ercot_downloader.py --year $(date +%Y) && python etl_loader.py >> /var/log/crr_etl.log 2>&1
```

---

## File structure
```
crr-api/
  db/
    schema.sql          ← PostgreSQL table/view definitions
    ercot_downloader.py ← Downloads zip files from ERCOT API
    etl_loader.py       ← Parses zips and loads into PostgreSQL
    pool_estimator.sql  ← Ready-to-run queries for CRR pool analysis
    requirements.txt    ← Python dependencies
    downloads/          ← Downloaded zip files land here
```
