-- ============================================================
-- CRR Database Schema
-- PostgreSQL — run once to initialize
-- ============================================================

-- ── Extensions ───────────────────────────────────────────────
CREATE EXTENSION IF NOT EXISTS pg_trgm;   -- fast text search on node names

-- ── Enumerations ─────────────────────────────────────────────
DO $$ BEGIN
  CREATE TYPE hedge_type_enum  AS ENUM ('OBL', 'OPT');
  CREATE TYPE bid_type_enum    AS ENUM ('BUY', 'SELL');
  CREATE TYPE crr_type_enum    AS ENUM ('PREAWARD', 'STANDARD');
  CREATE TYPE tou_enum         AS ENUM ('PeakWD', 'PeakWE', 'Off-peak');
  CREATE TYPE auction_kind     AS ENUM ('MONTHLY', 'ANNUAL');
EXCEPTION WHEN duplicate_object THEN NULL; END $$;


-- ============================================================
-- 1. AUCTION METADATA
--    One row per zip file downloaded from ERCOT
-- ============================================================
CREATE TABLE IF NOT EXISTS auction_file (
    id                   SERIAL PRIMARY KEY,
    ercot_doc_id         VARCHAR(50) UNIQUE,          -- ERCOT API docId
    report_type_id       INTEGER NOT NULL,             -- 11201=monthly, 11203=annual
    auction_kind         auction_kind NOT NULL,
    auction_name         VARCHAR(200) NOT NULL,        -- e.g. FEB2026MonthlyCRRAuction
    auction_run_date     DATE,                         -- date the auction was run
    delivery_start       DATE NOT NULL,                -- first delivery month covered
    delivery_end         DATE NOT NULL,                -- last delivery month covered
    annual_period_label  VARCHAR(30),                  -- e.g. '2026.1st6', null for monthly
    sequence_num         SMALLINT,                     -- 1-6 for annual, null for monthly
    source_filename      VARCHAR(300) NOT NULL,
    downloaded_at        TIMESTAMP DEFAULT NOW(),
    loaded_at            TIMESTAMP                     -- set after ETL completes
);
CREATE INDEX IF NOT EXISTS idx_auction_file_delivery
    ON auction_file (delivery_start, delivery_end);


-- ============================================================
-- 2. NODE → LOAD ZONE MAPPING
--    Loaded from your ERCOT-provided mapping file.
--    Nodes that ARE load zones (LZ_*) or hubs (HB_*) map to themselves.
-- ============================================================
CREATE TABLE IF NOT EXISTS node_zone_map (
    node_name            VARCHAR(80) PRIMARY KEY,
    load_zone            VARCHAR(20),    -- LZ_NORTH, LZ_HOUSTON, etc. (null if not in a zone)
    is_load_zone         BOOLEAN GENERATED ALWAYS AS (node_name LIKE 'LZ\_%' ESCAPE '\') STORED,
    is_hub               BOOLEAN GENERATED ALWAYS AS (node_name LIKE 'HB\_%' ESCAPE '\') STORED,
    weather_zone         VARCHAR(20),    -- NORTH, SOUTH, HOUSTON, WEST (optional)
    effective_date       DATE DEFAULT '2000-01-01',
    end_date             DATE DEFAULT '9999-12-31',
    source               VARCHAR(100)   -- which mapping file this came from
);

-- Pre-populate LZ and HB nodes that map to themselves
INSERT INTO node_zone_map (node_name, load_zone, weather_zone, source) VALUES
    ('LZ_NORTH',   'LZ_NORTH',   'NORTH',   'self'),
    ('LZ_HOUSTON', 'LZ_HOUSTON', 'HOUSTON', 'self'),
    ('LZ_SOUTH',   'LZ_SOUTH',   'SOUTH',   'self'),
    ('LZ_WEST',    'LZ_WEST',    'WEST',    'self'),
    ('LZ_LCRA',    'LZ_LCRA',    NULL,      'self'),
    ('LZ_RAYBN',   'LZ_RAYBN',   NULL,      'self'),
    ('LZ_AEN',     'LZ_AEN',     NULL,      'self'),
    ('LZ_CPS',     'LZ_CPS',     NULL,      'self'),
    ('HB_NORTH',   'LZ_NORTH',   'NORTH',   'self'),   -- HB_NORTH proxies for LZ_NORTH
    ('HB_HOUSTON', 'LZ_HOUSTON', 'HOUSTON', 'self'),
    ('HB_SOUTH',   'LZ_SOUTH',   'SOUTH',   'self'),
    ('HB_WEST',    'LZ_WEST',    'WEST',    'self'),
    ('HB_PAN',     NULL,         NULL,      'self'),
    ('HB_BUSAVG',  NULL,         NULL,      'self'),
    ('HB_HUBAVG',  NULL,         NULL,      'self')
ON CONFLICT (node_name) DO NOTHING;


-- ============================================================
-- 3. NERC HOLIDAYS
--    Used for accurate peak hour calculation.
--    Populate via etl_loader.py or manually.
-- ============================================================
CREATE TABLE IF NOT EXISTS nerc_holiday (
    holiday_date   DATE PRIMARY KEY,
    holiday_name   VARCHAR(60) NOT NULL,
    peak_treatment VARCHAR(10) DEFAULT 'WEPEAK'  -- how ERCOT classifies peak hrs on this day
);


-- ============================================================
-- 4. CRR MARKET RESULTS  (core fact table)
--    One row per cleared contract / time-of-use combination.
-- ============================================================
CREATE TABLE IF NOT EXISTS crr_market_result (
    id                   BIGSERIAL PRIMARY KEY,
    auction_file_id      INTEGER NOT NULL REFERENCES auction_file(id),

    -- From MarketResults CSV ─────────────────────────────────
    crr_id               BIGINT NOT NULL,
    original_crr_id      BIGINT,
    account_holder       VARCHAR(30),
    hedge_type           hedge_type_enum,
    bid_type             bid_type_enum,
    crr_type             crr_type_enum,
    source_node          VARCHAR(80) NOT NULL,
    sink_node            VARCHAR(80) NOT NULL,
    start_date           DATE NOT NULL,
    end_date             DATE NOT NULL,
    time_of_use          tou_enum NOT NULL,
    bid_24hour           BOOLEAN,
    mw                   NUMERIC(14, 4) NOT NULL,
    shadow_price_mwh     NUMERIC(14, 6) NOT NULL,

    -- Derived / computed ─────────────────────────────────────
    source_zone          VARCHAR(20),   -- from node_zone_map
    sink_zone            VARCHAR(20),   -- from node_zone_map
    contract_period_mths SMALLINT       -- 1-6 (number of months spanned)
        GENERATED ALWAYS AS (
            (DATE_PART('year',  end_date) - DATE_PART('year',  start_date)) * 12 +
            (DATE_PART('month', end_date) - DATE_PART('month', start_date)) + 1
        ) STORED,

    CONSTRAINT uq_crr_per_auction UNIQUE (crr_id, time_of_use, auction_file_id)
);

-- Indexes for common query patterns
CREATE INDEX IF NOT EXISTS idx_cmr_dates        ON crr_market_result (start_date, end_date);
CREATE INDEX IF NOT EXISTS idx_cmr_sink_zone    ON crr_market_result (sink_zone, start_date, end_date);
CREATE INDEX IF NOT EXISTS idx_cmr_source_zone  ON crr_market_result (source_zone, start_date, end_date);
CREATE INDEX IF NOT EXISTS idx_cmr_crr_id       ON crr_market_result (crr_id);
CREATE INDEX IF NOT EXISTS idx_cmr_account      ON crr_market_result (account_holder);


-- ============================================================
-- 5. CRR POOL ESTIMATE  (materialized view, refreshed after each load)
--    Pre-calculates Column1 hours and Column2 value for every
--    contract × calendar_month combination.
--
--    NERC holidays shift WDPEAK days to WEPEAK in the hours calc.
--    The simple version below uses calendar weekday math;
--    the etl_loader.py does the more precise version with holidays.
-- ============================================================
CREATE TABLE IF NOT EXISTS crr_contract_value (
    id                   BIGINT PRIMARY KEY REFERENCES crr_market_result(id),
    column1_hours        INTEGER NOT NULL,   -- peak hours over full contract period
    column2_value        NUMERIC(18, 4),     -- shadow_price_mwh * column1_hours * mw
    refreshed_at         TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_ccv_id ON crr_contract_value (id);


-- ============================================================
-- 6. POOL SUMMARY VIEW
--    The main query surface: total CRR pool by zone + month.
-- ============================================================
CREATE OR REPLACE VIEW v_crr_pool_by_zone_month AS
SELECT
    r.sink_zone,
    DATE_TRUNC('month', gs.delivery_month)::DATE  AS delivery_month,
    r.time_of_use,
    af.auction_kind,
    COUNT(*)                                       AS contract_count,
    SUM(r.mw)                                      AS total_mw,
    SUM(v.column2_value)                           AS total_value_usd,
    SUM(v.column2_value) / NULLIF(SUM(r.mw), 0)   AS avg_value_per_mw
FROM crr_market_result r
JOIN crr_contract_value  v  ON v.id = r.id
JOIN auction_file        af ON af.id = r.auction_file_id
-- Expand each multi-month contract into one row per delivery month it covers
JOIN LATERAL (
    SELECT generate_series(
        DATE_TRUNC('month', r.start_date)::DATE,
        DATE_TRUNC('month', r.end_date)::DATE,
        '1 month'::INTERVAL
    )::DATE AS delivery_month
) gs ON TRUE
WHERE r.sink_zone IS NOT NULL
GROUP BY 1, 2, 3, 4;


-- ============================================================
-- 7. CUSTOMER PROPORTION TABLE  (optional: store customer loads)
-- ============================================================
CREATE TABLE IF NOT EXISTS customer_load (
    id               SERIAL PRIMARY KEY,
    customer_name    VARCHAR(100) NOT NULL,
    load_zone        VARCHAR(20)  NOT NULL,
    period_month     DATE NOT NULL,                -- first day of the month
    avg_load_mw      NUMERIC(12, 4) NOT NULL,
    source           VARCHAR(100),
    loaded_at        TIMESTAMP DEFAULT NOW(),
    UNIQUE (customer_name, load_zone, period_month)
);

-- Zone-level total load (denominator for proportion calc)
CREATE TABLE IF NOT EXISTS zone_total_load (
    load_zone        VARCHAR(20) NOT NULL,
    period_month     DATE NOT NULL,
    avg_load_mw      NUMERIC(12, 4) NOT NULL,
    source           VARCHAR(100),
    loaded_at        TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (load_zone, period_month)
);


-- ============================================================
-- 8. CUSTOMER CRR ESTIMATE VIEW
-- ============================================================
CREATE OR REPLACE VIEW v_customer_crr_estimate AS
SELECT
    cl.customer_name,
    cl.load_zone,
    cl.period_month,
    cl.avg_load_mw                                             AS customer_mw,
    ztl.avg_load_mw                                            AS zone_total_mw,
    ROUND(cl.avg_load_mw / NULLIF(ztl.avg_load_mw, 0), 6)     AS load_proportion,
    p.total_value_usd                                          AS zone_crr_pool_usd,
    ROUND(cl.avg_load_mw
        / NULLIF(ztl.avg_load_mw, 0)
        * p.total_value_usd, 2)                                AS estimated_customer_crr_usd,
    p.contract_count,
    p.total_mw                                                 AS zone_crr_mw,
    p.auction_kind
FROM customer_load cl
JOIN zone_total_load ztl
    ON ztl.load_zone = cl.load_zone
   AND ztl.period_month = cl.period_month
JOIN (
    SELECT sink_zone, delivery_month, auction_kind,
           SUM(total_value_usd) AS total_value_usd,
           SUM(contract_count)  AS contract_count,
           SUM(total_mw)        AS total_mw
    FROM v_crr_pool_by_zone_month
    GROUP BY sink_zone, delivery_month, auction_kind
) p
    ON p.sink_zone = cl.load_zone
   AND p.delivery_month = cl.period_month;
