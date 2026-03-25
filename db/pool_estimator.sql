-- ============================================================
-- CRR Pool Estimator Queries
-- ============================================================
-- All queries parameterized by :target_month (first day of the month,
-- e.g. '2026-04-01').  In Python/psycopg2, replace :target_month with %s.
-- ============================================================


-- ────────────────────────────────────────────────────────────
-- Q1.  TOTAL CRR POOL FOR A LOAD ZONE IN A GIVEN MONTH
--      Sums Column2 (total contract value) for all contracts
--      with sink_zone = target zone that cover the target month.
-- ────────────────────────────────────────────────────────────
SELECT
    r.sink_zone,
    r.time_of_use,
    af.auction_kind,
    COUNT(*)                    AS contract_count,
    SUM(r.mw)                   AS total_mw,
    SUM(v.column2_value)        AS total_value_usd,
    SUM(v.column2_value)
        / NULLIF(SUM(r.mw), 0) AS avg_value_per_mw_usd
FROM crr_market_result r
JOIN crr_contract_value v   ON v.id = r.id
JOIN auction_file       af  ON af.id = r.auction_file_id
WHERE r.sink_zone   = 'LZ_NORTH'             -- ← change zone here
  AND r.start_date  <= (DATE_TRUNC('month', :target_month::DATE)
                        + INTERVAL '1 month - 1 day')::DATE
  AND r.end_date    >= DATE_TRUNC('month', :target_month::DATE)::DATE
GROUP BY 1, 2, 3
ORDER BY 3 DESC, 2;


-- ────────────────────────────────────────────────────────────
-- Q2.  POOL TOTAL (single number — what you'd show a customer)
-- ────────────────────────────────────────────────────────────
SELECT
    SUM(v.column2_value)    AS total_crr_pool_usd,
    SUM(r.mw)               AS total_mw,
    COUNT(*)                AS total_contracts
FROM crr_market_result r
JOIN crr_contract_value v ON v.id = r.id
WHERE r.sink_zone  = 'LZ_NORTH'
  AND r.start_date <= (DATE_TRUNC('month', :target_month::DATE)
                       + INTERVAL '1 month - 1 day')::DATE
  AND r.end_date   >= DATE_TRUNC('month', :target_month::DATE)::DATE;


-- ────────────────────────────────────────────────────────────
-- Q3.  ESTIMATED CUSTOMER SHARE
--      Replace 150.0 with customer's avg load (MW) and
--      12500.0 with total LZ_NORTH avg load (from ERCOT load data).
-- ────────────────────────────────────────────────────────────
WITH pool AS (
    SELECT SUM(v.column2_value) AS total_pool_usd,
           SUM(r.mw)            AS total_pool_mw
    FROM crr_market_result r
    JOIN crr_contract_value v ON v.id = r.id
    WHERE r.sink_zone  = 'LZ_NORTH'
      AND r.start_date <= (DATE_TRUNC('month', :target_month::DATE)
                           + INTERVAL '1 month - 1 day')::DATE
      AND r.end_date   >= DATE_TRUNC('month', :target_month::DATE)::DATE
)
SELECT
    150.0                                           AS customer_avg_mw,
    12500.0                                         AS zone_total_mw,
    ROUND(150.0 / 12500.0, 6)                       AS load_proportion,
    pool.total_pool_usd                             AS zone_crr_pool_usd,
    ROUND(150.0 / 12500.0 * pool.total_pool_usd, 2) AS customer_est_crr_usd,
    pool.total_pool_mw                              AS zone_crr_mw
FROM pool;


-- ────────────────────────────────────────────────────────────
-- Q4.  POOL BY ZONE — ALL LOAD ZONES FOR A MONTH
-- ────────────────────────────────────────────────────────────
SELECT
    r.sink_zone,
    COUNT(DISTINCT r.crr_id)    AS unique_contracts,
    SUM(r.mw)                   AS total_mw,
    SUM(v.column2_value)        AS total_value_usd
FROM crr_market_result r
JOIN crr_contract_value v ON v.id = r.id
WHERE r.sink_zone IS NOT NULL
  AND r.start_date <= (DATE_TRUNC('month', :target_month::DATE)
                       + INTERVAL '1 month - 1 day')::DATE
  AND r.end_date   >= DATE_TRUNC('month', :target_month::DATE)::DATE
GROUP BY r.sink_zone
ORDER BY total_value_usd DESC;


-- ────────────────────────────────────────────────────────────
-- Q5.  POOL TREND — MONTH BY MONTH FOR A ZONE
--      Shows how the pool value changes over time.
-- ────────────────────────────────────────────────────────────
SELECT
    delivery_month,
    auction_kind,
    time_of_use,
    contract_count,
    ROUND(total_mw, 1)          AS total_mw,
    ROUND(total_value_usd, 2)   AS total_value_usd
FROM v_crr_pool_by_zone_month
WHERE sink_zone = 'LZ_NORTH'
ORDER BY delivery_month, auction_kind, time_of_use;


-- ────────────────────────────────────────────────────────────
-- Q6.  WHAT DATA DO WE HAVE? (coverage check)
--      Shows which auction files are loaded and what months they cover.
-- ────────────────────────────────────────────────────────────
SELECT
    af.auction_kind,
    af.annual_period_label,
    af.sequence_num,
    af.auction_run_date,
    af.delivery_start,
    af.delivery_end,
    COUNT(r.id)     AS contract_count,
    af.loaded_at
FROM auction_file af
LEFT JOIN crr_market_result r ON r.auction_file_id = af.id
GROUP BY af.id
ORDER BY af.delivery_start, af.auction_kind, af.sequence_num;


-- ────────────────────────────────────────────────────────────
-- Q7.  NODES MISSING FROM ZONE MAP
--      Run this to find which Source/Sink nodes still need mapping.
-- ────────────────────────────────────────────────────────────
SELECT DISTINCT
    nodes.node_name,
    COUNT(*) OVER (PARTITION BY nodes.node_name) AS appearances
FROM (
    SELECT source_node AS node_name FROM crr_market_result WHERE source_zone IS NULL
    UNION ALL
    SELECT sink_node   AS node_name FROM crr_market_result WHERE sink_zone IS NULL
) nodes
LEFT JOIN node_zone_map nzm ON nzm.node_name = nodes.node_name
WHERE nzm.node_name IS NULL
ORDER BY appearances DESC
LIMIT 50;


-- ────────────────────────────────────────────────────────────
-- Q8.  NEXT-MONTH ESTIMATE (runs from the pre-use view)
--      For "what is the estimated CRR pot for next month?"
-- ────────────────────────────────────────────────────────────
WITH next_month AS (
    SELECT DATE_TRUNC('month', CURRENT_DATE + INTERVAL '1 month')::DATE AS m
)
SELECT
    p.sink_zone,
    next_month.m                    AS target_month,
    SUM(p.total_value_usd)          AS estimated_pool_usd,
    SUM(p.total_mw)                 AS total_mw,
    SUM(p.contract_count)           AS contracts,
    STRING_AGG(DISTINCT
        p.auction_kind::TEXT, ', '
        ORDER BY p.auction_kind::TEXT) AS data_sources
FROM v_crr_pool_by_zone_month p
CROSS JOIN next_month
WHERE p.delivery_month = next_month.m
GROUP BY p.sink_zone, next_month.m
ORDER BY estimated_pool_usd DESC;
