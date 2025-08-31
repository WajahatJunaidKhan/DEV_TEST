-- kpi_query.sql (DuckDB)
-- Replace :end_date with the last date (YYYY-MM-DD). This script computes last 30 vs prior 30.

-- Example usage: run in DuckDB REPL or via duckdb python

WITH params AS (
  SELECT DATE(current_date) AS end_date
),
periods AS (
  SELECT
    (end_date - 29) AS last_30_start,
    end_date AS last_30_end,
    (end_date - 59) AS prior_30_start,
    (end_date - 30) AS prior_30_end
  FROM params
),
agg AS (
  SELECT
    'last_30' AS period,
    SUM(spend) AS spend,
    SUM(conversions) AS conversions
  FROM ads_spend_raw, periods
  WHERE date BETWEEN periods.last_30_start AND periods.last_30_end

  UNION ALL

  SELECT
    'prior_30' AS period,
    SUM(spend) AS spend,
    SUM(conversions) AS conversions
  FROM ads_spend_raw, periods
  WHERE date BETWEEN periods.prior_30_start AND periods.prior_30_end
)
SELECT
  'CAC' AS metric,
  MAX(CASE WHEN period='last_30' THEN spend/NULLIF(conversions,0) END) AS last_30_value,
  MAX(CASE WHEN period='prior_30' THEN spend/NULLIF(conversions,0) END) AS prior_30_value,
  (MAX(CASE WHEN period='last_30' THEN spend/NULLIF(conversions,0) END) - MAX(CASE WHEN period='prior_30' THEN spend/NULLIF(conversions,0) END))
    / NULLIF(MAX(CASE WHEN period='prior_30' THEN spend/NULLIF(conversions,0) END),0) * 100 AS pct_change
FROM agg

UNION ALL

SELECT
  'ROAS' AS metric,
  MAX(CASE WHEN period='last_30' THEN (conversions*100.0)/NULLIF(spend,0) END) AS last_30_value,
  MAX(CASE WHEN period='prior_30' THEN (conversions*100.0)/NULLIF(spend,0) END) AS prior_30_value,
  (MAX(CASE WHEN period='last_30' THEN (conversions*100.0)/NULLIF(spend,0) END) - MAX(CASE WHEN period='prior_30' THEN (conversions*100.0)/NULLIF(spend,0) END))
    / NULLIF(MAX(CASE WHEN period='prior_30' THEN (conversions*100.0)/NULLIF(spend,0) END),0) * 100 AS pct_change
FROM agg;
