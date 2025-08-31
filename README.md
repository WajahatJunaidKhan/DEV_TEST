# DEV_TEST


Part 4 – Agent Demo (Bonus)

We can support natural-language questions by defining simple mappings to our SQL queries.  

"Compare CAC and ROAS for last 30 days vs prior 30 days."

Mapped SQL query:
```sql
WITH last_30 AS (
  SELECT 
    SUM(spend) AS spend,
    SUM(conversions) AS conversions
  FROM ads_spend
  WHERE date BETWEEN DATE('2025-08-01') AND DATE('2025-08-30')
),
prior_30 AS (
  SELECT 
    SUM(spend) AS spend,
    SUM(conversions) AS conversions
  FROM ads_spend
  WHERE date BETWEEN DATE('2025-07-02') AND DATE('2025-07-31')
)
SELECT 
  l.spend / NULLIF(l.conversions,0) AS cac_last_30,
  p.spend / NULLIF(p.conversions,0) AS cac_prior_30,
  (l.spend / NULLIF(l.conversions,0) - p.spend / NULLIF(p.conversions,0)) / NULLIF((p.spend / NULLIF(p.conversions,0)),0) * 100 AS cac_delta_pct,

  (l.conversions * 100.0 / NULLIF(l.spend,0)) AS roas_last_30,
  (p.conversions * 100.0 / NULLIF(p.spend,0)) AS roas_prior_30,
  ((l.conversions * 100.0 / NULLIF(l.spend,0)) - (p.conversions * 100.0 / NULLIF(p.spend,0))) / NULLIF((p.conversions * 100.0 / NULLIF(p.spend,0)),0) * 100 AS roas_delta_pct
FROM last_30 l, prior_30 p;






# Agent Demo – Natural Language to SQL/Metric Mapping

This file shows how natural-language user questions map to backend metrics.

---

### Example 1
**User:** "What was our CAC in July?"  
**SQL Run:**
```sql
SELECT (SUM(spend)::DOUBLE / SUM(conversions)) AS cac
FROM staging_ads
WHERE date BETWEEN '2025-07-01' AND '2025-07-31';


-Create Virtual Environment
python -m venv venv
source venv/bin/activate   # on Mac/Linux
venv\Scripts\activate      # on Windows


-Install Dependencies

-Run DuckDB transformations
duckdb :memory: -c "INSTALL httpfs; LOAD httpfs;"
duckdb database.db -c ".read sql/staging_ads.sql"
duckdb database.db -c ".read sql/metrics_cac.sql"
duckdb database.db -c ".read sql/metrics_roas.sql"



-Run API
uvicorn api_demo:app --reload
