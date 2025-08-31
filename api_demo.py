
---

## 2. `api_demo.py` (FastAPI)

```python
from fastapi import FastAPI, Query
import duckdb
from datetime import date

app = FastAPI(title="Ads Metrics API")

@app.get("/metrics")
def get_metrics(
    start: date = Query(..., description="Start date in YYYY-MM-DD"),
    end: date = Query(..., description="End date in YYYY-MM-DD")
):
    con = duckdb.connect("database.db")

    query = f"""
    WITH spend_rev AS (
        SELECT
            SUM(spend) AS total_spend,
            SUM(revenue) AS total_revenue,
            SUM(conversions) AS total_conversions
        FROM staging_ads
        WHERE date BETWEEN '{start}' AND '{end}'
    )
    SELECT
        total_spend,
        total_revenue,
        total_conversions,
        CASE WHEN total_conversions > 0 THEN total_spend::DOUBLE / total_conversions ELSE NULL END AS cac,
        CASE WHEN total_spend > 0 THEN total_revenue::DOUBLE / total_spend ELSE NULL END AS roas
    FROM spend_rev;
    """

    result = con.execute(query).fetchone()
    con.close()

    return {
        "start": str(start),
        "end": str(end),
        "spend": result[0],
        "revenue": result[1],
        "conversions": result[2],
        "cac": result[3],
        "roas": result[4]
    }
