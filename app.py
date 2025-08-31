from fastapi import FastAPI, Body, HTTPException
from datetime import datetime, timezone, date, timedelta
import duckdb
import pandas as pd
import os
from typing import Optional

DB_PATH = "ads_spend.duckdb"   # file created in working dir
TABLE_NAME = "ads_spend_raw"

app = FastAPI(title="Ads Ingest & Metrics Service")

# Ensure DB file & table exist
con = duckdb.connect(DB_PATH)
con.execute(f"""
CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
  date DATE,
  platform TEXT,
  account TEXT,
  campaign TEXT,
  country TEXT,
  device TEXT,
  spend DOUBLE,
  clicks BIGINT,
  impressions BIGINT,
  conversions BIGINT,
  load_date TIMESTAMP,
  source_file_name TEXT
);
""")
con.commit()

@app.get("/health")
def health():
    return {"status": "ok"}

@app.get("/count")
def count_rows():
    r = con.execute(f"SELECT COUNT(*) FROM {TABLE_NAME}").fetchone()
    return {"row_count": int(r[0])}

@app.post("/ingest")
def ingest(payload: dict = Body(...)):
    """
    POST JSON:
    {
      "url": "<direct-download-csv-url>",
      "source_file_name": "ads_spend.csv"
    }
    """
    url = payload.get("url")
    source_file_name = payload.get("source_file_name", "unknown.csv")

    if not url:
        raise HTTPException(status_code=400, detail="Missing 'url' in payload")

    # read CSV into pandas
    try:
        df = pd.read_csv(url)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Error reading CSV from url: {e}")

    # normalize column names
    df.columns = [c.strip().lower() for c in df.columns]

    # required columns check (basic)
    required = {"date","platform","account","campaign","country","device","spend","clicks","impressions","conversions"}
    if not required.issubset(set(df.columns)):
        missing = required - set(df.columns)
        raise HTTPException(status_code=400, detail=f"Missing required columns: {missing}")

    # cast types & parse dates
    df['date'] = pd.to_datetime(df['date'], errors='coerce').dt.date
    for col in ['spend','clicks','impressions','conversions']:
        df[col] = pd.to_numeric(df[col], errors='coerce')

    load_dt = datetime.now(timezone.utc)
    df['load_date'] = load_dt
    df['source_file_name'] = source_file_name

    # Replace NaN conversions/spend with 0 to avoid DB errors (you can change)
    df = df.where(pd.notnull(df), None)

    # Write to DuckDB (append)
    try:
        con.register("temp_in", df)
        con.execute(f"BEGIN")
        # use column order to match table
        con.execute(f"""
            INSERT INTO {TABLE_NAME}
            SELECT
              temp_in.date::DATE,
              temp_in.platform,
              temp_in.account,
              temp_in.campaign,
              temp_in.country,
              temp_in.device,
              temp_in.spend::DOUBLE,
              temp_in.clicks::BIGINT,
              temp_in.impressions::BIGINT,
              temp_in.conversions::BIGINT,
              temp_in.load_date::TIMESTAMP,
              temp_in.source_file_name
            FROM temp_in
        """)
        con.execute("COMMIT")
        con.unregister("temp_in")
    except Exception as e:
        con.execute("ROLLBACK")
        raise HTTPException(status_code=500, detail=f"DB write error: {e}")

    inserted = len(df)
    total = con.execute(f"SELECT COUNT(*) FROM {TABLE_NAME}").fetchone()[0]
    return {"inserted_rows": int(inserted), "total_rows": int(total), "load_date": load_dt.isoformat()}

# KPI helper endpoint: accepts arbitrary start/end and returns CAC & ROAS aggregated
@app.get("/metrics")
def metrics(start: Optional[str] = None, end: Optional[str] = None):
    """
    Query example:
    /metrics?start=2025-07-01&end=2025-07-30
    Returns JSON with CAC and ROAS aggregated across the date range
    """
    # default last 30 days
    today = date.today()
    if end:
        end_date = pd.to_datetime(end).date()
    else:
        end_date = today

    if start:
        start_date = pd.to_datetime(start).date()
    else:
        start_date = end_date - timedelta(days=29)

    # SQL to get sums
    q = f"""
    SELECT
      SUM(spend) as spend,
      SUM(conversions) as conversions,
      SUM(conversions)*100.0 as revenue
    FROM {TABLE_NAME}
    WHERE date BETWEEN DATE '{start_date}' AND DATE '{end_date}';
    """
    try:
        res = con.execute(q).fetchdf()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Query error: {e}")

    if res.empty:
        return {"start": str(start_date), "end": str(end_date), "CAC": None, "ROAS": None, "spend": 0, "conversions": 0}

    spend = float(res.at[0,'spend']) if res.at[0,'spend'] is not None else 0.0
    conversions = int(res.at[0,'conversions']) if res.at[0,'conversions'] is not None else 0
    revenue = float(res.at[0,'revenue']) if res.at[0,'revenue'] is not None else 0.0

    CAC = (spend / conversions) if conversions>0 else None
    ROAS = (revenue / spend) if spend>0 else None

    return {"start": str(start_date), "end": str(end_date), "spend": spend, "conversions": conversions, "revenue": revenue, "CAC": CAC, "ROAS": ROAS}
