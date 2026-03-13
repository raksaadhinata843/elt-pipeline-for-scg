import os
import io
import logging
import pandas as pd
import awswrangler as wr
import requests
from datetime import datetime, timedelta, timezone

logger = logging.getLogger()
logger.setLevel(logging.INFO)

EIA_URL = "https://api.eia.gov/v2/petroleum/pri/spt/data/"


def fetch_oil_brent(api_key, frequency, start_date, end_date, offset=0, length=5000):
    params = {
        "api_key":            api_key,
        "frequency":          frequency,
        "data[0]":            "value",
        "facets[series][]":   "EPCBRENT",
        "sort[0][column]":    "period",
        "sort[0][direction]": "asc",
        "offset":             offset,
        "length":             length,
    }

    resp = requests.get(EIA_URL, params=params, timeout=30)
    resp.raise_for_status()

    payload = resp.json()
    data    = payload["response"]["data"]
    total   = int(payload["response"]["total"])
    
    logger.info(f"Fetched {len(data)} records (offset={offset}, total={total})")

    if offset + length < total:
        data += fetch_eia(api_key, frequency, start_date, end_date, offset + length, length)

    return data


def lambda_handler(event, context):
    api_key   = os.environ["EIA_API_KEY"]
    bucket    = os.environ["S3_BUCKET"]
    prefix    = os.environ.get("S3_PREFIX", "bronze/energy/brent")
    frequency = os.environ.get("EIA_FREQUENCY", "daily")
    full_load = os.environ.get("FULL_LOAD", True)

    now = datetime.now(timezone.utc)
    raw = fetch_oil_brent(api_key, frequency, "2010-01-01", now.strftime("%Y-%m-%d"))
    if not raw:
        print("API returned empty response")
        return {"statusCode": 200, "body": "No data returned from API"}


    df = pd.DataFrame(raw[0]["observations"])
    df["value"] = pd.to_numeric(df["value"], errors='coerce')
    df["date"] = pd.to_datetime(df["date"])

    logger.info(f"Starting ingestion | full_load={full_load}")

    timestamp = now.strftime("%Y%m%dT%H%M%SZ")

    s3_key = (
        f"s3://{bucket}/{prefix}/"
        f"year={now.year}/month={now.month:02d}/day={now.day:02d}/"
        f"{timestamp}.parquet"
    )
    
    wr.s3.to_parquet(
        df=df,
        path=f"s3://{bucket}/{s3_key}",
        index=False
    )
    
    logger.info(f"Dumped {len(raw)} raw records to s3://{bucket}/{s3_key}")
    return {"statusCode": 200, "records": len(raw), "s3_key": s3_key}
