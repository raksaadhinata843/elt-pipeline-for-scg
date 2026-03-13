import os
import logging
import pandas as pd
import awswrangler as wr
import requests
from datetime import datetime, timedelta, timezone

logger = logging.getLogger()
logger.setLevel(logging.INFO)

FRED_BASE_URL = "https://api.stlouisfed.org/fred/series/observations"


def fetch_coal_newcastle(api_key: str, start_date: str = None, end_date: str = None) -> dict:
    params = {
        "api_key":           api_key,
        "series_id":         "PCOALAUUSDM",
        "file_type":         "json",
        "sort_order":        "asc",
        "observation_start": start_date,
        "observation_end":   end_date,
    }

    response = requests.get(FRED_BASE_URL, params=params, timeout=30)
    response.raise_for_status()

    return response.json()


def lambda_handler(event: dict, context) -> dict:
    api_key = os.environ["FRED_API_KEY"]
    bucket  = os.environ["S3_BUCKET"]
    prefix  = os.environ.get("S3_PREFIX", "bronze/energy/coal")

    full_load = event.get("full_load", True)

    now = datetime.now(timezone.utc)
    raw = fetch_coal_newcastle(api_key, "2010-01-01", now.strftime("%Y-%m-%d"))
    
    logger.info(f"Fetching Coal Newcastle")

    df = pd.DataFrame(raw["observations"])
    df["value"] = pd.to_numeric(df["value"], errors='coerce')
    df["date"] = pd.to_datetime(df["date"])
 
    raw["observations"] = [
        obs for obs in raw["observations"] 
        if obs["value"] != "."
    ]

    # Dump raw response as-is ke S3
    timestamp = now.strftime("%Y%m%dT%H%M%SZ")

    s3_key = (
        f"s3://{bucket}/{prefix}/"
        f"year={now.year}/month={now.month:02d}/day={now.day:02d}/"
        f"{timestamp}.parquet"
    )

    wr.s3.to_parquet(
        df=df,
        key=s3_key,
        index=False
    )
    
    logger.info(f"Uploaded to s3://{bucket}/{s3_key}")

    return {
        "statusCode": 200,
        "s3_uri":     f"s3://{bucket}/{s3_key}",
    }
























