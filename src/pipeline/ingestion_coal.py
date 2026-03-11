import os
import json
import logging
import boto3
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

    full_load = event.get("full_load", False)

    now        = datetime.now(timezone.utc).date()
    end_date   = now.isoformat()
    start_date = None if full_load else (now - timedelta(days=int(os.environ.get("LOOKBACK_DAYS", "30")))).isoformat()

    logger.info(f"Fetching Coal Newcastle | {start_date} → {end_date}")

    raw = fetch_coal_newcastle(api_key, start_date, end_date)

    raw["observations"] = [
        obs for obs in raw["observations"] 
        if obs["value"] != "."
    ]

    # Dump raw response as-is ke S3
    timestamp = now.strftime("%Y%m%dT%H%M%SZ")

    s3_key = (
        f"{prefix}/"
        f"year={now.year}/"
        f"month={now.month:02d}/"
        f"day={now.day:02d}/"
        f"{timestamp}.json"
    )

    boto3.client("s3").put_object(
        Bucket      = bucket,
        Key         = s3_key,
        Body        = json.dumps(raw).encode("utf-8"),
        ContentType = "application/json",
    )

    logger.info(f"Uploaded to s3://{bucket}/{s3_key}")

    return {
        "statusCode": 200,
        "s3_uri":     f"s3://{bucket}/{s3_key}",
}












