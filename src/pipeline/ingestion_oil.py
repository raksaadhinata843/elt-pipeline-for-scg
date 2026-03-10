import os
import io
import json
import logging
import boto3
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
    if start_date:
        params["start"] = start_date
    if end_date:
        params["end"] = end_date

    resp = requests.get(EIA_URL, params=params, timeout=30)
    resp.raise_for_status()

    payload = resp.json()
    data    = payload["response"]["data"]
    total   = payload["response"]["total"]

    logger.info(f"Fetched {len(data)} records (offset={offset}, total={total})")

    if offset + length < total:
        data += fetch_eia(api_key, frequency, start_date, end_date, offset + length, length)

    return data


def lambda_handler(event, context):
    api_key   = os.environ["EIA_API_KEY"]
    bucket    = os.environ["S3_BUCKET"]
    prefix    = os.environ.get("S3_PREFIX", "bronze/energy/brent")
    frequency = os.environ.get("EIA_FREQUENCY", "daily")
    full_load = os.environ.get("FULL_LOAD", "false").lower() == "true"

    now      = datetime.now(timezone.utc).date()
    end_date   = now.isoformat()
    start_date = None if full_load else (now - timedelta(days=int(os.environ.get("LOOKBACK_DAYS", "30")))).isoformat()

    logger.info(f"Starting ingestion | full_load={full_load} start={start_date} end={end_date}")

    raw = fetch_eia(api_key, frequency, start_date, end_date)

    timestamp = now.strftime("%Y%m%dT%H%M%SZ")

    s3_key = (
        f"{prefix}/"
        f"year={now.year}/"
        f"month={now.month:02d}/"
        f"day={now.day:02d}/"
        f"{timestamp}.json"
    )
    
    boto3.client("s3").put_object(Bucket=bucket, Key=s3_key, Body=json.dumps(raw).encode("utf-8"), ContentType="application/json")

    logger.info(f"Dumped {len(raw)} raw records to s3://{bucket}/{s3_key}")
    return {"statusCode": 200, "records": len(raw), "s3_key": s3_key}
