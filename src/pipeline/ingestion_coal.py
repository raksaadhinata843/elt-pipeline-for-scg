import os
import json
import logging
import boto3
import requests
from datetime import datetime, timedelta, timezone

logger = logging.getLogger()
logger.setLevel(logging.INFO)

EIA_BASE_URL = "https://api.eia.gov/v2/coal/spot-price/data/"


def fetch_coal_newcastle(api_key: str, start_date: str = None, end_date: str = None) -> dict:
    params = {
        "api_key":            api_key,
        "frequency":          "monthly",
        "data[0]":            "value",
        "facets[series][]":   "COAL.PRICE.NEWCASTLE",
        "sort[0][column]":    "period",
        "sort[0][direction]": "asc",
        "length":             5000,
        "start":              "start_date",
        "end":                "end_date"
    }

    response = requests.get(EIA_BASE_URL, params=params, timeout=30)
    response.raise_for_status()

    return response.json()


def lambda_handler(event: dict, context) -> dict:
    api_key = os.environ["EIA_API_KEY"]
    bucket  = os.environ["S3_BUCKET"]
    prefix  = os.environ.get("S3_PREFIX", "raw/coal_newcastle")

    today      = datetime.now(timezone.utc).date()
    start_date = os.environ.get("START_DATE", (today - timedelta(days=30)).isoformat())
    end_date   = os.environ.get("END_DATE", today.isoformat())

    logger.info(f"Fetching Coal Newcastle | {start_date} → {end_date}")

    raw = fetch_coal_newcastle(api_key, start_date, end_date)

    # Dump raw response as-is ke S3
    now = datetime.now(timezone.utc)
    s3_key = (
        f"{prefix}/"
        f"year={now.year}/"
        f"month={now.month:02d}/"
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

