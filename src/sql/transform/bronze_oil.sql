INSERT INTO bronze_oil_price
    SELECT
        CAST(date AS DATE)        AS observation_date,
        CAST(value AS DOUBLE)     AS price,
        units,
        series,
        CURRENT_TIMESTAMP         AS ingestion_timestamp
   FROM read_parquet(
    's3://scg-energy-analytics-data/bronze/energy/brent/year=2026/month=03/day=17/20260317T162408Z.parquet'
    );
