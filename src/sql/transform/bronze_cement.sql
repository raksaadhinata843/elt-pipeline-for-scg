INSERT INTO bronze_cement_price
    SELECT
        CAST(date AS DATE) AS observation_date,
        CAST(value AS DOUBLE) AS price,
        CAST(realtime_start AS DATE)  AS realtime_start,
        CAST(realtime_end AS DATE)    AS realtime_end,
        CURRENT_TIMESTAMP             AS ingestion_timestamp
    FROM read_parquet(
    's3://scg-energy-analytics-data/bronze/construction/cement/year=2026/month=03/day=13/20260313T201009Z.parquet'
    )

ON target.observation_date = source.observation_date

WHEN MATCHED THEN
UPDATE SET price = source.price

WHEN NOT MATCHED THEN
INSERT (observation_date, price)
VALUES (source.observation_date, source.price);
