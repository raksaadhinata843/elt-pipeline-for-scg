MERGE INTO bronze_oil_price AS target
USING (

SELECT
    CAST(date AS DATE) AS observation_date,
    CAST(value AS DOUBLE) AS price
    units,
    series,
    CURRENT_TIMESTAMP         AS ingestion_timestamp

FROM read_parquet(
's3://scg-energy-analytics-data/bronze/energy/brent/year=2026/month=03/day=14/20260314T210045Z.parquet'
)

) AS source

ON target.observation_date = source.observation_date

WHEN MATCHED THEN
UPDATE SET price = source.price

WHEN NOT MATCHED THEN
INSERT (observation_date, price)
VALUES (source.observation_date, source.price);
