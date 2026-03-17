CREATE TABLE IF NOT EXISTS bronze_oil_price (
    observation_date DATE,
    price DOUBLE,
    units VARCHAR,
    series VARCHAR,
    ingestion_timestamp TIMESTAMP
);
