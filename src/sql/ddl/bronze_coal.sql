CREATE TABLE IF NOT EXISTS bronze_coal_price (
    observation_date DATE,
    price DOUBLE,
    realtime_start DATE,
    realtime_end DATE,
    ingestion_timestamp TIMESTAMP
);
