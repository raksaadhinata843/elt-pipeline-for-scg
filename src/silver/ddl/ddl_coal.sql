CREATE OR REPLACE TABLE silver_coal AS
SELECT
  date,
  value AS coal_price
FROM bronze_coal
QUALIFY row_number() OVER (PARTITION BY date ORDER BY realtime_start DESC) = 1;
