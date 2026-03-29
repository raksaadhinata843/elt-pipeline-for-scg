CREATE OR REPLACE TABLE silver_cement AS
SELECT
  date::DATE AS date,
  value::DOUBLE AS cement_price
FROM bronze_cement
QUALIFY row_number() OVER (PARTITION BY date ORDER BY realtime_start DESC) = 1;
