CREATE OR REPLACE TABLE silver_coal as
SELECT
  date,
  value AS coal_price
FROM bronze_coal
qualify row_number() over (partition BY date ORDER BY date DESC) = 1;
