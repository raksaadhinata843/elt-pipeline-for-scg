CREATE OR REPLACE TABLE silver_oil AS
SELECT
  period AS date,
  value AS oil_price
FROM bronze_oil
QUALIFY row_number() OVER (PARTITION BY period ORDER BY period DESC) = 1;
