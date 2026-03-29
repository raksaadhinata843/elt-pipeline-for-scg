CREATE OR REPLACE TABLE silver_oil AS
SELECT
  period AS date,
  value AS oil_price
FROM bronze_oil
