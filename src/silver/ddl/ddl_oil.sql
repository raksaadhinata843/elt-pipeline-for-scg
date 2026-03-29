CREATE OR REPLACE TABLE silver_oil AS
SELECT
  period::DATE AS date,
  value::DOUBLE AS oil_price
FROM bronze_oil
QUALIFY row_number() OVER (PARTITION BY period ORDER BY period DESC) = 1;
