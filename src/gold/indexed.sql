CREATE OR REPLACE TABLE gold_indexed AS
WITH UniqueUnified AS (
    SELECT DISTINCT 
        obs_month, 
        cement_price, 
        coal_price, 
        oil_price
    FROM gold_unified 
),
BasePrices AS (
    -- Ambil harga Januari 2010 sebagai patokan (Base 100)
    SELECT 
        cement_price as base_cement,
        coal_price as base_coal,
        oil_price as base_oil
    FROM UniqueUnified
    WHERE obs_month = '2010-01-01'
    LIMIT 1
)
SELECT 
    u.obs_month,
    u.cement_price,
    u.coal_price,
    u.oil_price,
    -- Rumus Indexing
    (u.cement_price / b.base_cement) * 100 as cement_idx,
    (u.coal_price / b.base_coal) * 100 as coal_idx,
    (u.oil_price / b.base_oil) * 100 as oil_idx
FROM UniqueUnified u, BasePrices b
ORDER BY u.obs_month ASC;
