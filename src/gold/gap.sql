CREATE OR REPLACE TABLE gold_gap AS
SELECT 
    obs_month,
    cement_idx,
    coal_idx,
    oil_idx,
    -- Gap Coal (Produksi)
    (cement_idx - coal_idx) AS gap_coal,
    -- Gap Oil (Logistik)
    (cement_idx - oil_idx) AS gap_oil,
    -- Total Gap: Tekanan energi keseluruhan terhadap semen
    (cement_idx - ((coal_idx + oil_idx) / 2)) AS gap_total_energy
FROM gold_indexed;
