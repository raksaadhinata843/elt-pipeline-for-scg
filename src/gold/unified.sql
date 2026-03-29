CREATE OR REPLACE TABLE gold_unified AS
SELECT 
    DATE_TRUNC('month', c.date) AS obs_month,
    c.cement_price AS cement_price,
    -- Rata-rata harga energi di bulan tersebut
    AVG(co.coal_price) OVER(PARTITION BY DATE_TRUNC('month', co.date)) as coal_price,
    AVG(o.oil_price) OVER(PARTITION BY DATE_TRUNC('month', CAST(o.date AS TIMESTAMP))) as oil_price
FROM silver_cement c
INNER JOIN silver_coal co ON DATE_TRUNC('month', c.date) = DATE_TRUNC('month', co.date)
INNER JOIN silver_oil o ON DATE_TRUNC('month', c.date) = DATE_TRUNC('month', CAST(o.date AS TIMESTAMP))
WHERE C.DATE  >= '2010-01-01'
ORDER BY obs_month;
