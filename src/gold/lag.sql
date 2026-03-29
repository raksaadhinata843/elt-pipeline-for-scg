CREATE OR REPLACE TABLE gold_lag AS
SELECT * FROM (
  SELECT 
        obs_month,
        cement_idx,
        -- Lag Batubara
        LAG(coal_idx, 1) OVER(ORDER BY obs_month) AS coal_lag1,
        LAG(coal_idx, 2) OVER(ORDER BY obs_month) AS coal_lag2,
        LAG(coal_idx, 3) OVER(ORDER BY obs_month) AS coal_lag3,
        LAG(coal_idx, 4) OVER(ORDER BY obs_month) AS coal_lag4,
        -- Lag Minyak
        LAG(oil_idx, 1) OVER(ORDER BY obs_month) AS oil_lag1,
        LAG(oil_idx, 2) OVER(ORDER BY obs_month) AS oil_lag2,
        LAG(oil_idx, 3) OVER(ORDER BY obs_month) AS oil_lag3,
        LAG(oil_idx, 4) OVER(ORDER BY obs_month) AS oil_lag4
    FROM gold_indexed
) AS sub
WHERE obs_month >= '2010-05-01'; -- bulan di tentukan karna menghindari null value
