SELECT 
    CAST(app_id as int) as app_id, 
    country_code,
    count(1) as impressions,
    SUM(CASE WHEN ingestion_dt IS NULL THEN 0 ELSE 1 END) as clicks,
    SUM(CASE WHEN revenue IS NULL THEN 0 ELSE revenue END) as revenue
FROM impressions_df
WHERE app_id != -1 AND country_code is not null AND country_code != ''
GROUP BY 1, 2