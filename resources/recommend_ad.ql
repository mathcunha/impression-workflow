WITH grouped_impressions as (
SELECT 
    app_id, 
    country_code,
    advertiser_id, 
    count(1) as impressions,
    SUM(CASE WHEN revenue IS NULL THEN 0 ELSE revenue END) as revenue
FROM impressions_df
WHERE app_id != -1 AND advertiser_id != -1 AND country_code is not null AND country_code != ''
GROUP BY 1, 2, 3
),
revenue_rate_data as (
    SELECT app_id, country_code, advertiser_id, revenue / impressions as revenue_rate
    FROM grouped_impressions
),
revenue_rate_data_with_rn as (
    SELECT *, ROW_NUMBER() OVER(PARTITION BY app_id, country_code ORDER BY revenue_rate DESC) as rn
    FROM revenue_rate_data
)
SELECT 
    CAST(app_id as int) as app_id, country_code
    , group_concat(advertiser_id) as recommended_advertiser_ids
FROM revenue_rate_data_with_rn
WHERE rn <= 5
GROUP BY app_id, country_code
ORDER BY app_id, country_code