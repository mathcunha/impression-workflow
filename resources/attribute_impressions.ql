SELECT impressions.*, clicks.dt as ingestion_dt, clicks.revenue
FROM impressions
LEFT JOIN clicks on clicks.impression_id = impressions.id