CREATE OR REPLACE TABLE `shopify-pubsub-project.adhoc_data_asia.daily_ad_spend_and_revenue`
AS
SELECT
  ad_id,
  ad_name,
  date_start,
  date_stop,
  ROW_NUMBER() OVER(PARTITION  BY ad_id ORDER BY date_start) DAY_NO,
  MIN(date_start) OVER(PARTITION BY ad_id) AS start_date,
  spend as spend_1d,
  CAST(JSON_EXTRACT_SCALAR(ACT_VALUES, '$.1d_click') AS FLOAT64) AS revenue_1d,
  SUM(spend) OVER(PARTITION BY ad_id ORDER BY date_start) as cumulative_spend,
  SUM(CAST(JSON_EXTRACT_SCALAR(ACT_VALUES, '$.1d_click') AS FLOAT64)) OVER(PARTITION BY ad_id ORDER BY date_start) as cumulative_revenue,
  CASE
  WHEN
    SUM(spend) OVER(PARTITION BY ad_id ORDER BY date_start) = 0
  THEN NULL
  ELSE
    ROUND((SUM(CAST(JSON_EXTRACT_SCALAR(ACT_VALUES, '$.1d_click') AS FLOAT64)) OVER(PARTITION BY ad_id ORDER BY date_start))/(SUM(spend) OVER(PARTITION BY ad_id ORDER BY date_start)),3) END AS ROAS,
FROM 
  `shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads_insights`,
  UNNEST(JSON_EXTRACT_ARRAY(action_values)) AS ACT_VALUES
WHERE JSON_EXTRACT_SCALAR(ACT_VALUES, '$.action_type') = 'purchase'
ORDER BY 
  ad_id, date_start DESC;
