MERGE INTO `shopify-pubsub-project.adhoc_data_asia.daily_ad_spend_and_revenue` AS TARGET
USING
(
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
    SUM(spend) OVER(PARTITION BY ad_id ORDER BY date_start) = 0 THEN NULL
  ELSE
    ROUND((SUM(CAST(JSON_EXTRACT_SCALAR(ACT_VALUES, '$.1d_click') AS FLOAT64)) OVER(PARTITION BY ad_id ORDER BY date_start))/(SUM(spend) OVER(PARTITION BY ad_id ORDER BY date_start)),3)
  END AS ROAS,
FROM
(
  SELECT
  *,
  ROW_NUMBER() OVER(PARTITION BY ad_id ORDER BY date_start DESC, _airbyte_extracted_at DESC) as row_num
  FROM
  `shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads_insights`,
  UNNEST(JSON_EXTRACT_ARRAY(action_values)) AS ACT_VALUES
  WHERE JSON_EXTRACT_SCALAR(ACT_VALUES, '$.action_type') = 'purchase'
)
WHERE row_num = 1 and date(date_start) >= DATE_SUB(CURRENT_DATE("Asia/Kolkata"), INTERVAL 0 DAY)
) AS SOURCE
ON TARGET.ad_id = SOURCE.ad_id AND TARGET.date_start = SOURCE.date_start
WHEN MATCHED AND TARGET.date_start < SOURCE.date_start
THEN UPDATE SET
TARGET.ad_id = SOURCE.ad_id,
TARGET.ad_name = SOURCE.ad_name,
TARGET.date_start = SOURCE.date_start,
TARGET.date_stop = SOURCE.date_stop,
TARGET.DAY_NO = SOURCE.DAY_NO,
TARGET.start_date = SOURCE.start_date,
TARGET.spend_1d = SOURCE.spend_1d,
TARGET.revenue_1d = SOURCE.revenue_1d,
TARGET.cumulative_spend = SOURCE.cumulative_spend,
TARGET.cumulative_revenue = SOURCE.cumulative_revenue,
TARGET.ROAS = SOURCE.ROAS
WHEN NOT MATCHED
THEN INSERT
(
ad_id,
ad_name,
date_start,
date_stop,
DAY_NO,
start_date,
spend_1d,
revenue_1d,
cumulative_spend,
cumulative_revenue,
ROAS
)
VALUES
(
SOURCE.ad_id,
SOURCE.ad_name,
SOURCE.date_start,
SOURCE.date_stop,
SOURCE.DAY_NO,
SOURCE.start_date,
SOURCE.spend_1d,
SOURCE.revenue_1d,
SOURCE.cumulative_spend,
SOURCE.cumulative_revenue,
SOURCE.ROAS
)