MERGE INTO `shopify-pubsub-project.adhoc_data_asia.daily_ads_count` AS TARGET
USING
(
SELECT
  DISTINCT ad_id,
  start_date
  FROM
  (
    SELECT
    *,
    ROW_NUMBER() OVER(PARTITION BY ad_id ORDER BY start_date DESC) as rn
    FROM `shopify-pubsub-project.adhoc_data_asia.daily_ad_spend_and_revenue`
    WHERE start_date >= DATE_SUB(CURRENT_DATE("Asia/Kolkata"), INTERVAL 1 DAY)
  )
) AS SOURCE
ON TARGET.ad_id = SOURCE.ad_id AND TARGET.start_date = SOURCE.start_date
WHEN MATCHED AND TARGET.start_date < SOURCE.start_date
THEN UPDATE SET
TARGET.ad_id = SOURCE.ad_id,
TARGET.start_date = SOURCE.start_date
WHEN NOT MATCHED
THEN INSERT
(
ad_id,
start_date
)
VALUES
(
SOURCE.ad_id,
SOURCE.start_date
)