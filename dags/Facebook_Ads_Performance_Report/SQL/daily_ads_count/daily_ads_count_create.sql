CREATE OR REPLACE TABLE `shopify-pubsub-project.adhoc_data_asia.daily_ads_count`
AS
SELECT 
  DISTINCT ad_id,
  start_date,
FROM `shopify-pubsub-project.adhoc_data_asia.daily_ad_spend_and_revenue`
