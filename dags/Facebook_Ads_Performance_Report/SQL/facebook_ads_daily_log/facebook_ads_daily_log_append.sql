-- Remove entries for the current date to avoid duplicates
DELETE FROM `shopify-pubsub-project.adhoc_data_asia.facebook_ads_daily_log`
WHERE run_date = CURRENT_DATE("Asia/Kolkata");

-- Insert new data for the current date
INSERT INTO `shopify-pubsub-project.adhoc_data_asia.facebook_ads_daily_log`
SELECT
  CURRENT_DATE("Asia/Kolkata") AS run_date,
  COUNT(DISTINCT A.ad_id) AS ad_count,
  B.TIER
FROM `shopify-pubsub-project.adhoc_data_asia.daily_ad_spend_and_revenue` A
JOIN `shopify-pubsub-project.adhoc_data_asia.FACEBOOK_ADS_SPEND_TIERS_NEW` B
ON A.ad_id = B.ad_id
GROUP BY B.TIER

UNION ALL

SELECT
  CURRENT_DATE("Asia/Kolkata") AS run_date,
  COUNT(DISTINCT ad_id) AS ad_count,
  'TOTAL AD COUNT' AS TIER
FROM `shopify-pubsub-project.adhoc_data_asia.daily_ad_spend_and_revenue`;
