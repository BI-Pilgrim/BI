CREATE OR REPLACE TABLE `shopify-pubsub-project.adhoc_data_asia.FACEBOOK_ADS_SPEND_TIERS_NEW`
AS


WITH TIERED_DATA AS
(
SELECT
  *,
  ROW_NUMBER() OVER(PARTITION BY ad_id ORDER BY ad_id, DAY_NO DESC) AS LATEST
FROM
(
SELECT
  *,
  CASE
    WHEN DAY_NO = 14 AND cumulative_spend >= 150000 AND ROAS >= 1 THEN "PLATINUM"
    WHEN DAY_NO = 14 AND cumulative_spend >= 150000 AND ROAS >= 0.75 THEN "GOLD"
    WHEN DAY_NO = 14 AND cumulative_spend >= 50000 AND ROAS >= 0.50 THEN "SILVER"
    WHEN DAY_NO = 3 AND cumulative_spend >= 50000 AND ROAS >= 1 THEN "POTENTIAL PLATINUM"
    ELSE NULL
  END AS TIER
FROM shopify-pubsub-project.adhoc_data_asia.daily_ad_spend_and_revenue
WHERE DAY_NO = 3 OR DAY_NO = 14
)
WHERE TIER IS NOT NULL
),

aggregated AS
(
SELECT
  ad_id,
  MIN(date_start) AS start_date,
  DATE_DIFF(MAX(date_start), MIN(date_start), DAY) AS lt_duration,  -- Convert interval to number of days
  SUM(revenue_1d) AS lt_revenue,
  SUM(spend_1d) AS lt_spends,
  CASE
    WHEN SUM(spend_1d) = 0 THEN NULL
    ELSE SUM(revenue_1d) / SUM(spend_1d)
  END AS lt_ROAS
FROM `shopify-pubsub-project.adhoc_data_asia.daily_ad_spend_and_revenue`
GROUP BY ad_id
)
SELECT 
  T.ad_id,
  T.ad_name,
  T.TIER,
  A.start_date,
  A.lt_duration,
  ROUND(T.cumulative_revenue,3) as revenue_14d,
  ROUND(T.cumulative_spend,3) as spend_14d,
  ROUND(T.ROAS,3) as ROAS_14d,
  ROUND(A.lt_revenue,3) AS lt_revenue,
  ROUND(A.lt_spends,3) AS lt_spends,
  ROUND(A.lt_ROAS,3) AS lt_ROAS
FROM TIERED_DATA T 
JOIN aggregated A
ON T.ad_id = a.ad_id
WHERE LATEST = 1;