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
    WHEN ROAS >= 1 AND cumulative_spend >= 150000 AND DAY_NO >= 7 THEN 'PLATINUM'
    WHEN ROAS >= 1 AND cumulative_spend >= 150000 AND (DAY_NO BETWEEN 3 AND 7) THEN 'PLATINUM'
    WHEN ROAS >= 1 AND (cumulative_spend BETWEEN 50000 AND 150000) AND DAY_NO >= 7 THEN 'GOLD'
    WHEN ROAS >= 1 AND (cumulative_spend BETWEEN 50000 AND 150000) AND (DAY_NO BETWEEN 3 AND 7) THEN 'TBD'
    WHEN ROAS >= 0.75 AND cumulative_spend >= 150000 AND DAY_NO >= 7 THEN 'GOLD'
    WHEN ROAS >= 0.75 AND cumulative_spend >= 150000 AND (DAY_NO BETWEEN 3 AND 7) THEN 'GOLD'
    WHEN ROAS >= 0.75 AND (cumulative_spend BETWEEN 50000 AND 150000) AND DAY_NO >= 7 THEN 'SILVER'
    WHEN ROAS >= 0.75 AND (cumulative_spend BETWEEN 50000 AND 150000) AND (DAY_NO BETWEEN 3 AND 7) THEN 'TBD'
    WHEN ROAS >= 0.5 AND cumulative_spend >= 150000 AND DAY_NO >= 7 THEN NULL
    WHEN ROAS >= 0.5 AND cumulative_spend >= 150000 AND (DAY_NO BETWEEN 3 AND 7) THEN NULL
    WHEN ROAS >= 0.5 AND (cumulative_spend BETWEEN 50000 AND 150000) AND DAY_NO >= 7 THEN NULL
    WHEN ROAS >= 0.5 AND (cumulative_spend BETWEEN 50000 AND 150000) AND (DAY_NO BETWEEN 3 AND 7) THEN NULL
    WHEN ROAS < 0.5 AND cumulative_spend >= 150000 AND DAY_NO >= 7 THEN NULL
    WHEN ROAS < 0.5 AND cumulative_spend >= 150000 AND (DAY_NO BETWEEN 3 AND 7) THEN NULL
    WHEN ROAS < 0.5 AND (cumulative_spend BETWEEN 50000 AND 150000) AND DAY_NO >= 7 THEN NULL
    WHEN ROAS < 0.5 AND (cumulative_spend BETWEEN 50000 AND 150000) AND (DAY_NO BETWEEN 3 AND 7) THEN NULL
    ELSE NULL
  END AS TIER

FROM shopify-pubsub-project.adhoc_data_asia.daily_ad_spend_and_revenue
WHERE DAY_NO >= 3 
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
  T.cumulative_revenue as revenue_14d,
  T.cumulative_spend as spend_14d,
  T.ROAS as ROAS_14d,
  A.lt_revenue,
  A.lt_spends,
  ROUND(A.lt_ROAS,3) AS lt_ROAS
FROM TIERED_DATA T 
JOIN aggregated A
ON T.ad_id = a.ad_id
WHERE LATEST = 1;
