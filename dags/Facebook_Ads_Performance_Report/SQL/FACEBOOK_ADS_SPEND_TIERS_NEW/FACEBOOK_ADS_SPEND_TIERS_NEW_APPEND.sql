CREATE OR REPLACE TABLE `shopify-pubsub-project.adhoc_data_asia.FACEBOOK_ADS_SPEND_TIERS_NEW`
AS


WITH TIERED_DATA AS
(
SELECT
  *,MERGE INTO `shopify-pubsub-project.adhoc_data_asia.FACEBOOK_ADS_SPEND_TIERS_NEW` AS TARGET
USING (
  WITH TIERED_DATA AS (
    SELECT
      *,
      ROW_NUMBER() OVER(PARTITION BY ad_id ORDER BY ad_id, DAY_NO DESC) AS LATEST
    FROM (
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

  aggregated AS (
    SELECT
      ad_id,
      MIN(date_start) AS start_date,
      DATE_DIFF(MAX(date_start), MIN(date_start), DAY) AS lt_duration,
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
    ROUND(T.cumulative_revenue, 3) AS revenue_14d,
    ROUND(T.cumulative_spend, 3) AS spend_14d,
    ROUND(T.ROAS, 3) AS ROAS_14d,
    ROUND(A.lt_revenue, 3) AS lt_revenue,
    ROUND(A.lt_spends, 3) AS lt_spends,
    ROUND(A.lt_ROAS, 3) AS lt_ROAS,
    T.date_start
  FROM TIERED_DATA T
  JOIN aggregated A
  ON T.ad_id = A.ad_id
  WHERE LATEST = 1 AND DATE(T.date_start) >= DATE_SUB(CURRENT_DATE("Asia/Kolkata"), INTERVAL 0 DAY)
) AS SOURCE
ON TARGET.ad_id = SOURCE.ad_id
WHEN MATCHED AND TARGET.start_date < SOURCE.start_date
THEN UPDATE SET
  ad_name = SOURCE.ad_name,
  TIER = SOURCE.TIER,
  start_date = SOURCE.start_date,
  lt_duration = SOURCE.lt_duration,
  revenue_14d = SOURCE.revenue_14d,
  spend_14d = SOURCE.spend_14d,
  ROAS_14d = SOURCE.ROAS_14d,
  lt_revenue = SOURCE.lt_revenue,
  lt_spends = SOURCE.lt_spends,
  lt_ROAS = SOURCE.lt_ROAS
WHEN NOT MATCHED
THEN INSERT (
  ad_id,
  ad_name,
  TIER,
  start_date,
  lt_duration,
  revenue_14d,
  spend_14d,
  ROAS_14d,
  lt_revenue,
  lt_spends,
  lt_ROAS
)
VALUES (
  SOURCE.ad_id,
  SOURCE.ad_name,
  SOURCE.TIER,
  SOURCE.start_date,
  SOURCE.lt_duration,
  SOURCE.revenue_14d,
  SOURCE.spend_14d,
  SOURCE.ROAS_14d,
  SOURCE.lt_revenue,
  SOURCE.lt_spends,
  SOURCE.lt_ROAS
);

DELETE FROM `shopify-pubsub-project.adhoc_data_asia.FACEBOOK_ADS_SPEND_TIERS_NEW`
WHERE start_date < DATE_SUB(CURRENT_DATE("Asia/Kolkata"), INTERVAL 365 DAY);


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



-- SELECT 
--   *,
-- FROM (
--   SELECT
--     *,
--     CASE
--       WHEN DAY_NO = 14 AND cumulative_spend >= 150000 AND ROAS >= 1 THEN "PLATINUM"
--       -- THE ACTUAL CONDITION THAT MAKES SENSE FOR PLATINUM SHOULD BE 
--       -- WHEN DAY_NO = 14 AND cumulative_spend <= 150000 AND ROAS >= 1 THEN "PLATINUM" 
--       WHEN DAY_NO = 14 AND cumulative_spend >= 150000 AND ROAS >= 0.75 THEN "GOLD"
--       WHEN DAY_NO = 14 AND cumulative_spend >= 50000 AND ROAS >= 0.50 THEN "SILVER"
--       WHEN DAY_NO = 3 AND cumulative_spend >= 50000 AND ROAS >= 1 THEN "POTENTIAL PLATINUM"
--       ELSE NULL
--     END AS TIER
--   FROM shopify-pubsub-project.adhoc_data_asia.daily_ad_spend_and_revenue
-- )
-- QUALIFY TIER IS NOT NULL AND ROW_NUMBER() OVER(PARTITION BY ad_id ORDER BY DAY_NO DESC) = 1
-- -- IF AN ADD RAN ONLY FOR 3 DAYS THEN THERE WILL ONLY BE ONE ROW FOR DAY_NO=3 AND THERE WILL BE NO RECORD OF DAY_NO 14
-- ORDER BY start_date DESC;