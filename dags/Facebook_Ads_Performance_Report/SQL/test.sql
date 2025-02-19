WITH RankedData AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY ad_id ORDER BY TIER_RANK ASC, DAY_NO DESC) AS BestTierRow,  -- To get the BEST_TIER
    ROW_NUMBER() OVER(PARTITION BY ad_id ORDER BY ad_id, DAY_NO DESC) AS LATEST --To get only the row with the LATEST status
  FROM (
    SELECT
      *,
      CASE  -- To assign the case for each ad
        WHEN ROAS >= 1.4 AND cumulative_spend >= 100000 AND DAY_NO >= 5 THEN 'TITANIUM'
        WHEN ROAS >= 1.4 AND (cumulative_spend BETWEEN 50000 AND 100001) AND DAY_NO >= 5 THEN 'POT. TITANIUM'
        WHEN (ROAS BETWEEN 1.0 AND 1.4) AND cumulative_spend >= 100000 AND DAY_NO >= 5 THEN 'PLATINUM'
        WHEN (ROAS BETWEEN 1.0 AND 1.4) AND (cumulative_spend BETWEEN 50000 AND 100001) AND DAY_NO >= 5 THEN 'POT. PLATINUM'
        WHEN (ROAS BETWEEN 0.8 AND 1.0) AND cumulative_spend >= 50000 AND DAY_NO >= 5 THEN 'GOLD'
        WHEN (ROAS BETWEEN 0.6 AND 0.8) AND cumulative_spend < 100000 AND DAY_NO >= 5 THEN 'SILVER'
        ELSE 'OTHERS'
      END AS TIER,

      CASE  --To assign the Earliest TIER for each add (Earliest tier is the tier when ad spent 1 lakh)
        WHEN ROAS >= 1.4 AND cumulative_spend >= 100000 THEN 'TITANIUM'
        WHEN ROAS >= 1.4 AND cumulative_spend BETWEEN 50000 AND 100001 THEN 'POT. TITANIUM'
        WHEN (ROAS BETWEEN 1.0 AND 1.4) AND cumulative_spend >= 100000 THEN 'PLATINUM'
        WHEN (ROAS BETWEEN 1.0 AND 1.4) AND cumulative_spend BETWEEN 50000 AND 100001 THEN 'POT. PLATINUM'
        WHEN (ROAS BETWEEN 0.8 AND 1.0) AND cumulative_spend >= 50000 THEN 'GOLD'
        WHEN (ROAS BETWEEN 0.6 AND 0.8) AND cumulative_spend < 100000 THEN 'SILVER'
        ELSE 'OTHERS'
      END AS EARLIEST_TIER,

      CASE  --TO assign the best tier an ad achieved(lowest the rank better the TIER)
        WHEN ROAS >= 1.4 AND cumulative_spend >= 100000 AND DAY_NO >= 5 THEN 1
        WHEN ROAS >= 1.4 AND cumulative_spend < 100000 AND DAY_NO >= 5 THEN 2
        WHEN (ROAS BETWEEN 1.0 AND 1.4) AND cumulative_spend >= 100000 AND DAY_NO >= 5 THEN 3
        WHEN (ROAS BETWEEN 1.0 AND 1.4) AND cumulative_spend < 100000 AND DAY_NO >= 5 THEN 4
        WHEN (ROAS BETWEEN 0.8 AND 1.0) AND cumulative_spend >= 50000 AND DAY_NO >= 5 THEN 5
        WHEN (ROAS BETWEEN 0.6 AND 0.8) AND cumulative_spend < 100000 AND DAY_NO >= 5 THEN 6
        ELSE 999
      END AS TIER_RANK
    FROM shopify-pubsub-project.adhoc_data_asia.daily_ad_spend_and_revenue
    WHERE DAY_NO >= 5
  ) 
  WHERE TIER IS NOT NULL
), 

-- Filtering the best tier for each ad
BestTier AS (
  SELECT ad_id, TIER AS BEST_TIER
  FROM RankedData
  WHERE BestTierRow = 1 -- Selects the row with the lowest TIER_RANK for each ad_id
),

-- Joining the best tier cte with ranked data to get final data 
FinalData AS (
  SELECT 
    rd.ad_id,
    rd.ad_name,
    rd.start_date,
    rd.date_start,
    bt.BEST_TIER,
    rd.TIER AS CURRENT_TIER,
    rd.EARLIEST_TIER,
    rd.LATEST,
    rd.ROAS AS Current_ROAS,
    CAST(rd.cumulative_spend AS INT64) AS Tot_spends,
    CAST(rd.cumulative_revenue AS INT64) AS Tot_revenue
  FROM RankedData rd
  LEFT JOIN BestTier bt ON rd.ad_id = bt.ad_id
  WHERE rd.LATEST = 1 --To get only the row with latest status
),


-- Joining the final Data with ad creative master to get influencer names and gender
JoinedData AS (
  SELECT 
    ai.*,
    influencer,
    gender,
    ROW_NUMBER() OVER (PARTITION BY ai.ad_id ORDER BY LENGTH(id.influencer) DESC) AS RowNum
  FROM FinalData ai
  LEFT JOIN `shopify-pubsub-project.adhoc_data_asia.creative_master` id 
  ON ai.ad_name LIKE CONCAT('%', id.influencer, '%')
)

-- Querying the data from JoinedData
SELECT 
  ad_id,
  ad_name,
  start_date,
  BEST_TIER,
  CURRENT_TIER,
  EARLIEST_TIER,
  -- LATEST,
  Current_ROAS,
  Tot_spends,
  Tot_revenue,
  influencer,
  gender
FROM JoinedData
WHERE RowNum = 1 --and influencer is null -- Selects only the best match per ad_id
ORDER BY ad_id, ad_name;
