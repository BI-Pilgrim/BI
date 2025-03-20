MERGE `shopify-pubsub-project.Facebook_Ads_Performance_Report.facebook_ads_daily_tier_count_log` TGT
USING (
  SELECT
    CURRENT_DATE("Asia/Kolkata") AS run_date,
    A.current_tier AS TIER,
    COUNT(DISTINCT A.ad_id) AS ad_count
  FROM (
    SELECT
      ad_id,
      CASE  
        WHEN roas_l7d >= 1.4 AND spend_l7d >= 100000 THEN 'TITANIUM'
        WHEN roas_l7d >= 1.4 AND (spend_l7d BETWEEN 50000 AND 100000) THEN 'POT. TITANIUM'
        WHEN (roas_l7d BETWEEN 1.0 AND 1.4) AND spend_l7d >= 100000 THEN 'PLATINUM'
        WHEN (roas_l7d BETWEEN 1.0 AND 1.4) AND (spend_l7d BETWEEN 50000 AND 100000) THEN 'POT. PLATINUM'
        WHEN (roas_l7d BETWEEN 0.8 AND 1.0) AND spend_l7d >= 50000 THEN 'GOLD'
        WHEN (roas_l7d BETWEEN 0.6 AND 0.8) AND (spend_l7d BETWEEN 50000 AND 100000) THEN 'SILVER'
        ELSE 'OTHERS'
      END AS current_tier
    FROM (
      SELECT DISTINCT
        ad_id,
        spend_l7d,
        revenue_l7d,
        SAFE_DIVIDE(revenue_l7d, spend_l7d) AS roas_l7d
      FROM `shopify-pubsub-project.Facebook_Ads_Performance_Report.Final_Report2`
    )
  ) A
  GROUP BY A.current_tier

  UNION ALL

  SELECT
    CURRENT_DATE("Asia/Kolkata") AS run_date,
    'TOTAL AD COUNT' AS TIER,
    COUNT(DISTINCT ad_id) AS ad_count
  FROM (
    SELECT
      ad_id
    FROM (
      SELECT DISTINCT
        ad_id,
        spend_l7d,
        revenue_l7d,
        SAFE_DIVIDE(revenue_l7d, spend_l7d) AS roas_l7d
      FROM `shopify-pubsub-project.Facebook_Ads_Performance_Report.Final_Report2`
    )
  )
) SRC
ON TGT.run_date = SRC.run_date AND TGT.TIER = SRC.TIER
WHEN MATCHED THEN
  UPDATE SET TGT.ad_count = SRC.ad_count
WHEN NOT MATCHED THEN
  INSERT (run_date, TIER, ad_count)
  VALUES (SRC.run_date, SRC.TIER, SRC.ad_count);
