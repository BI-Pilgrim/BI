WITH RankedSource AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY ad_group_id ORDER BY _airbyte_extracted_at DESC) AS rn
  FROM (
    SELECT
      _airbyte_extracted_at,
      ad_group_optimized_targeting_enabled,
      ad_group_cpc_bid_micros / 1000000 AS ad_group_cpc_bid_rupees,
      ad_group_cpm_bid_micros / 1000000 AS ad_group_cpm_bid_rupees,
      ad_group_cpv_bid_micros / 1000000 AS ad_group_cpv_bid_rupees,
      ad_group_effective_target_cpa_micros * 1000000 AS ad_group_effective_target_cpa_rupees,
      ad_group_id,
      ad_group_percent_cpc_bid_micros,
      ad_group_target_cpa_micros / 1000000 AS ad_group_target_cpa_rupees,
      ad_group_target_cpm_micros / 1000000 AS ad_group_target_cpm_rupees,
      campaign_id,
      metrics_cost_micros / 1000000 AS metrics_cost_rupees,
      ad_group_effective_target_roas,
      ad_group_target_roas,
      ad_group_ad_rotation_mode,
      JSON_EXTRACT_SCALAR(ad_group_targeting_setting_target_restrictions, '$.target_restriction') AS target_restriction,
      JSON_EXTRACT_SCALAR(ad_group_targeting_setting_target_restrictions, '$.targeting_dimension') AS targeting_dimension,
      JSON_EXTRACT_SCALAR(ad_group_targeting_setting_target_restrictions, '$.targeting_value') AS targeting_value
    FROM
      shopify-pubsub-project.pilgrim_bi_google_ads.ad_group
    WHERE
      DATE(_airbyte_extracted_at) >= DATE_SUB(CURRENT_DATE("Asia/Kolkata"), INTERVAL 10 DAY)
  )
)
SELECT *
FROM RankedSource
WHERE rn = 1;

MERGE INTO `shopify-pubsub-project.Data_Warehouse_GoogleAds_Staging.ad_group` AS TARGET
USING (
  WITH RankedSource AS (
    SELECT
      *,
      ROW_NUMBER() OVER (PARTITION BY ad_group_id ORDER BY _airbyte_extracted_at DESC) AS rn
    FROM (
      SELECT
      -- All your source columns and transformations
      _airbyte_extracted_at,
      ad_group_optimized_targeting_enabled,
      ad_group_cpc_bid_micros / 1000000 AS ad_group_cpc_bid_rupees,
      ad_group_cpm_bid_micros / 1000000 AS ad_group_cpm_bid_rupees,
      ad_group_cpv_bid_micros / 1000000 AS ad_group_cpv_bid_rupees,
      ad_group_effective_target_cpa_micros * 1000000 AS ad_group_effective_target_cpa_rupees,
      ad_group_id,
      ad_group_percent_cpc_bid_micros,
      ad_group_target_cpa_micros / 1000000 AS ad_group_target_cpa_rupees,
      ad_group_target_cpm_micros / 1000000 AS ad_group_target_cpm_rupees,
      campaign_id,
      metrics_cost_micros / 1000000 AS metrics_cost_rupees,
      ad_group_effective_target_roas,
      ad_group_target_roas,
      ad_group_ad_rotation_mode,
      JSON_EXTRACT_SCALAR(ad_group_targeting_setting_target_restrictions, '$.target_restriction') AS target_restriction,
      JSON_EXTRACT_SCALAR(ad_group_targeting_setting_target_restrictions, '$.targeting_dimension') AS targeting_dimension,
      JSON_EXTRACT_SCALAR(ad_group_targeting_setting_target_restrictions, '$.targeting_value') AS targeting_value
      FROM
        shopify-pubsub-project.pilgrim_bi_google_ads.ad_group
      WHERE
        DATE(_airbyte_extracted_at) >= DATE_SUB(CURRENT_DATE("Asia/Kolkata"), INTERVAL 10 DAY)
    )
  )
  SELECT *
  FROM RankedSource
  WHERE rn = 1
) AS SOURCE
ON TARGET.ad_group_id = SOURCE.ad_group_id
-- Rest of your merge statement...
WHEN MATCHED AND SOURCE._airbyte_extracted_at > TARGET._airbyte_extracted_at 
THEN UPDATE SET
  TARGET._airbyte_extracted_at = SOURCE._airbyte_extracted_at,
  TARGET.ad_group_optimized_targeting_enabled = SOURCE.ad_group_optimized_targeting_enabled,
  TARGET.ad_group_cpc_bid_rupees = SOURCE.ad_group_cpc_bid_rupees,
  TARGET.ad_group_cpm_bid_rupees = SOURCE.ad_group_cpm_bid_rupees,
  TARGET.ad_group_cpv_bid_rupees = SOURCE.ad_group_cpv_bid_rupees,
  TARGET.ad_group_effective_target_cpa_rupees = SOURCE.ad_group_effective_target_cpa_rupees,
  TARGET.ad_group_id = SOURCE.ad_group_id,
  TARGET.ad_group_percent_cpc_bid_micros = SOURCE.ad_group_percent_cpc_bid_micros,
  TARGET.ad_group_target_cpa_rupees = SOURCE.ad_group_target_cpa_rupees,
  TARGET.ad_group_target_cpm_rupees = SOURCE.ad_group_target_cpm_rupees,
  TARGET.campaign_id = SOURCE.campaign_id,
  TARGET.metrics_cost_rupees = SOURCE.metrics_cost_rupees,
  TARGET.ad_group_effective_target_roas = SOURCE.ad_group_effective_target_roas,
  TARGET.ad_group_target_roas = SOURCE.ad_group_target_roas,
  TARGET.ad_group_ad_rotation_mode = SOURCE.ad_group_ad_rotation_mode,
  TARGET.target_restriction = SOURCE.target_restriction,
  TARGET.targeting_dimension = SOURCE.targeting_dimension,
  TARGET.targeting_value = SOURCE.targeting_value
WHEN NOT MATCHED THEN INSERT
(
  _airbyte_extracted_at,
  ad_group_optimized_targeting_enabled,
  ad_group_cpc_bid_rupees,
  ad_group_cpm_bid_rupees,
  ad_group_cpv_bid_rupees,
  ad_group_effective_target_cpa_rupees,
  ad_group_id,
  ad_group_percent_cpc_bid_micros,
  ad_group_target_cpa_rupees,
  ad_group_target_cpm_rupees,
  campaign_id,
  metrics_cost_rupees,
  ad_group_effective_target_roas,
  ad_group_target_roas,
  ad_group_ad_rotation_mode,
  target_restriction,
  targeting_dimension,
  targeting_value
)
VALUES
(
  SOURCE._airbyte_extracted_at,
  SOURCE.ad_group_optimized_targeting_enabled,
  SOURCE.ad_group_cpc_bid_rupees,
  SOURCE.ad_group_cpm_bid_rupees,
  SOURCE.ad_group_cpv_bid_rupees,
  SOURCE.ad_group_effective_target_cpa_rupees,
  SOURCE.ad_group_id,
  SOURCE.ad_group_percent_cpc_bid_micros,
  SOURCE.ad_group_target_cpa_rupees,
  SOURCE.ad_group_target_cpm_rupees,
  SOURCE.campaign_id,
  SOURCE.metrics_cost_rupees,
  SOURCE.ad_group_effective_target_roas,
  SOURCE.ad_group_target_roas,
  SOURCE.ad_group_ad_rotation_mode,
  SOURCE.target_restriction,
  SOURCE.targeting_dimension,
  SOURCE.targeting_value
)
