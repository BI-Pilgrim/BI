CREATE OR REPLACE TABLE `shopify-pubsub-project.Data_Warehouse_GoogleAds_Staging.ad_group_ad_App_eng_ad_head`
PARTITION BY DATE_TRUNC(_airbyte_extracted_at, DAY)
AS
SELECT
  ad_group_ad_ad_id,
  ad_group_id,
  _airbyte_extracted_at,

  -- ad_group_ad_ad_app_engagement_ad_headlines,
  REGEXP_EXTRACT(JSON_EXTRACT_SCALAR(ABC, '$'), r'text: \"([^\"]+)\"') AS app_eng_ad_head
FROM
  `shopify-pubsub-project.pilgrim_bi_google_ads.ad_group_ad`,
  UNNEST(JSON_EXTRACT_ARRAY(ad_group_ad_ad_app_engagement_ad_headlines)) AS ABC;
