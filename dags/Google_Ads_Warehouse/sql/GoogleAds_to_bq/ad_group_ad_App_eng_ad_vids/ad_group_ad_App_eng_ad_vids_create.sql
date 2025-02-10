CREATE OR REPLACE TABLE `shopify-pubsub-project.Data_Warehouse_GoogleAds_Staging.ad_group_ad_App_eng_ad_vids`
PARTITION BY DATE_TRUNC(_airbyte_extracted_at, DAY)
AS
SELECT
  ad_group_ad_ad_id,
  ad_group_id,
  segments_date,
  _airbyte_extracted_at,
  REGEXP_EXTRACT(JSON_EXTRACT_SCALAR(xy, '$'),r'customers/\d+/assets/\d+') AS app_eng_ad_vids,
FROM
(
SELECT *,
ROW_NUMBER() OVER(PARTITION BY ad_group_ad_ad_id,segments_date,REGEXP_EXTRACT(JSON_EXTRACT_SCALAR(xy, '$'), r'text: \"([^\"]+)\"') ORDER BY _airbyte_extracted_at DESC) AS RN,
FROM `shopify-pubsub-project.pilgrim_bi_google_ads.ad_group_ad`,
unnest(json_extract_array(ad_group_ad_ad_app_engagement_ad_videos)) as xy
)
WHERE RN = 1 AND REGEXP_EXTRACT(JSON_EXTRACT_SCALAR(xy, '$'),r'customers/\d+/assets/\d+') IS NOT NULL