MERGE INTO `shopify-pubsub-project.Data_Warehouse_GoogleAds_Staging.ad_group_ad_App_eng_ad_vids` as TARGET
USING
(
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
) AS SOURCE
ON TARGET.ad_group_ad_ad_id = SOURCE.ad_group_ad_ad_id
and TARGET.segments_date = SOURCE.segments_date
and TARGET.app_eng_ad_vids = SOURCE.app_eng_ad_vids
WHEN MATCHED AND SOURCE._airbyte_extracted_at > TARGET._airbyte_extracted_at
THEN UPDATE SET
TARGET.ad_group_ad_ad_id = SOURCE.ad_group_ad_ad_id,
TARGET.ad_group_id = SOURCE.ad_group_id,
TARGET.segments_date = SOURCE.segments_date,
TARGET._airbyte_extracted_at = SOURCE._airbyte_extracted_at,
TARGET.app_eng_ad_vids = SOURCE.app_eng_ad_vids
WHEN NOT MATCHED
THEN INSERT
(
  ad_group_ad_ad_id,
  ad_group_id,
  segments_date,
  _airbyte_extracted_at,
  app_eng_ad_vids
)
VALUES
(
  SOURCE.ad_group_ad_ad_id,
  SOURCE.ad_group_id,
  SOURCE.segments_date,
  SOURCE._airbyte_extracted_at,
  SOURCE.app_eng_ad_vids
)