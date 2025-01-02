MERGE INTO `shopify-pubsub-project.Data_Warehouse_GoogleAds_Staging.ad_group_ad_App_ad_yt_vids` as TARGET
USING
(
  SELECT
    ad_group_ad_ad_id,
    ad_group_id,
    _airbyte_extracted_at,

    -- ad_group_ad_ad_app_ad_youtube_videos,
    REGEXP_EXTRACT(JSON_EXTRACT_SCALAR(xy, '$'),r'customers/\d+/assets/\d+') AS ad_group_ad_ad_app_ad_youtube_vids,

  FROM
  (
    SELECT
      *,
      ROW_NUMBER() OVER(PARTITION BY ad_group_id ORDER BY ad_group_id DESC) AS ROW_NUM
    FROM
      `shopify-pubsub-project.pilgrim_bi_google_ads.ad_group_ad`,
      unnest(json_extract_array(ad_group_ad_ad_app_ad_youtube_videos)) as xy
  )
  WHERE
    ROW_NUM = 1
) AS SOURCE
ON TARGET.ad_group_ad_ad_id = SOURCE.ad_group_ad_ad_id
WHEN MATCHED AND SOURCE._airbyte_extracted_at > TARGET._airbyte_extracted_at
THEN UPDATE SET
TARGET.ad_group_ad_ad_id = SOURCE.ad_group_ad_ad_id,
TARGET.ad_group_id = SOURCE.ad_group_id,
TARGET._airbyte_extracted_at = SOURCE._airbyte_extracted_at,
TARGET.ad_group_ad_ad_app_ad_youtube_vids = SOURCE.ad_group_ad_ad_app_ad_youtube_vids
WHEN NOT MATCHED
THEN INSERT
(
  ad_group_ad_ad_id,
  ad_group_id,
  _airbyte_extracted_at,
  ad_group_ad_ad_app_ad_youtube_vids
)
VALUES
(
  SOURCE.ad_group_ad_ad_id,
  SOURCE.ad_group_id,
  SOURCE._airbyte_extracted_at,
  SOURCE.ad_group_ad_ad_app_ad_youtube_vids
)