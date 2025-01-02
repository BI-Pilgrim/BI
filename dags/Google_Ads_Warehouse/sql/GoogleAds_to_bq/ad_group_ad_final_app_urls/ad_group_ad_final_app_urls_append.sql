MERGE INTO `shopify-pubsub-project.Data_Warehouse_GoogleAds_Staging.ad_group_ad_final_app_urls` as TARGET
USING
(
  SELECT
    ad_group_ad_ad_id,
    ad_group_id,
    _airbyte_extracted_at,

   -- ad_group_ad_ad_final_app_urls,
      REGEXP_EXTRACT(JSON_EXTRACT_SCALAR(url_data, '$'), r'os_type:\s*(\w+)') AS operating_sys,
      REGEXP_EXTRACT(JSON_EXTRACT_SCALAR(url_data, '$'), r'url:\s*\"([^\"]+)\"') AS url

  FROM
  (
    SELECT
      *,
      ROW_NUMBER() OVER(PARTITION BY ad_group_id ORDER BY ad_group_id DESC) AS ROW_NUM
    FROM
   `shopify-pubsub-project.pilgrim_bi_google_ads.ad_group_ad`,
   UNNEST(JSON_EXTRACT_ARRAY(ad_group_ad_ad_final_app_urls)) AS url_data
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
TARGET.operating_sys = SOURCE.operating_sys,
TARGET.url = SOURCE.url
WHEN NOT MATCHED
THEN INSERT
(
  ad_group_ad_ad_id,
  ad_group_id,
  _airbyte_extracted_at,
  operating_sys,
  url
)
VALUES
(
  SOURCE.ad_group_ad_ad_id,
  SOURCE.ad_group_id,
  SOURCE._airbyte_extracted_at,
  SOURCE.operating_sys,
  SOURCE.url
)