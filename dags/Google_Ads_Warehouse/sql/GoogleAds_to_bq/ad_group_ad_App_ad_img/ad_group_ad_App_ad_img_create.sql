CREATE OR REPLACE TABLE `shopify-pubsub-project.Data_Warehouse_GoogleAds_Staging.ad_group_ad_App_ad_img`
PARTITION BY DATE_TRUNC(_airbyte_extracted_at, DAY)
AS
SELECT
  ad_group_ad_ad_id,
  ad_group_id,
  _airbyte_extracted_at,

  -- ad_group_ad_ad_app_ad_images,
  REGEXP_EXTRACT(JSON_EXTRACT_SCALAR(xy, '$'),r'customers/\d+/assets/\d+') AS ad_group_ad_ad_app_ad_img,
FROM
  `shopify-pubsub-project.pilgrim_bi_google_ads.ad_group_ad`,
  unnest(json_extract_array(ad_group_ad_ad_app_ad_images)) as xy
