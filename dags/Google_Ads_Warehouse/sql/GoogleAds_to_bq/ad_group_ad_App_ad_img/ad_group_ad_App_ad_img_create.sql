CREATE OR REPLACE TABLE `shopify-pubsub-project.Data_Warehouse_GoogleAds_Staging.ad_group_ad_App_ad_img`
PARTITION BY DATE_TRUNC(_airbyte_extracted_at, DAY)
AS
SELECT
  ad_group_ad_ad_id,
  ad_group_id,
  segments_date,
  _airbyte_extracted_at,
  REGEXP_EXTRACT(JSON_EXTRACT_SCALAR(xy, '$'),r'customers/\d+/assets/\d+') AS ad_group_ad_ad_app_ad_img,
FROM
(
  select *,
  row_number() over(partition by ad_group_ad_ad_id,segments_date,REGEXP_EXTRACT(JSON_EXTRACT_SCALAR(xy, '$'), r'text: \"([^\"]+)\"') order by _airbyte_extracted_at desc) as rn,
  from `shopify-pubsub-project.pilgrim_bi_google_ads.ad_group_ad`,
  unnest(json_extract_array(ad_group_ad_ad_app_ad_images)) as xy
)
where rn = 1 and REGEXP_EXTRACT(JSON_EXTRACT_SCALAR(xy, '$'),r'customers/\d+/assets/\d+') is not null