MERGE INTO `shopify-pubsub-project.Data_Warehouse_GoogleAds_Staging.ad_group_ad_App_ad_img` as TARGET
USING
(
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
) AS SOURCE
ON TARGET.ad_group_ad_ad_id = SOURCE.ad_group_ad_ad_id
and TARGET.segments_date = SOURCE.segments_date
and TARGET.ad_group_ad_ad_app_ad_img = SOURCE.ad_group_ad_ad_app_ad_img
WHEN MATCHED AND SOURCE._airbyte_extracted_at > TARGET._airbyte_extracted_at
THEN UPDATE SET
TARGET.ad_group_ad_ad_id = SOURCE.ad_group_ad_ad_id,
TARGET.ad_group_id = SOURCE.ad_group_id,
TARGET.segments_date = SOURCE.segments_date,
TARGET._airbyte_extracted_at = SOURCE._airbyte_extracted_at,
TARGET.ad_group_ad_ad_app_ad_img = SOURCE.ad_group_ad_ad_app_ad_img
WHEN NOT MATCHED
THEN INSERT
(
ad_group_ad_ad_id,
ad_group_id,
segments_date,
_airbyte_extracted_at,
ad_group_ad_ad_app_ad_img
)
VALUES
(
SOURCE.ad_group_ad_ad_id,
SOURCE.ad_group_id,
SOURCE.segments_date,
SOURCE._airbyte_extracted_at,
SOURCE.ad_group_ad_ad_app_ad_img
)