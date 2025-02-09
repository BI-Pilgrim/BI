MERGE INTO `shopify-pubsub-project.Data_Warehouse_GoogleAds_Staging.ad_group_ad_final_app_urls` as TARGET
USING
(
SELECT
  ad_group_ad_ad_id,
  ad_group_id,
  segments_date,
  _airbyte_extracted_at,
  REGEXP_EXTRACT(JSON_EXTRACT_SCALAR(url_data, '$'), r'os_type:\s*(\w+)') AS operating_sys,
  REGEXP_EXTRACT(JSON_EXTRACT_SCALAR(url_data, '$'), r'url:\s*\"([^\"]+)\"') AS url
FROM
(
select
*,
row_number() over(partition by ad_group_ad_ad_id,segments_date,REGEXP_EXTRACT(JSON_EXTRACT_SCALAR(url_data, '$'), r'url:\s*\"([^\"]+)\"') order by _airbyte_extracted_at desc) as rn
from `shopify-pubsub-project.pilgrim_bi_google_ads.ad_group_ad`,
UNNEST(JSON_EXTRACT_ARRAY(ad_group_ad_ad_final_app_urls)) AS url_data
)
where rn = 1 and REGEXP_EXTRACT(JSON_EXTRACT_SCALAR(url_data, '$'), r'os_type:\s*(\w+)') is not null
and REGEXP_EXTRACT(JSON_EXTRACT_SCALAR(url_data, '$'), r'url:\s*\"([^\"]+)\"') is not null
and DATE(_airbyte_extracted_at) >= DATE_SUB(CURRENT_DATE("Asia/Kolkata"), INTERVAL 10 DAY)
) AS SOURCE
ON TARGET.ad_group_ad_ad_id = SOURCE.ad_group_ad_ad_id
and TARGET.segments_date = SOURCE.segments_date
and TARGET.url = SOURCE.url
WHEN MATCHED AND SOURCE._airbyte_extracted_at > TARGET._airbyte_extracted_at
THEN UPDATE SET
TARGET.ad_group_ad_ad_id = SOURCE.ad_group_ad_ad_id,
TARGET.ad_group_id = SOURCE.ad_group_id,
TARGET.segments_date = SOURCE.segments_date,
TARGET._airbyte_extracted_at = SOURCE._airbyte_extracted_at,
TARGET.operating_sys = SOURCE.operating_sys,
TARGET.url = SOURCE.url
WHEN NOT MATCHED
THEN INSERT
(
  ad_group_ad_ad_id,
  ad_group_id,
  segments_date,
  _airbyte_extracted_at,
  operating_sys,
  url
)
VALUES
(
  SOURCE.ad_group_ad_ad_id,
  SOURCE.ad_group_id,
  SOURCE.segments_date,
  SOURCE._airbyte_extracted_at,
  SOURCE.operating_sys,
  SOURCE.url
)