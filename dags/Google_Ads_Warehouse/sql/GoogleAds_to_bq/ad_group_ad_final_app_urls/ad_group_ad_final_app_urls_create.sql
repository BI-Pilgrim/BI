CREATE OR REPLACE TABLE `shopify-pubsub-project.Data_Warehouse_GoogleAds_Staging.ad_group_ad_final_app_urls`
PARTITION BY DATE_TRUNC(_airbyte_extracted_at, DAY)
AS
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