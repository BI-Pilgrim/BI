CREATE OR REPLACE TABLE `shopify-pubsub-project.Data_Warehouse_GoogleAds_Staging.ad_group_ad_video_responsive_ad_descriptions`
PARTITION BY DATE_TRUNC(_airbyte_extracted_at, DAY)
AS
SELECT
  ad_group_ad_ad_id,
  ad_group_id,
  segments_date,
  _airbyte_extracted_at,
  case
  when REGEXP_EXTRACT(JSON_EXTRACT_SCALAR(ABC, '$'), r'text: \"([^\"]+)\"') is null then REGEXP_EXTRACT(JSON_EXTRACT_SCALAR(ABC, '$'), r'\"text\":\s*\"([^\"]+)\"')
  when REGEXP_EXTRACT(JSON_EXTRACT_SCALAR(ABC, '$'), r'\"text\":\s*\"([^\"]+)\"') is null then REGEXP_EXTRACT(JSON_EXTRACT_SCALAR(ABC, '$'), r'text: \"([^\"]+)\"')
  end AS video_responsive_ad_descriptions
FROM
(
select *,
row_number() over(partition by ad_group_ad_ad_id,segments_date,REGEXP_EXTRACT(JSON_EXTRACT_SCALAR(ABC, '$'), r'text: \"([^\"]+)\"') order by _airbyte_extracted_at desc) as rn
from `shopify-pubsub-project.pilgrim_bi_google_ads.ad_group_ad`,
UNNEST(JSON_EXTRACT_ARRAY(ad_group_ad_ad_video_responsive_ad_descriptions)) AS ABC
)
where (rn = 1 and REGEXP_EXTRACT(JSON_EXTRACT_SCALAR(ABC, '$'), r'text: \"([^\"]+)\"') is not null)
or (rn = 1 and REGEXP_EXTRACT(JSON_EXTRACT_SCALAR(ABC, '$'), r'\"text\":\s*\"([^\"]+)\"') is not null)
