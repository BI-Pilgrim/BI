MERGE INTO `shopify-pubsub-project.Data_Warehouse_GoogleAds_Staging.ad_group_ad_rresponsive_ad_long_head` as TARGET
USING
(
SELECT
  ad_group_ad_ad_id,
  ad_group_id,
  segments_date,
  _airbyte_extracted_at,
  REGEXP_EXTRACT(JSON_EXTRACT_SCALAR(ABC, '$'), r'text: \"([^\"]+)\"') AS responsive_ad_long_headlines
FROM
(
select *,
row_number() over(partition by ad_group_ad_ad_id,segments_date,REGEXP_EXTRACT(JSON_EXTRACT_SCALAR(ABC, '$'), r'text: \"([^\"]+)\"') order by _airbyte_extracted_at desc) as rn
from `shopify-pubsub-project.pilgrim_bi_google_ads.ad_group_ad`,
UNNEST(JSON_EXTRACT_ARRAY(ad_group_ad_ad_video_responsive_ad_long_headlines)) AS ABC
)
where rn = 1 and REGEXP_EXTRACT(JSON_EXTRACT_SCALAR(ABC, '$'), r'text: \"([^\"]+)\"') is not null and DATE(_airbyte_extracted_at) >= DATE_SUB(CURRENT_DATE("Asia/Kolkata"), INTERVAL 10 DAY)
) AS SOURCE
ON TARGET.ad_group_ad_ad_id = SOURCE.ad_group_ad_ad_id
and TARGET.segments_date = SOURCE.segments_date
and TARGET.responsive_ad_long_headlines = SOURCE.responsive_ad_long_headlines
WHEN MATCHED AND SOURCE._airbyte_extracted_at > TARGET._airbyte_extracted_at
THEN UPDATE SET
TARGET.ad_group_ad_ad_id = SOURCE.ad_group_ad_ad_id,
TARGET.ad_group_id = SOURCE.ad_group_id,
TARGET._airbyte_extracted_at = SOURCE._airbyte_extracted_at,
TARGET.responsive_ad_long_headlines = SOURCE.responsive_ad_long_headlines
WHEN NOT MATCHED
THEN INSERT
(
  ad_group_ad_ad_id,
  ad_group_id,
  _airbyte_extracted_at,
  responsive_ad_long_headlines
)
VALUES
(
  SOURCE.ad_group_ad_ad_id,
  SOURCE.ad_group_id,
  SOURCE._airbyte_extracted_at,
  SOURCE.responsive_ad_long_headlines
)