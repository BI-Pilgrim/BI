MERGE INTO `shopify-pubsub-project.Data_Warehouse_GoogleAds_Staging.ad_group_ad_respnsive_display_ad_head` as TARGET
USING
(
SELECT
  ad_group_ad_ad_id,
  ad_group_id,
  segments_date,
  _airbyte_extracted_at,
  REGEXP_REPLACE(JSON_EXTRACT_SCALAR(ABC, '$'), r'text: \\"|\\n', '') AS responsive_display_ad_headlines,
FROM
(
select *,
row_number() over(partition by ad_group_ad_ad_id,segments_date,REGEXP_REPLACE(JSON_EXTRACT_SCALAR(ABC, '$'), r'text: \\"|\\n', '') order by _airbyte_extracted_at desc) as rn
from `shopify-pubsub-project.pilgrim_bi_google_ads.ad_group_ad`,
UNNEST(JSON_EXTRACT_ARRAY(ad_group_ad_ad_responsive_display_ad_headlines)) AS ABC
)
where rn = 1 and DATE(_airbyte_extracted_at) >= DATE_SUB(CURRENT_DATE("Asia/Kolkata"), INTERVAL 10 DAY)
) AS SOURCE
ON TARGET.ad_group_ad_ad_id = SOURCE.ad_group_ad_ad_id
and TARGET.segments_date = SOURCE.segments_date
and TARGET.responsive_display_ad_headlines = SOURCE.responsive_display_ad_headlines
WHEN MATCHED AND SOURCE._airbyte_extracted_at > TARGET._airbyte_extracted_at
THEN UPDATE SET
TARGET.ad_group_ad_ad_id = SOURCE.ad_group_ad_ad_id,
TARGET.ad_group_id = SOURCE.ad_group_id,
TARGET.segments_date = SOURCE.segments_date,
TARGET._airbyte_extracted_at = SOURCE._airbyte_extracted_at,
TARGET.responsive_display_ad_headlines = SOURCE.responsive_display_ad_headlines
WHEN NOT MATCHED
THEN INSERT
(
  ad_group_ad_ad_id,
  ad_group_id,
  segments_date,
  _airbyte_extracted_at,
  responsive_display_ad_headlines
)
VALUES
(
  SOURCE.ad_group_ad_ad_id,
  SOURCE.ad_group_id,
  SOURCE.segments_date,
  SOURCE._airbyte_extracted_at,
  SOURCE.responsive_display_ad_headlines
)