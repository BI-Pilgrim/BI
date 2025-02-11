CREATE OR REPLACE TABLE `shopify-pubsub-project.Data_Warehouse_GoogleAds_Staging.ad_group_ad_vid_resp_ad_call_to_actions`
AS
SELECT
  ad_group_ad_ad_id,
  ad_group_id,
  segments_date,
  _airbyte_extracted_at,
  REGEXP_EXTRACT(JSON_EXTRACT_SCALAR(action, '$'), r'text: \"([^\"]+)\"') AS video_responsive_ad_call_to_actions
FROM
(
select *,
row_number() over(partition by ad_group_ad_ad_id,segments_date,REGEXP_EXTRACT(JSON_EXTRACT_SCALAR(action, '$'), r'text: \"([^\"]+)\"') order by _airbyte_extracted_at desc) as rn
from `shopify-pubsub-project.pilgrim_bi_google_ads.ad_group_ad`,
  UNNEST(JSON_EXTRACT_ARRAY(ad_group_ad_ad_video_responsive_ad_call_to_actions)) AS action
)
where rn = 1 and REGEXP_EXTRACT(JSON_EXTRACT_SCALAR(action, '$'), r'text: \"([^\"]+)\"') is not null
