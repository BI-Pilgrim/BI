merge into `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.ads_insights_action_carousel_card_video_play_actions` as target
using
(
SELECT
  _airbyte_extracted_at,
  ad_id,
  adset_id,
  account_id,
  campaign_id,
  date_start,
  JSON_EXTRACT_SCALAR(vid_play_action, '$.28d_click') AS vid_play_action_28d_click,
  JSON_EXTRACT_SCALAR(vid_play_action, '$.28d_view') AS vid_play_action_28d_view,
  JSON_EXTRACT_SCALAR(vid_play_action, '$.7d_click') AS vid_play_action_7d_click,
  JSON_EXTRACT_SCALAR(vid_play_action, '$.7d_click') AS vid_play_action_7d_view,
  JSON_EXTRACT_SCALAR(vid_play_action, '$.1d_click') AS vid_play_action_1d_click,
  JSON_EXTRACT_SCALAR(vid_play_action, '$.1d_view') AS vid_play_action_1d_view,
  JSON_EXTRACT_SCALAR(vid_play_action, '$.action_type') AS vid_play_action_action_type,
  JSON_EXTRACT_SCALAR(vid_play_action, '$.value') AS vid_play_action_value,

FROM
(
select
*,
row_number() over(partition by ad_id,date_start,JSON_EXTRACT_SCALAR(vid_play_action, '$.action_type') order by _airbyte_extracted_at desc) as rn
from shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads_insights_action_carousel_card,
UNNEST(JSON_EXTRACT_ARRAY(video_play_actions)) AS vid_play_action
)
where rn = 1 and date(_airbyte_extracted_at) >= date_sub(current_date("Asia/Kolkata"), INTERVAL 10 day)
) as source
on target.ad_id = source.ad_id
and target.date_start = source.date_start
and target.vid_play_action_action_type = source.vid_play_action_action_type
when matched and target._airbyte_extracted_at < source._airbyte_extracted_at
then update set
target._airbyte_extracted_at = source._airbyte_extracted_at,
target.ad_id = source.ad_id,
target.adset_id = source.adset_id,
target.account_id = source.account_id,
target.campaign_id = source.campaign_id,
target.date_start = source.date_start,
target.vid_play_action_28d_click = source.vid_play_action_28d_click,
target.vid_play_action_28d_view = source.vid_play_action_28d_view,
target.vid_play_action_7d_click = source.vid_play_action_7d_click,
target.vid_play_action_7d_view = source.vid_play_action_7d_view,
target.vid_play_action_1d_click = source.vid_play_action_1d_click,
target.vid_play_action_1d_view = source.vid_play_action_1d_view,
target.vid_play_action_action_type = source.vid_play_action_action_type,
target.vid_play_action_value = source.vid_play_action_value
when not matched
then insert
(
  _airbyte_extracted_at,
  ad_id,
  adset_id,
  account_id,
  campaign_id,
  date_start,
  vid_play_action_28d_click,
  vid_play_action_28d_view,
  vid_play_action_7d_click,
  vid_play_action_7d_view,
  vid_play_action_1d_click,
  vid_play_action_1d_view,
  vid_play_action_action_type,
  vid_play_action_value
)
values
(
  source._airbyte_extracted_at,
  source.ad_id,
  source.adset_id,
  source.account_id,
  source.campaign_id,
  source.date_start,
  source.vid_play_action_28d_click,
  source.vid_play_action_28d_view,
  source.vid_play_action_7d_click,
  source.vid_play_action_7d_view,
  source.vid_play_action_1d_click,
  source.vid_play_action_1d_view,
  source.vid_play_action_action_type,
  source.vid_play_action_value
)