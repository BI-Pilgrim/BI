MERGE INTO `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.ads_insights_action_carousel_card_video_play_actions` AS TARGET
USING
(
  SELECT
    _airbyte_extracted_at,
    ad_id,
    adset_id,
    account_id,
    campaign_id,
   
    -- video_play_actions,
    JSON_EXTRACT_SCALAR(vid_play_action, '$.28d_click') AS vid_play_action_28d_click, -- 200(C)
    JSON_EXTRACT_SCALAR(vid_play_action, '$.28d_view') AS vid_play_action_28d_view,
    JSON_EXTRACT_SCALAR(vid_play_action, '$.7d_click') AS vid_play_action_7d_click,
    JSON_EXTRACT_SCALAR(vid_play_action, '$.7d_click') AS vid_play_action_7d_view,
    JSON_EXTRACT_SCALAR(vid_play_action, '$.1d_click') AS vid_play_action_1d_click,
    JSON_EXTRACT_SCALAR(vid_play_action, '$.1d_view') AS vid_play_action_1d_view,
    JSON_EXTRACT_SCALAR(vid_play_action, '$.action_type') AS vid_play_action_action_type,
    JSON_EXTRACT_SCALAR(vid_play_action, '$.value') AS vid_play_action_value,


    FROM
    (
      SELECT
        *,
        ROW_NUMBER() OVER(PARTITION BY adset_id ORDER BY _airbyte_extracted_at) AS row_num
      FROM
        shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads_insights_action_carousel_card,
        UNNEST(JSON_EXTRACT_ARRAY(video_play_actions)) AS vid_play_action
      WHERE
        DATE(_airbyte_extracted_at) > DATE_SUB(CURRENT_DATE("Asia/Kolkata"), INTERVAL 10 DAY)
    )
    WHERE row_nuM = 1  
) AS SOURCE


ON TARGET.ad_id = SOURCE.ad_id
WHEN MATCHED AND TARGET._airbyte_extracted_at < SOURCE._airbyte_extracted_at
THEN UPDATE SET
  TARGET._airbyte_extracted_at = SOURCE._airbyte_extracted_at,
  TARGET.ad_id = SOURCE.ad_id,
  TARGET.adset_id = SOURCE.adset_id,
  TARGET.account_id = SOURCE.account_id,
  TARGET.campaign_id = SOURCE.campaign_id,
  TARGET.vid_play_action_28d_click = SOURCE.vid_play_action_28d_click,
  TARGET.vid_play_action_28d_view = SOURCE.vid_play_action_28d_view,
  TARGET.vid_play_action_7d_click = SOURCE.vid_play_action_7d_click,
  TARGET.vid_play_action_7d_view = SOURCE.vid_play_action_7d_view,
  TARGET.vid_play_action_1d_click = SOURCE.vid_play_action_1d_click,
  TARGET.vid_play_action_1d_view = SOURCE.vid_play_action_1d_view,
  TARGET.vid_play_action_action_type = SOURCE.vid_play_action_action_type,
  TARGET.vid_play_action_value = SOURCE.vid_play_action_value
WHEN NOT MATCHED
THEN INSERT
(
  _airbyte_extracted_at,
  ad_id,
  adset_id,
  account_id,
  campaign_id,
  vid_play_action_28d_click,
  vid_play_action_28d_view,
  vid_play_action_7d_click,
  vid_play_action_7d_view,
  vid_play_action_1d_click,
  vid_play_action_1d_view,
  vid_play_action_action_type,
  vid_play_action_value
)
VALUES
(
  SOURCE._airbyte_extracted_at,
  SOURCE.ad_id,
  SOURCE.adset_id,
  SOURCE.account_id,
  SOURCE.campaign_id,
  SOURCE.vid_play_action_28d_click,
  SOURCE.vid_play_action_28d_view,
  SOURCE.vid_play_action_7d_click,
  SOURCE.vid_play_action_7d_view,
  SOURCE.vid_play_action_1d_click,
  SOURCE.vid_play_action_1d_view,
  SOURCE.vid_play_action_action_type,
  SOURCE.vid_play_action_value
)
