MERGE INTO `shopify-pubsub-project.Data_Warehouse_GoogleAds_Staging.campaign_criterion` AS TARGET
USING
(
select
_airbyte_extracted_at,
campaign_criterion_age_range_type,
campaign_criterion_campaign,
campaign_criterion_mobile_application_name,
campaign_criterion_negative,
campaign_criterion_resource_name,
campaign_criterion_youtube_channel_channel_id,
campaign_criterion_youtube_video_video_id,
campaign_id,
change_status_last_change_date_time,
deleted_at,
from
(
SELECT
  *,
  ROW_NUMBER() OVER (PARTITION BY campaign_id ORDER BY _airbyte_extracted_at DESC) AS row_num
FROM
shopify-pubsub-project.pilgrim_bi_google_ads.campaign_criterion
where
DATE(_airbyte_extracted_at) >= DATE_SUB(CURRENT_DATE("Asia/Kolkata"), INTERVAL 10 DAY)
)
WHERE row_num = 1
)
AS SOURCE
on target.campaign_id = source.campaign_id
WHEN MATCHED AND source._airbyte_extracted_at > target._airbyte_extracted_at
then update set
target._airbyte_extracted_at = source._airbyte_extracted_at,
target.campaign_criterion_age_range_type = source.campaign_criterion_age_range_type,
target.campaign_criterion_campaign = source.campaign_criterion_campaign,
target.campaign_criterion_mobile_application_name = source.campaign_criterion_mobile_application_name,
target.campaign_criterion_negative = source.campaign_criterion_negative,
target.campaign_criterion_resource_name = source.campaign_criterion_resource_name,
target.campaign_criterion_youtube_channel_channel_id = source.campaign_criterion_youtube_channel_channel_id,
target.campaign_criterion_youtube_video_video_id = source.campaign_criterion_youtube_video_video_id,
target.campaign_id = source.campaign_id,
target.change_status_last_change_date_time = source.change_status_last_change_date_time,
target.deleted_at = source.deleted_at
when not matched
then insert
(
_airbyte_extracted_at,
campaign_criterion_age_range_type,
campaign_criterion_campaign,
campaign_criterion_mobile_application_name,
campaign_criterion_negative,
campaign_criterion_resource_name,
campaign_criterion_youtube_channel_channel_id,
campaign_criterion_youtube_video_video_id,
campaign_id,
change_status_last_change_date_time,
deleted_at
)
values
(
source._airbyte_extracted_at,
source.campaign_criterion_age_range_type,
source.campaign_criterion_campaign,
source.campaign_criterion_mobile_application_name,
source.campaign_criterion_negative,
source.campaign_criterion_resource_name,
source.campaign_criterion_youtube_channel_channel_id,
source.campaign_criterion_youtube_video_video_id,
source.campaign_id,
source.change_status_last_change_date_time,
source.deleted_at
);