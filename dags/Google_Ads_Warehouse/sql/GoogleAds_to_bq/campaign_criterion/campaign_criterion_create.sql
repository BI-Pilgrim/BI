create or replace table `shopify-pubsub-project.Data_Warehouse_GoogleAds_Staging.campaign_criterion`
partition by date_trunc(_airbyte_extracted_at, day)
As
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
select *,
row_number() over(partition by campaign_criterion_resource_name order by _airbyte_extracted_at desc) as rn
from shopify-pubsub-project.pilgrim_bi_google_ads.campaign_criterion
)
where rn = 1
