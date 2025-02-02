create or replace table `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.ad_creatives`
as
SELECT
_airbyte_extracted_at,
id,
body,
name,
title,
status,
actor_id,
adlabels,
link_url,
url_tags,
video_id,
image_url,
object_id,
account_id,
image_hash,
link_og_id,
object_url,
image_crops,
object_type,
template_url,
thumbnail_url,
product_set_id,
asset_feed_spec,
object_story_id,
applink_treatment,
object_story_spec,
template_url_spec,
instagram_actor_id,
instagram_story_id,
thumbnail_data_url,
call_to_action_type,
instagram_permalink_url,
effective_object_story_id,
source_instagram_media_id,
effective_instagram_story_id,
FROM
(
select
*,
row_number() over(partition by id order by _airbyte_extracted_at desc) as rn
from `shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ad_creatives`
)
where rn = 1


