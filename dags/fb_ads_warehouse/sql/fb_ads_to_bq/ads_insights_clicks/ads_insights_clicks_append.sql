merge into `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.ads_insights_clicks` as target
using
(
SELECT
  _airbyte_extracted_at,
  campaign_id,
  adset_id,
  ad_id,
  date_start,
  JSON_EXTRACT_SCALAR(unique_outbound, '$.action_target_id') AS unique_outbound_target_id,
  JSON_EXTRACT_SCALAR(unique_outbound, '$.value') AS unique_outbound_clicks,
  JSON_EXTRACT_SCALAR(value, '$.action_destination') AS action_destination,
  JSON_EXTRACT_SCALAR(value, '$.action_target_id') AS action_target_id,
  JSON_EXTRACT_SCALAR(value, '$.action_type') AS action_type,
  JSON_EXTRACT_SCALAR(value, '$.value') AS click_value,
FROM
(
  select
  *,
  row_number() over(partition by ad_id, date_start order by _airbyte_extracted_at desc) as rn
  from shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads_insights,
  UNNEST(JSON_EXTRACT_ARRAY(outbound_clicks,'$')) AS value,
  UNNEST(JSON_EXTRACT_ARRAY(unique_outbound_clicks)) AS unique_outbound
)
where rn =1 and date(_airbyte_extracted_at) >= date_sub(current_date("Asia/Kolkata"), interval 10 day)
) as source
on target.ad_id = source.ad_id
and target.date_start = source.date_start
when matched and target._airbyte_extracted_at < source._airbyte_extracted_at
then update set
target._airbyte_extracted_at = source._airbyte_extracted_at,
target.campaign_id = source.campaign_id,
target.adset_id = source.adset_id,
target.ad_id = source.ad_id,
target.date_start = source.date_start,
target.unique_outbound_target_id = source.unique_outbound_target_id,
target.unique_outbound_clicks = source.unique_outbound_clicks,
target.action_destination = source.action_destination,
target.action_target_id = source.action_target_id,
target.action_type = source.action_type,
target.click_value = source.click_value
when not matched
then insert
(
  _airbyte_extracted_at,
  campaign_id,
  adset_id,
  ad_id,
  date_start,
  unique_outbound_target_id,
  unique_outbound_clicks,
  action_destination,
  action_target_id,
  action_type,
  click_value
)
values
(
source._airbyte_extracted_at,
source.campaign_id,
source.adset_id,
source.ad_id,
source.date_start,
source.unique_outbound_target_id,
source.unique_outbound_clicks,
source.action_destination,
source.action_target_id,
source.action_type,
source.click_value
)