merge into `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.ads_insights_action_carousel_card_conversion_values` as target
using
(
SELECT
  _airbyte_extracted_at,
  ad_id,
  adset_id,
  account_id,
  campaign_id,
  date_start,
  date_stop,


  -- conversion_values,
  JSON_EXTRACT_SCALAR(conv_values, '$.1d_click') AS conv_values_1d_click, --400(B)
  JSON_EXTRACT_SCALAR(conv_values, '$.28d_click') AS conv_values_28d_click,
  JSON_EXTRACT_SCALAR(conv_values, '$.7d_click') AS conv_values_7d_click,
  JSON_EXTRACT_SCALAR(conv_values, '$.action_type') AS conv_values_action_type,
  JSON_EXTRACT_SCALAR(conv_values, '$.value') AS conv_values_value,

FROM
(
  select
  *,
  row_number() over(partition by ad_id,date_start,JSON_EXTRACT_SCALAR(conv_values, '$.action_type')  order by _airbyte_extracted_at desc) as rn,
  from shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads_insights_action_carousel_card,
  UNNEST(JSON_EXTRACT_ARRAY(conversion_values)) AS conv_values
)
where rn = 1  and date(_airbyte_extracted_at) >= date_sub(current_date("Asia/Kolkata"), interval 10 day)
) as source
on target.ad_id = source.ad_id
and target.date_start = source.date_start
and target.conv_values_action_type = source.conv_values_action_type
when matched and target._airbyte_extracted_at < source._airbyte_extracted_at
then update set
  target._airbyte_extracted_at = source._airbyte_extracted_at,
  target.ad_id = source.ad_id,
  target.adset_id = source.adset_id,
  target.account_id = source.account_id,
  target.campaign_id = source.campaign_id,
  target.date_start = source.date_start,
  target.date_stop = source.date_stop,
  target.conv_values_1d_click = source.conv_values_1d_click,
  target.conv_values_28d_click = source.conv_values_28d_click,
  target.conv_values_7d_click = source.conv_values_7d_click,
  target.conv_values_action_type = source.conv_values_action_type,
  target.conv_values_value = source.conv_values_value
when not matched
then insert
(
  _airbyte_extracted_at,
  ad_id,
  adset_id,
  account_id,
  campaign_id,
  date_start,
  date_stop,
  conv_values_1d_click,
  conv_values_28d_click,
  conv_values_7d_click,
  conv_values_action_type,
  conv_values_value
)
values
(
  source._airbyte_extracted_at,
  source.ad_id,
  source.adset_id,
  source.account_id,
  source.campaign_id,
  source.date_start,
  source.date_stop,
  source.conv_values_1d_click,
  source.conv_values_28d_click,
  source.conv_values_7d_click,
  source.conv_values_action_type,
  source.conv_values_value
)