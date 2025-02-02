CREATE OR REPLACE TABLE `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.ads_insights_action_carousel_card_cost_per_conversion`
PARTITION BY DATE_TRUNC(_airbyte_extracted_at, DAY)
AS
SELECT
  _airbyte_extracted_at,
  ad_id,
  adset_id,
  account_id,
  campaign_id,
  date_start,


  -- cost_per_conversion,
  JSON_EXTRACT_SCALAR(cost_per_conv, '$.28d_click') AS cost_per_conv_28d_click, --400(D)
  JSON_EXTRACT_SCALAR(cost_per_conv, '$.28d_view') AS cost_per_conv_28d_view,
  JSON_EXTRACT_SCALAR(cost_per_conv, '$.7d_click') AS cost_per_conv_7d_click,
  JSON_EXTRACT_SCALAR(cost_per_conv, '$.7d_click') AS cost_per_conv_7d_view,
  JSON_EXTRACT_SCALAR(cost_per_conv, '$.1d_click') AS cost_per_conv_1d_click,
  JSON_EXTRACT_SCALAR(cost_per_conv, '$.1d_view') AS cost_per_conv_1d_view,
  JSON_EXTRACT_SCALAR(cost_per_conv, '$.action_type') AS cost_per_conv_action_type,
  JSON_EXTRACT_SCALAR(cost_per_conv, '$.value') AS cost_per_conv_value,
  rn,

FROM
(
  select *,
  row_number() over(partition by ad_id,date_start,JSON_EXTRACT_SCALAR(cost_per_conv, '$.action_type') order by _airbyte_extracted_at desc) as rn,
  from shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads_insights_action_carousel_card,
  UNNEST(JSON_EXTRACT_ARRAY(cost_per_conversion)) AS cost_per_conv
)
where rn = 1
