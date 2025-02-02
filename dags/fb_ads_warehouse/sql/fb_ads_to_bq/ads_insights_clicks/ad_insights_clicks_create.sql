CREATE OR REPLACE TABLE `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.ads_insights_clicks`
PARTITION BY DATE_TRUNC(_airbyte_extracted_at, DAY)
AS
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
where rn =1
