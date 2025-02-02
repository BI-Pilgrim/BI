merge into `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.ads_insights_action_type_cost_per_unique_action_type` as target
using
(
SELECT
  _airbyte_extracted_at,
  ad_id,
  date_start,
  adset_id,
  campaign_id,

  -- cost_per_unique_action_type,
  JSON_EXTRACT_SCALAR(cpuat, '$.1d_click') AS cost_per_unique_action_type_1d_click,
  JSON_EXTRACT_SCALAR(cpuat, '$.1d_view') AS cost_per_unique_action_type_1d_view,
  JSON_EXTRACT_SCALAR(cpuat, '$.28d_click') AS cost_per_unique_action_type_28d_click,
  JSON_EXTRACT_SCALAR(cpuat, '$.28d_view') AS cost_per_unique_action_type_28d_view,
  JSON_EXTRACT_SCALAR(cpuat, '$.7d_click') AS cost_per_unique_action_type_7d_click,
  JSON_EXTRACT_SCALAR(cpuat, '$.7d_view') AS cost_per_unique_action_type_7d_view,
  JSON_EXTRACT_SCALAR(cpuat, '$.action_type') AS cost_per_unique_action_type_action_type,
  JSON_EXTRACT_SCALAR(cpuat, '$.value') AS cost_per_unique_action_type_value,
FROM
(
select
*,
row_number() over(partition by ad_id,date_start,JSON_EXTRACT_SCALAR(cpuat, '$.action_type') order by _airbyte_extracted_at desc) as rn
from shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads_insights_action_type,
UNNEST(JSON_EXTRACT_ARRAY(cost_per_unique_action_type)) AS cpuat
)
where rn = 1 and date(date_start) >= date_sub(current_date("Asia/Kolkata"), INTERVAL 10 day)
) as source
on target.ad_id = source.ad_id
and target.date_start = source.date_start
and target.cost_per_unique_action_type_action_type = source.cost_per_unique_action_type_action_type
when matched and target.date_start < source.date_start
then update set
target._airbyte_extracted_at = source._airbyte_extracted_at,
target.ad_id = source.ad_id,
target.date_start = source.date_start,
target.adset_id = source.adset_id,
target.campaign_id = source.campaign_id,
target.cost_per_unique_action_type_1d_click = source.cost_per_unique_action_type_1d_click,
target.cost_per_unique_action_type_1d_view = source.cost_per_unique_action_type_1d_view,
target.cost_per_unique_action_type_28d_click = source.cost_per_unique_action_type_28d_click,
target.cost_per_unique_action_type_28d_view = source.cost_per_unique_action_type_28d_view,
target.cost_per_unique_action_type_7d_click = source.cost_per_unique_action_type_7d_click,
target.cost_per_unique_action_type_7d_view = source.cost_per_unique_action_type_7d_view,
target.cost_per_unique_action_type_action_type = source.cost_per_unique_action_type_action_type,
target.cost_per_unique_action_type_value = source.cost_per_unique_action_type_value
when not matched
then insert
(
  _airbyte_extracted_at,
  ad_id,
  date_start,
  adset_id,
  campaign_id,
  cost_per_unique_action_type_1d_click,
  cost_per_unique_action_type_1d_view,
  cost_per_unique_action_type_28d_click,
  cost_per_unique_action_type_28d_view,
  cost_per_unique_action_type_7d_click,
  cost_per_unique_action_type_7d_view,
  cost_per_unique_action_type_action_type,
  cost_per_unique_action_type_value
)
values
(
source._airbyte_extracted_at,
source.ad_id,
source.date_start,
source.adset_id,
source.campaign_id,
source.cost_per_unique_action_type_1d_click,
source.cost_per_unique_action_type_1d_view,
source.cost_per_unique_action_type_28d_click,
source.cost_per_unique_action_type_28d_view,
source.cost_per_unique_action_type_7d_click,
source.cost_per_unique_action_type_7d_view,
source.cost_per_unique_action_type_action_type,
source.cost_per_unique_action_type_value
)