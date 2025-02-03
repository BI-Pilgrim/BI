merge into `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.ads_insights_action_carousel_card_unique_actions_n_conversions_n_web_ctr` as target
using
(
SELECT
  _airbyte_extracted_at,
  ad_id,
  adset_id,
  account_id,
  campaign_id,
  date_start,
  JSON_EXTRACT_SCALAR(CON1, '$.28d_click') AS conversions_28d_click, --30K(A)
  JSON_EXTRACT_SCALAR(CON1, '$.28d_view') AS conversions_28d_view,
  JSON_EXTRACT_SCALAR(CON1, '$.7d_click') AS conversions_7d_click,
  JSON_EXTRACT_SCALAR(CON1, '$.7d_click') AS conversions_7d_view,
  JSON_EXTRACT_SCALAR(CON1, '$.1d_click') AS conversions_1d_click,
  JSON_EXTRACT_SCALAR(CON1, '$.1d_view') AS conversions_1d_view,
  JSON_EXTRACT_SCALAR(CON1, '$.action_type') AS conversions_action_type,
  JSON_EXTRACT_SCALAR(CON1, '$.value') AS conversions_value,
  JSON_EXTRACT_SCALAR(JSON_EXTRACT(conversions, '$[0]'), '$.action_type') AS action_type, --30K(A)
  CAST(JSON_EXTRACT_SCALAR(JSON_EXTRACT(conversions, '$[0]'), '$.value') AS FLOAT64) AS value,
  JSON_EXTRACT_SCALAR(unique_act, '$.1d_click') AS unique_act_1d_click, --30K(A)
  JSON_EXTRACT_SCALAR(unique_act, '$.28d_click') AS unique_act_28d_click,
  JSON_EXTRACT_SCALAR(unique_act, '$.7d_click') AS unique_act_7d_click,
  JSON_EXTRACT_SCALAR(unique_act, '$.action_type') AS unique_act_action_type,
  JSON_EXTRACT_SCALAR(unique_act, '$.value') AS unique_act_value,
FROM
(
select
*,
row_number() over(partition by ad_id,date_start,JSON_EXTRACT_SCALAR(CON1, '$.action_type'),JSON_EXTRACT_SCALAR(unique_act, '$.action_type') order by _airbyte_extracted_at desc) as rn
from shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads_insights_action_carousel_card,
UNNEST(JSON_EXTRACT_ARRAY(conversions)) AS CON1,
UNNEST(JSON_EXTRACT_ARRAY(unique_actions)) AS unique_act
)
where rn = 1 and date(_airbyte_extracted_at) >= date_sub(current_date("Asia/Kolkata"), INTERVAL 10 day)
) as source
on target.ad_id = source.ad_id
and target.date_start = source.date_start
and target.conversions_action_type = source.conversions_action_type
and target.unique_act_action_type = source.unique_act_action_type
when matched and target._airbyte_extracted_at < source._airbyte_extracted_at
then update set
target._airbyte_extracted_at = source._airbyte_extracted_at,
target.ad_id = source.ad_id,
target.adset_id = source.adset_id,
target.account_id = source.account_id,
target.campaign_id = source.campaign_id,
target.date_start = source.date_start,
target.conversions_28d_click = source.conversions_28d_click,
target.conversions_28d_view = source.conversions_28d_view,
target.conversions_7d_click = source.conversions_7d_click,
target.conversions_7d_view = source.conversions_7d_view,
target.conversions_1d_click = source.conversions_1d_click,
target.conversions_1d_view = source.conversions_1d_view,
target.conversions_action_type = source.conversions_action_type,
target.conversions_value = source.conversions_value,
target.action_type = source.action_type,
target.value = source.value,
target.unique_act_1d_click = source.unique_act_1d_click,
target.unique_act_28d_click = source.unique_act_28d_click,
target.unique_act_7d_click = source.unique_act_7d_click,
target.unique_act_action_type = source.unique_act_action_type,
target.unique_act_value = source.unique_act_value
when not matched
then insert
(
  _airbyte_extracted_at,
  ad_id,
  adset_id,
  account_id,
  campaign_id,
  date_start,
  conversions_28d_click,
  conversions_28d_view,
  conversions_7d_click,
  conversions_7d_view,
  conversions_1d_click,
  conversions_1d_view,
  conversions_action_type,
  conversions_value,
  action_type, 
  value,
  unique_act_1d_click, 
  unique_act_28d_click,
  unique_act_7d_click,
  unique_act_action_type,
  unique_act_value
)
values
(
source._airbyte_extracted_at,
source.ad_id,
source.adset_id,
source.account_id,
source.campaign_id,
source.date_start,
source.conversions_28d_click,
source.conversions_28d_view,
source.conversions_7d_click,
source.conversions_7d_view,
source.conversions_1d_click,
source.conversions_1d_view,
source.conversions_action_type,
source.conversions_value,
source.action_type,
source.value,
source.unique_act_1d_click,
source.unique_act_28d_click,
source.unique_act_7d_click,
source.unique_act_action_type,
source.unique_act_value
)