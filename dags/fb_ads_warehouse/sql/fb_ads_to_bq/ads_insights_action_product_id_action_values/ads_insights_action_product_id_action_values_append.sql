merge into `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.ads_insights_action_product_id_action_values` as target
using
(
SELECT
  _airbyte_extracted_at,
  ad_id,
  date_start,
  adset_id,
  account_id,
  campaign_id,
  product_id,


  -- action_values,
  JSON_EXTRACT_SCALAR(act_values, '$.1d_click') AS act_values_1d_click,
  JSON_EXTRACT_SCALAR(act_values, '$.1d_view') AS act_values_1d_view,
  JSON_EXTRACT_SCALAR(act_values, '$.28d_click') AS act_values_28d_click,
  JSON_EXTRACT_SCALAR(act_values, '$.28d_views') AS act_values_28d_views,  
  JSON_EXTRACT_SCALAR(act_values, '$.7d_click') AS act_values_7d_click,
  JSON_EXTRACT_SCALAR(act_values, '$.7d_view') AS act_values_7d_view,
  JSON_EXTRACT_SCALAR(act_values, '$.action_type') AS act_values_action_type,
  JSON_EXTRACT_SCALAR(act_values, '$.value') AS act_values_value,
FROM
(
select
*,
row_number() over(partition by ad_id,date_start,product_id,JSON_EXTRACT_SCALAR(act_values, '$.action_type') order by _airbyte_extracted_at desc) as rn
from shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads_insights_action_product_id,
UNNEST(JSON_EXTRACT_ARRAY(action_values)) AS act_values
)
where rn = 1 and date(_airbyte_extracted_at) >= date_sub(current_date("Asia/Kolkata"), INTERVAL 10 day)
) as source
on target.ad_id = source.ad_id
and target.date_start = source.date_start
and target.product_id = source.product_id
and target.act_values_action_type = source.act_values_action_type
when matched and target.date_start < source.date_start
then update set
  target._airbyte_extracted_at = source._airbyte_extracted_at,
  target.ad_id = source.ad_id,
  target.date_start = source.date_start,
  target.adset_id = source.adset_id,
  target.account_id = source.account_id,
  target.campaign_id = source.campaign_id,
  target.product_id = source.product_id,
  target.act_values_1d_click = source.act_values_1d_click,
  target.act_values_1d_view = source.act_values_1d_view,
  target.act_values_28d_click = source.act_values_28d_click,
  target.act_values_28d_views = source.act_values_28d_views,  
  target.act_values_7d_click = source.act_values_7d_click,
  target.act_values_7d_view = source.act_values_7d_view,
  target.act_values_action_type = source.act_values_action_type,
  target.act_values_value = source.act_values_value
when not matched
then insert
(
_airbyte_extracted_at,
ad_id,
date_start,
adset_id,
account_id,
campaign_id,
product_id,
act_values_1d_click,
act_values_1d_view,
act_values_28d_click,
act_values_28d_views,
act_values_7d_click,
act_values_7d_view,
act_values_action_type,
act_values_value
)
values
(
source._airbyte_extracted_at,
source.ad_id,
source.date_start,
source.adset_id,
source.account_id,
source.campaign_id,
source.product_id,
source.act_values_1d_click,
source.act_values_1d_view,
source.act_values_28d_click,
source.act_values_28d_views,
source.act_values_7d_click,
source.act_values_7d_view,
source.act_values_action_type,
source.act_values_value
)