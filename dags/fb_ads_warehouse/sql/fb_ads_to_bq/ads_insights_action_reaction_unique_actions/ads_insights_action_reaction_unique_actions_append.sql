merge into `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.ads_insights_action_reaction_unique_actions` as target
using
(
SELECT
    _airbyte_extracted_at,
    ad_id,
    date_start,
    adset_id,
    account_id,
    campaign_id,


    -- unique_actions,
    json_extract_scalar(unique_acts, '$.1d_click') AS unique_actions_1d_click,
    json_extract_scalar(unique_acts, '$.1d_view') AS unique_actions_1d_view,
    json_extract_scalar(unique_acts, '$.28d_click') AS unique_actions_28d_click,
    json_extract_scalar(unique_acts, '$.28d_view') AS unique_actions_28d_view,
    json_extract_scalar(unique_acts, '$.7d_click') AS unique_actions_7d_click,
    json_extract_scalar(unique_acts, '$.7d_view') AS unique_actions_7d_view,
    json_extract_scalar(unique_acts, '$.action_type') AS unique_actions_action_type,
    json_extract_scalar(unique_acts, '$.value') AS unique_actions_value,
FROM
(
select
*,
row_number() over(partition by ad_id, date_start, json_extract_scalar(unique_acts, '$.action_type') order by _airbyte_extracted_at desc) as rn
from shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads_insights_action_reaction,
UNNEST(JSON_EXTRACT_ARRAY(unique_actions)) AS unique_acts
)
where rn = 1 and date(date_start) >= date_sub(current_date("Asia/Kolkata"), INTERVAL 10 day)
) as source
on target.ad_id = source.ad_id
and target.date_start = source.date_start
and target.unique_actions_action_type = source.unique_actions_action_type
when matched and target.date_start < source.date_start
then update set
target._airbyte_extracted_at = source._airbyte_extracted_at,
target.ad_id = source.ad_id,
target.date_start = source.date_start,
target.adset_id = source.adset_id,
target.account_id = source.account_id,
target.campaign_id = source.campaign_id,
target.unique_actions_1d_click = source.unique_actions_1d_click,
target.unique_actions_1d_view = source.unique_actions_1d_view,
target.unique_actions_28d_click = source.unique_actions_28d_click,
target.unique_actions_28d_view = source.unique_actions_28d_view,
target.unique_actions_7d_click = source.unique_actions_7d_click,
target.unique_actions_7d_view = source.unique_actions_7d_view,
target.unique_actions_action_type = source.unique_actions_action_type,
target.unique_actions_value = source.unique_actions_value
when not matched
then insert
(
  _airbyte_extracted_at,
  ad_id,
  date_start,
  adset_id,
  account_id,
  campaign_id,
  unique_actions_1d_click,
  unique_actions_1d_view,
  unique_actions_28d_click,
  unique_actions_28d_view,
  unique_actions_7d_click,
  unique_actions_7d_view,
  unique_actions_action_type,
  unique_actions_value
)
values
(
source._airbyte_extracted_at,
source.ad_id,
source.date_start,
source.adset_id,
source.account_id,
source.campaign_id,
source.unique_actions_1d_click,
source.unique_actions_1d_view,
source.unique_actions_28d_click,
source.unique_actions_28d_view,
source.unique_actions_7d_click,
source.unique_actions_7d_view,
source.unique_actions_action_type,
source.unique_actions_value
)