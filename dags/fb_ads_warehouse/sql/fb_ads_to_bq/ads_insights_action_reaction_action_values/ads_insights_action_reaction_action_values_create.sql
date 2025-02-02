CREATE OR REPLACE TABLE `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.ads_insights_action_reaction_action_values`
AS
SELECT
    _airbyte_extracted_at,
    ad_id,
    date_start,
    adset_id,
    account_id,
    campaign_id,


    -- action_values,
    json_extract_scalar(act_values, '$.1d_click') AS action_values_1d_click,
    json_extract_scalar(act_values, '$.1d_view') AS action_values_1d_view,
    json_extract_scalar(act_values, '$.28d_click') AS action_values_28d_click,
    json_extract_scalar(act_values, '$.28d_view') AS action_values_28d_view,
    json_extract_scalar(act_values, '$.7d_click') AS action_values_7d_click,
    json_extract_scalar(act_values, '$.7d_view') AS action_values_7d_view,
    json_extract_scalar(act_values, '$.action_type') AS action_values_action_type,
    json_extract_scalar(act_values, '$.value') AS action_values_value,
FROM
(
select
*,
row_number() over(partition by ad_id,date_start,json_extract_scalar(act_values, '$.action_type') order by _airbyte_extracted_at desc) as rn
from shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads_insights_action_reaction,
UNNEST(JSON_EXTRACT_ARRAY(action_values)) AS act_values
)
where rn = 1
  
