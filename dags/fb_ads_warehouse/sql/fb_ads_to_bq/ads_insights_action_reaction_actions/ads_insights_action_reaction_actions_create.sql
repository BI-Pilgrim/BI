CREATE OR REPLACE TABLE `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.ads_insights_action_reaction_actions`
AS
SELECT
    ad_id,
    date_start,
    adset_id,
    account_id,
    campaign_id,


    -- actions,
    sum(cast(json_extract_scalar(acts, '$.1d_click') as float64)) as actions_1d_click,
    sum(cast(json_extract_scalar(acts, '$.1d_view') as float64)) as actions_1d_view,
    sum(cast(json_extract_scalar(acts, '$.28d_click') as float64)) as actions_28d_click,
    sum(cast(json_extract_scalar(acts, '$.28d_view') as float64)) as actions_28d_view,
    sum(cast(json_extract_scalar(acts, '$.7d_click') as float64)) as actions_7d_click,
    sum(cast(json_extract_scalar(acts, '$.7d_view') as float64)) as actions_7d_view,
    json_extract_scalar(acts, '$.action_type') as actions_action_type,
    sum(cast(json_extract_scalar(acts, '$.value') as int)) as actions_value,
FROM
(
select
*,
row_number() over(partition by ad_id,date_start,json_extract_scalar(acts, '$.action_type') order by _airbyte_extracted_at desc) as rn
from shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads_insights_action_reaction,
UNNEST(JSON_EXTRACT_ARRAY(actions)) AS acts
)
group by all