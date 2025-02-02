CREATE OR REPLACE TABLE `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.ads_insights_action_reaction_unique_actions`
AS
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
where rn = 1
