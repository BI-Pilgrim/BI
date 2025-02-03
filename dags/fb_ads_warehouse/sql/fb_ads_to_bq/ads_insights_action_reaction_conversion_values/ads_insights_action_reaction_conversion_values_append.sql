merge into `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.ads_insights_action_reaction_conversion_values` as target
using
(
SELECT
    _airbyte_extracted_at,
    ad_id,
    date_start,
    adset_id,
    account_id,
    campaign_id,


    -- conversion_values,
    json_extract_scalar(con_val, '$.1d_click') AS conversion_values_1d_click,
    json_extract_scalar(con_val, '$.1d_view') AS conversion_values_1d_view,
    json_extract_scalar(con_val, '$.28d_click') AS conversion_values_28d_click,
    json_extract_scalar(con_val, '$.28d_view') AS conversion_values_28d_view,
    json_extract_scalar(con_val, '$.7d_click') AS conversion_values_7d_click,
    json_extract_scalar(con_val, '$.7d_view') AS conversion_values_7d_view,
    json_extract_scalar(con_val, '$.action_type') AS conversion_values_action_type,
    json_extract_scalar(con_val, '$.value') AS conversion_values_value,
FROM
(
select
*,
row_number() over(partition by ad_id,date_start,json_extract_scalar(con_val, '$.action_type') order by _airbyte_extracted_at desc) as rn
from shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads_insights_action_reaction,
UNNEST(JSON_EXTRACT_ARRAY(conversion_values)) AS con_val
)
where rn = 1 and date(date_start) >= date_sub(current_date("Asia/Kolkata"), INTERVAL 10 day)
) as source
on target.ad_id = source.ad_id
and target.date_start = source.date_start
and target.conversion_values_action_type = source.conversion_values_action_type
when matched and target.date_start < source.date_start
then update set
target._airbyte_extracted_at = source._airbyte_extracted_at,
target.ad_id = source.ad_id,
target.date_start = source.date_start,
target.adset_id = source.adset_id,
target.account_id = source.account_id,
target.campaign_id = source.campaign_id,
target.conversion_values_1d_click = source.conversion_values_1d_click,
target.conversion_values_1d_view = source.conversion_values_1d_view,
target.conversion_values_28d_click = source.conversion_values_28d_click,
target.conversion_values_28d_view = source.conversion_values_28d_view,
target.conversion_values_7d_click = source.conversion_values_7d_click,
target.conversion_values_7d_view = source.conversion_values_7d_view,
target.conversion_values_action_type = source.conversion_values_action_type,
target.conversion_values_value = source.conversion_values_value
when not matched
then insert
(
_airbyte_extracted_at,
ad_id,
date_start,
adset_id,
account_id,
campaign_id,
conversion_values_1d_click,
conversion_values_1d_view,
conversion_values_28d_click,
conversion_values_28d_view,
conversion_values_7d_click,
conversion_values_7d_view,
conversion_values_action_type,
conversion_values_value
)
values
(
source._airbyte_extracted_at,
source.ad_id,
source.date_start,
source.adset_id,
source.account_id,
source.campaign_id,
source.conversion_values_1d_click,
source.conversion_values_1d_view,
source.conversion_values_28d_click,
source.conversion_values_28d_view,
source.conversion_values_7d_click,
source.conversion_values_7d_view,
source.conversion_values_action_type,
source.conversion_values_value
)