merge into `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.ads_insights_action_carousel_card_mobile_app_purchase_roas` as target
using
(
SELECT
  _airbyte_extracted_at,
  ad_id,
  adset_id,
  account_id,
  campaign_id,
  date_start,


  -- mobile_app_purchase_roas,
  JSON_EXTRACT_SCALAR(mob_app_purchase_roas, '$.28d_click') AS mob_app_purchase_roas_28d_click, --11K(E)
  JSON_EXTRACT_SCALAR(mob_app_purchase_roas, '$.28d_view') AS mob_app_purchase_roas_28d_view,
  JSON_EXTRACT_SCALAR(mob_app_purchase_roas, '$.7d_click') AS mob_app_purchase_roas_7d_click,
  JSON_EXTRACT_SCALAR(mob_app_purchase_roas, '$.7d_click') AS mob_app_purchase_roas_7d_view,
  JSON_EXTRACT_SCALAR(mob_app_purchase_roas, '$.1d_click') AS mob_app_purchase_roas_1d_click,
  JSON_EXTRACT_SCALAR(mob_app_purchase_roas, '$.1d_view') AS mob_app_purchase_roas_1d_view,
  JSON_EXTRACT_SCALAR(mob_app_purchase_roas, '$.action_type') AS mob_app_purchase_roas_action_type,
  JSON_EXTRACT_SCALAR(mob_app_purchase_roas, '$.value') AS mob_app_purchase_roas_value,

FROM
(
  select *,
  row_number() over(partition by ad_id,date_start,JSON_EXTRACT_SCALAR(mob_app_purchase_roas, '$.action_type') order by _airbyte_extracted_at desc) as rn,
  from shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads_insights_action_carousel_card,
  UNNEST(JSON_EXTRACT_ARRAY(mobile_app_purchase_roas)) AS mob_app_purchase_roas
)
where rn = 1 and date(created_time) >= date_sub(current_date("Asia/Kolkata"), INTERVAL 10 day)
) as source
on target.ad_id = source.ad_id
and target.date_start = source.date_start
and target.mob_app_purchase_roas_action_type = source.mob_app_purchase_roas_action_type
when matched and target._airbyte_extracted_at < source._airbyte_extracted_at
then update set
target._airbyte_extracted_at = source._airbyte_extracted_at,
target.ad_id = source.ad_id,
target.adset_id = source.adset_id,
target.account_id = source.account_id,
target.campaign_id = source.campaign_id,
target.date_start = source.date_start,
target.mob_app_purchase_roas_28d_click = source.mob_app_purchase_roas_28d_click, --11K(E)
target.mob_app_purchase_roas_28d_view = source.mob_app_purchase_roas_28d_view,
target.mob_app_purchase_roas_7d_click = source.mob_app_purchase_roas_7d_click,
target.mob_app_purchase_roas_7d_view = source.mob_app_purchase_roas_7d_view,
target.mob_app_purchase_roas_1d_click = source.mob_app_purchase_roas_1d_click,
target.mob_app_purchase_roas_1d_view = source.mob_app_purchase_roas_1d_view,
target.mob_app_purchase_roas_action_type = source.mob_app_purchase_roas_action_type,
target.mob_app_purchase_roas_value = source.mob_app_purchase_roas_value
when not matched
then insert
(
_airbyte_extracted_at,
ad_id,
adset_id,
account_id,
campaign_id,
date_start,
mob_app_purchase_roas_28d_click,
mob_app_purchase_roas_28d_view,
mob_app_purchase_roas_7d_click,
mob_app_purchase_roas_7d_view,
mob_app_purchase_roas_1d_click,
mob_app_purchase_roas_1d_view,
mob_app_purchase_roas_action_type,
mob_app_purchase_roas_value
)
values
(
source._airbyte_extracted_at,
source.ad_id,
source.adset_id,
source.account_id,
source.campaign_id,
source.date_start,
source.mob_app_purchase_roas_28d_click,
source.mob_app_purchase_roas_28d_view,
source.mob_app_purchase_roas_7d_click,
source.mob_app_purchase_roas_7d_view,
source.mob_app_purchase_roas_1d_click,
source.mob_app_purchase_roas_1d_view,
source.mob_app_purchase_roas_action_type,
source.mob_app_purchase_roas_value
)