MERGE INTO `shopify-pubsub-project.Data_Warehouse_GoogleAds_Staging.customer` AS TARGET
USING
(
select
  _airbyte_extracted_at,
  customer_auto_tagging_enabled,
  customer_call_reporting_setting_call_conversion_action,
  customer_call_reporting_setting_call_conversion_reporting_enabled,
  customer_call_reporting_setting_call_reporting_enabled,
  customer_conversion_tracking_setting_conversion_tracking_id,
  customer_conversion_tracking_setting_cross_account_conversion_tracking_id,
  customer_currency_code,
  customer_descriptive_name,
  customer_final_url_suffix,
  customer_has_partners_badge,
  customer_id,
  customer_manager,
  customer_optimization_score,
  customer_optimization_score_weight,
  customer_remarketing_setting_google_global_site_tag,
  customer_resource_name,
  customer_test_account,
  customer_time_zone,
  customer_tracking_url_template,
  segments_date,
from
(
SELECT
  *,
  ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY _airbyte_extracted_at DESC) AS row_num
FROM
shopify-pubsub-project.pilgrim_bi_google_ads.customer
where
DATE(_airbyte_extracted_at) >= DATE_SUB(CURRENT_DATE("Asia/Kolkata"), INTERVAL 10 DAY)
)
WHERE row_num = 1
)
AS SOURCE
on target.customer_id = source.customer_id
WHEN MATCHED AND source._airbyte_extracted_at > target._airbyte_extracted_at
then update set
target._airbyte_extracted_at = source._airbyte_extracted_at,
target.customer_auto_tagging_enabled = source.customer_auto_tagging_enabled,
target.customer_call_reporting_setting_call_conversion_action = source.customer_call_reporting_setting_call_conversion_action,
target.customer_call_reporting_setting_call_conversion_reporting_enabled = source.customer_call_reporting_setting_call_conversion_reporting_enabled,
target.customer_call_reporting_setting_call_reporting_enabled = source.customer_call_reporting_setting_call_reporting_enabled,
target.customer_conversion_tracking_setting_conversion_tracking_id = source.customer_conversion_tracking_setting_conversion_tracking_id,
target.customer_conversion_tracking_setting_cross_account_conversion_tracking_id = source.customer_conversion_tracking_setting_cross_account_conversion_tracking_id,
target.customer_currency_code = source.customer_currency_code,
target.customer_descriptive_name = source.customer_descriptive_name,
target.customer_final_url_suffix = source.customer_final_url_suffix,
target.customer_has_partners_badge = source.customer_has_partners_badge,
target.customer_id = source.customer_id,
target.customer_manager = source.customer_manager,
target.customer_optimization_score = source.customer_optimization_score,
target.customer_optimization_score_weight = source.customer_optimization_score_weight,
target.customer_remarketing_setting_google_global_site_tag = source.customer_remarketing_setting_google_global_site_tag,
target.customer_resource_name = source.customer_resource_name,
target.customer_test_account = source.customer_test_account,
target.customer_time_zone = source.customer_time_zone,
target.customer_tracking_url_template = source.customer_tracking_url_template,
target.segments_date = source.segments_date
when not matched
then insert
(
_airbyte_extracted_at,
  customer_auto_tagging_enabled,
  customer_call_reporting_setting_call_conversion_action,
  customer_call_reporting_setting_call_conversion_reporting_enabled,
  customer_call_reporting_setting_call_reporting_enabled,
  customer_conversion_tracking_setting_conversion_tracking_id,
  customer_conversion_tracking_setting_cross_account_conversion_tracking_id,
  customer_currency_code,
  customer_descriptive_name,
  customer_final_url_suffix,
  customer_has_partners_badge,
  customer_id,
  customer_manager,
  customer_optimization_score,
  customer_optimization_score_weight,
  customer_remarketing_setting_google_global_site_tag,
  customer_resource_name,
  customer_test_account,
  customer_time_zone,
  customer_tracking_url_template,
  segments_date
)
values
(
source._airbyte_extracted_at,
source.customer_auto_tagging_enabled,
source.customer_call_reporting_setting_call_conversion_action,
source.customer_call_reporting_setting_call_conversion_reporting_enabled,
source.customer_call_reporting_setting_call_reporting_enabled,
source.customer_conversion_tracking_setting_conversion_tracking_id,
source.customer_conversion_tracking_setting_cross_account_conversion_tracking_id,
source.customer_currency_code,
source.customer_descriptive_name,
source.customer_final_url_suffix,
source.customer_has_partners_badge,
source.customer_id,
source.customer_manager,
source.customer_optimization_score,
source.customer_optimization_score_weight,
source.customer_remarketing_setting_google_global_site_tag,
source.customer_resource_name,
source.customer_test_account,
source.customer_time_zone,
source.customer_tracking_url_template,
source.segments_date
);