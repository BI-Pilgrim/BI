create or replace table `shopify-pubsub-project.Data_Warehouse_GoogleAds_Staging.customer`
partition by date_trunc(_airbyte_extracted_at, day)
as
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
    shopify-pubsub-project.pilgrim_bi_google_ads.customer