merge into `shopify-pubsub-project.Data_Warehouse_GoogleAds_Staging.ad_group` as target
using
(
select
  _airbyte_extracted_at,
  ad_group_optimized_targeting_enabled,
  ad_group_cpc_bid_micros,
  ad_group_cpm_bid_micros,
  ad_group_cpv_bid_micros,
  ad_group_effective_target_cpa_micros,
  ad_group_id,
  ad_group_name,
  segments_date,
  ad_group_percent_cpc_bid_micros,
  ad_group_target_cpa_micros,
  ad_group_target_cpm_micros,
  campaign_id,
  metrics_cost_micros,
  ad_group_targeting_setting_target_restrictions,
  ad_group_effective_target_roas,
  ad_group_target_roas,
  ad_group_ad_rotation_mode,
  ad_group_base_ad_group,
  ad_group_campaign,
  ad_group_display_custom_bid_dimension,
  ad_group_effective_target_cpa_source,
  ad_group_effective_target_roas_source,
  ad_group_final_url_suffix,
  ad_group_resource_name,
  ad_group_status,
  ad_group_type
from
(
  select *,
  row_number() over(partition by ad_group_id,segments_date order by _airbyte_extracted_at desc) as rn
  from shopify-pubsub-project.pilgrim_bi_google_ads.ad_group
)
where rn = 1 and DATE(_airbyte_extracted_at) >= DATE_SUB(CURRENT_DATE("Asia/Kolkata"), INTERVAL 10 DAY)
) as source
on target.ad_group_id = source.ad_group_id
and target.segments_date = source.segments_date
when matched and target._airbyte_extracted_at < source._airbyte_extracted_at
then update set
  target._airbyte_extracted_at = source._airbyte_extracted_at,
  target.ad_group_optimized_targeting_enabled = source.ad_group_optimized_targeting_enabled,
  target.ad_group_cpc_bid_micros = source.ad_group_cpc_bid_micros,
  target.ad_group_cpm_bid_micros = source.ad_group_cpm_bid_micros,
  target.ad_group_cpv_bid_micros = source.ad_group_cpv_bid_micros,
  target.ad_group_effective_target_cpa_micros = source.ad_group_effective_target_cpa_micros,
  target.ad_group_id = source.ad_group_id,
  target.ad_group_percent_cpc_bid_micros = source.ad_group_percent_cpc_bid_micros,
  target.ad_group_target_cpa_micros = source.ad_group_target_cpa_micros,
  target.ad_group_target_cpm_micros = source.ad_group_target_cpm_micros,
  target.campaign_id = source.campaign_id,
  target.metrics_cost_micros = source.metrics_cost_micros,
  target.ad_group_targeting_setting_target_restrictions = source.ad_group_targeting_setting_target_restrictions,
  target.ad_group_effective_target_roas = source.ad_group_effective_target_roas,
  target.ad_group_target_roas = source.ad_group_target_roas,
  target.ad_group_ad_rotation_mode = source.ad_group_ad_rotation_mode,
  target.ad_group_base_ad_group = source.ad_group_base_ad_group,
  target.ad_group_campaign = source.ad_group_campaign,
  target.ad_group_display_custom_bid_dimension = source.ad_group_display_custom_bid_dimension,
  target.ad_group_effective_target_cpa_source = source.ad_group_effective_target_cpa_source,
  target.ad_group_effective_target_roas_source = source.ad_group_effective_target_roas_source,
  target.ad_group_final_url_suffix = source.ad_group_final_url_suffix,
  target.ad_group_name = source.ad_group_name,
  target.ad_group_resource_name = source.ad_group_resource_name,
  target.ad_group_status = source.ad_group_status,
  target.ad_group_type = source.ad_group_type
when not matched
then insert
(
_airbyte_extracted_at,
ad_group_optimized_targeting_enabled,
ad_group_cpc_bid_micros,
ad_group_cpm_bid_micros,
ad_group_cpv_bid_micros,
ad_group_effective_target_cpa_micros,
ad_group_id,
ad_group_percent_cpc_bid_micros,
ad_group_target_cpa_micros,
ad_group_target_cpm_micros,
campaign_id,
metrics_cost_micros,
ad_group_targeting_setting_target_restrictions,
ad_group_effective_target_roas,
ad_group_target_roas,
ad_group_ad_rotation_mode,
ad_group_base_ad_group,
ad_group_campaign,
ad_group_display_custom_bid_dimension,
ad_group_effective_target_cpa_source,
ad_group_effective_target_roas_source,
ad_group_final_url_suffix,
ad_group_name,
ad_group_resource_name,
ad_group_status,
ad_group_type
)
values
(
_airbyte_extracted_at,
ad_group_optimized_targeting_enabled,
ad_group_cpc_bid_micros,
ad_group_cpm_bid_micros,
ad_group_cpv_bid_micros,
ad_group_effective_target_cpa_micros,
ad_group_id,
ad_group_percent_cpc_bid_micros,
ad_group_target_cpa_micros,
ad_group_target_cpm_micros,
campaign_id,
metrics_cost_micros,
ad_group_targeting_setting_target_restrictions,
ad_group_effective_target_roas,
ad_group_target_roas,
ad_group_ad_rotation_mode,
ad_group_base_ad_group,
ad_group_campaign,
ad_group_display_custom_bid_dimension,
ad_group_effective_target_cpa_source,
ad_group_effective_target_roas_source,
ad_group_final_url_suffix,
ad_group_name,
ad_group_resource_name,
ad_group_status,
ad_group_type
)