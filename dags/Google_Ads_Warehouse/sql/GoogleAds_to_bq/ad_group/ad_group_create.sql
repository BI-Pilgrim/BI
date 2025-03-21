create or replace table `shopify-pubsub-project.Data_Warehouse_GoogleAds_Staging.ad_group`
partition by date_trunc(_airbyte_extracted_at, day)
as
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
where rn = 1