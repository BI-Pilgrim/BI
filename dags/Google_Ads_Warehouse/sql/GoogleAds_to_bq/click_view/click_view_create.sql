create or replace table `shopify-pubsub-project.Data_Warehouse_GoogleAds_Staging.click_view`
partition by date_trunc(_airbyte_extracted_at, day)
as
  select
  _airbyte_extracted_at,
  ad_group_id,
  ad_group_name,
  campaign_id,
  campaign_name,
  campaign_network_settings_target_content_network,
  campaign_network_settings_target_google_search,
  campaign_network_settings_target_partner_search_network,
  campaign_network_settings_target_search_network,
  click_view_ad_group_ad,
  click_view_gclid,
  click_view_keyword,
  click_view_keyword_info_match_type,
  click_view_keyword_info_text,
  customer_id,
  segments_ad_network_type,
  segments_date,
  from
    shopify-pubsub-project.pilgrim_bi_google_ads.click_view
