MERGE INTO `shopify-pubsub-project.Data_Warehouse_GoogleAds_Staging.click_view` AS TARGET
USING
(
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
(
select *,
row_number() over(partition by click_view_gclid,segments_ad_network_type order by _airbyte_extracted_at desc) as rn
from shopify-pubsub-project.pilgrim_bi_google_ads.click_view
)
where rn = 1 and DATE(_airbyte_extracted_at) >= DATE_SUB(CURRENT_DATE("Asia/Kolkata"), INTERVAL 10 DAY)
)
AS SOURCE
on target.click_view_gclid = source.click_view_gclid
and target.segments_ad_network_type = source.segments_ad_network_type
WHEN MATCHED AND source._airbyte_extracted_at > target._airbyte_extracted_at
then update set
target._airbyte_extracted_at = source._airbyte_extracted_at,
target.ad_group_id = source.ad_group_id,
target.ad_group_name = source.ad_group_name,
target.campaign_id = source.campaign_id,
target.campaign_name = source.campaign_name,
target.campaign_network_settings_target_content_network = source.campaign_network_settings_target_content_network,
target.campaign_network_settings_target_google_search = source.campaign_network_settings_target_google_search,
target.campaign_network_settings_target_partner_search_network = source.campaign_network_settings_target_partner_search_network,
target.campaign_network_settings_target_search_network = source.campaign_network_settings_target_search_network,
target.click_view_ad_group_ad = source.click_view_ad_group_ad,
target.click_view_gclid = source.click_view_gclid,
target.click_view_keyword = source.click_view_keyword,
target.click_view_keyword_info_match_type = source.click_view_keyword_info_match_type,
target.click_view_keyword_info_text = source.click_view_keyword_info_text,
target.customer_id = source.customer_id,
target.segments_ad_network_type = source.segments_ad_network_type,
target.segments_date = source.segments_date
when not matched
then insert
(
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
segments_date
)
values
(
source._airbyte_extracted_at,
source.ad_group_id,
source.ad_group_name,
source.campaign_id,
source.campaign_name,
source.campaign_network_settings_target_content_network,
source.campaign_network_settings_target_google_search,
source.campaign_network_settings_target_partner_search_network,
source.campaign_network_settings_target_search_network,
source.click_view_ad_group_ad,
source.click_view_gclid,
source.click_view_keyword,
source.click_view_keyword_info_match_type,
source.click_view_keyword_info_text,
source.customer_id,
source.segments_ad_network_type,
source.segments_date
);