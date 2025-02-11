create or replace table `shopify-pubsub-project.Data_Warehouse_GoogleAds_Staging.Sanity_check` as
with Sources as
(
select 
'ad_group_ad_normal' as table_name,
'ad_group_ad' as source_table,
max(date(segments_date)) as Source_max_date,
count(distinct case when date(segments_date) = (select max(date(segments_date)) from `shopify-pubsub-project.Data_Warehouse_GoogleAds_Staging.ad_group_ad_normal`) then concat(ad_group_ad_ad_id,segments_date) end ) as Source_pk_count
from `shopify-pubsub-project.pilgrim_bi_google_ads.ad_group_ad`

union all

select 
'account_performance_report' as table_name,
'account_performance_report' as source_table,
max(date(segments_date)) as Source_max_date,
count(distinct case when date(segments_date) = (select max(date(segments_date)) from `shopify-pubsub-project.Data_Warehouse_GoogleAds_Staging.account_performance_report`) then concat(customer_id,segments_date,segments_device,segments_ad_network_type) end ) as Source_pk_count
from `shopify-pubsub-project.pilgrim_bi_google_ads.account_performance_report`

union all

select 
'ad_group' as table_name,
'ad_group' as source_table,
max(date(segments_date)) as Source_max_date,
count(distinct case when date(segments_date) = (select max(date(segments_date)) from `shopify-pubsub-project.Data_Warehouse_GoogleAds_Staging.ad_group`) then concat(ad_group_id,segments_date) end ) as Source_pk_count
from `shopify-pubsub-project.pilgrim_bi_google_ads.ad_group`

union all

select 
'ad_group_ad_App_ad_desc' as table_name,
'ad_group_ad' as source_table,
max(date(segments_date)) as Source_max_date,
0 as Source_pk_count
from `shopify-pubsub-project.pilgrim_bi_google_ads.ad_group_ad`

union all

select 
'ad_group_ad_App_ad_head' as table_name,
'ad_group_ad' as source_table,
max(date(segments_date)) as Source_max_date,
0 as Source_pk_count
from `shopify-pubsub-project.pilgrim_bi_google_ads.ad_group_ad`

union all

select 
'ad_group_ad_App_ad_img' as table_name,
'ad_group_ad' as source_table,
max(date(segments_date)) as Source_max_date,
0 as Source_pk_count
from `shopify-pubsub-project.pilgrim_bi_google_ads.ad_group_ad`

union all

select 
'ad_group_ad_App_ad_yt_vids' as table_name,
'ad_group_ad' as source_table,
max(date(segments_date)) as Source_max_date,
0 as Source_pk_count
from `shopify-pubsub-project.pilgrim_bi_google_ads.ad_group_ad`

union all

select 
'ad_group_ad_App_eng_ad_head' as table_name,
'ad_group_ad' as source_table,
max(date(segments_date)) as Source_max_date,
0 as Source_pk_count
from `shopify-pubsub-project.pilgrim_bi_google_ads.ad_group_ad`

union all

select 
'ad_group_ad_App_eng_ad_vids' as table_name,
'ad_group_ad' as source_table,
max(date(segments_date)) as Source_max_date,
0 as Source_pk_count
from `shopify-pubsub-project.pilgrim_bi_google_ads.ad_group_ad`

union all

select 
'ad_group_ad_final_app_urls' as table_name,
'ad_group_ad' as source_table,
max(date(segments_date)) as Source_max_date,
0 as Source_pk_count
from `shopify-pubsub-project.pilgrim_bi_google_ads.ad_group_ad`

union all

select 
'ad_group_ad_final_app_urls' as table_name,
'ad_group_ad' as source_table,
max(date(segments_date)) as Source_max_date,
0 as Source_pk_count
from `shopify-pubsub-project.pilgrim_bi_google_ads.ad_group_ad`

union all

select 
'ad_group_ad_label' as table_name,
'ad_group_ad_label' as source_table,
max(date(_airbyte_extracted_at)) as Source_max_date,
count(distinct case when date(_airbyte_extracted_at) = (select max(date(_airbyte_extracted_at)) from `shopify-pubsub-project.Data_Warehouse_GoogleAds_Staging.ad_group_ad_label`) then concat(ad_group_ad_ad_id,label_id) end ) as Source_pk_count
from `shopify-pubsub-project.pilgrim_bi_google_ads.ad_group_ad_label`

union all

select 
'ad_group_ad_legacy' as table_name,
'ad_group_ad_legacy' as source_table,
max(date(segments_date)) as Source_max_date,
count(distinct case when date(segments_date) = (select max(date(segments_date)) from `shopify-pubsub-project.Data_Warehouse_GoogleAds_Staging.ad_group_ad_legacy`) then concat(ad_group_ad_ad_id,segments_date,segments_ad_network_type) end ) as Source_pk_count
from `shopify-pubsub-project.pilgrim_bi_google_ads.ad_group_ad_legacy`

union all

select 
'ad_group_ad_respnsive_display_ad_head' as table_name,
'ad_group_ad' as source_table,
max(date(segments_date)) as Source_max_date,
0 as Source_pk_count
from `shopify-pubsub-project.pilgrim_bi_google_ads.ad_group_ad`

union all

select 
'ad_group_ad_rresponsive_ad_long_head' as table_name,
'ad_group_ad' as source_table,
max(date(segments_date)) as Source_max_date,
0 as Source_pk_count
from `shopify-pubsub-project.pilgrim_bi_google_ads.ad_group_ad`

union all

select 
'ad_group_ad_search_ad_desc' as table_name,
'ad_group_ad' as source_table,
max(date(segments_date)) as Source_max_date,
0 as Source_pk_count
from `shopify-pubsub-project.pilgrim_bi_google_ads.ad_group_ad`

-- union all

-- select 
-- 'ad_group_ad_url_custom_parameters' as table_name,
-- 'ad_group_ad' as source_table,
-- max(date(segments_date)) as Source_max_date,
-- count(distinct case when date(segments_date) = (select max(date(segments_date)) from `shopify-pubsub-project.Data_Warehouse_GoogleAds_Staging.ad_group_ad_url_custom_parameters`) then concat(ad_group_ad_ad_id,segments_date) end ) as Source_pk_count
-- from `shopify-pubsub-project.pilgrim_bi_google_ads.ad_group_ad`

union all

select 
'ad_group_ad_vid_resp_ad_call_to_actions' as table_name,
'ad_group_ad' as source_table,
max(date(segments_date)) as Source_max_date,
0 as Source_pk_count
from `shopify-pubsub-project.pilgrim_bi_google_ads.ad_group_ad`

union all

select 
'ad_group_ad_video_responsive_ad_descriptions' as table_name,
'ad_group_ad' as source_table,
max(date(segments_date)) as Source_max_date,
0 as Source_pk_count
from `shopify-pubsub-project.pilgrim_bi_google_ads.ad_group_ad`

union all

select 
'ad_group_ad_video_responsive_ad_headlines' as table_name,
'ad_group_ad' as source_table,
max(date(segments_date)) as Source_max_date,
0 as Source_pk_count
from `shopify-pubsub-project.pilgrim_bi_google_ads.ad_group_ad`

union all

select 
'ad_group_ad_video_responsive_search_ad_headlines' as table_name,
'ad_group_ad' as source_table,
max(date(segments_date)) as Source_max_date,
0 as Source_pk_count
from `shopify-pubsub-project.pilgrim_bi_google_ads.ad_group_ad`

union all

select 
'ad_group_bidding_strategy' as table_name,
'ad_group_bidding_strategy' as source_table,
max(date(segments_date)) as Source_max_date,
count(distinct case when date(segments_date) = (select max(date(segments_date)) from `shopify-pubsub-project.Data_Warehouse_GoogleAds_Staging.ad_group_bidding_strategy`) then concat(ad_group_id,segments_date) end ) as Source_pk_count
from `shopify-pubsub-project.pilgrim_bi_google_ads.ad_group_bidding_strategy`

union all

select 
'ad_group_criterion' as table_name,
'ad_group_bidding_strategy' as source_table,
max(date(_airbyte_extracted_at)) as Source_max_date,
count(distinct case when date(_airbyte_extracted_at) = (select max(date(_airbyte_extracted_at)) from `shopify-pubsub-project.Data_Warehouse_GoogleAds_Staging.ad_group_criterion`) then ad_group_criterion_resource_name end ) as Source_pk_count
from `shopify-pubsub-project.pilgrim_bi_google_ads.ad_group_criterion`

union all

select 
'audience' as table_name,
'audience' as source_table,
max(date(_airbyte_extracted_at)) as Source_max_date,
count(distinct case when date(_airbyte_extracted_at) = (select max(date(_airbyte_extracted_at)) from `shopify-pubsub-project.Data_Warehouse_GoogleAds_Staging.audience`) then audience_id end ) as Source_pk_count
from `shopify-pubsub-project.pilgrim_bi_google_ads.audience`

union all

select 
'campaign' as table_name,
'campaign' as source_table,
max(date(_airbyte_extracted_at)) as Source_max_date,
count(distinct case when date(_airbyte_extracted_at) = (select max(date(_airbyte_extracted_at)) from `shopify-pubsub-project.Data_Warehouse_GoogleAds_Staging.campaign`) then concat(campaign_id,segments_date,segments_hour,segments_ad_network_type) end ) as Source_pk_count
from `shopify-pubsub-project.pilgrim_bi_google_ads.campaign`

union all

select 
'campaign_bidding_strategy' as table_name,
'campaign_bidding_strategy' as source_table,
max(date(_airbyte_extracted_at)) as Source_max_date,
count(distinct case when date(_airbyte_extracted_at) = (select max(date(_airbyte_extracted_at)) from `shopify-pubsub-project.Data_Warehouse_GoogleAds_Staging.campaign_bidding_strategy`) then concat(campaign_id,segments_date) end ) as Source_pk_count
from `shopify-pubsub-project.pilgrim_bi_google_ads.campaign_bidding_strategy`

union all

select 
'campaign_criterion' as table_name,
'campaign_criterion' as source_table,
max(date(_airbyte_extracted_at)) as Source_max_date,
count(distinct case when date(_airbyte_extracted_at) = (select max(date(_airbyte_extracted_at)) from `shopify-pubsub-project.Data_Warehouse_GoogleAds_Staging.campaign_criterion`) then campaign_criterion_resource_name end ) as Source_pk_count
from `shopify-pubsub-project.pilgrim_bi_google_ads.campaign_criterion`

union all

select 
'campaign_label' as table_name,
'campaign_label' as source_table,
max(date(_airbyte_extracted_at)) as Source_max_date,
count(distinct case when date(_airbyte_extracted_at) = (select max(date(_airbyte_extracted_at)) from `shopify-pubsub-project.Data_Warehouse_GoogleAds_Staging.campaign_label`) then concat(campaign_id,label_id) end ) as Source_pk_count
from `shopify-pubsub-project.pilgrim_bi_google_ads.campaign_label`

union all

select 
'click_view' as table_name,
'click_view' as source_table,
max(date(_airbyte_extracted_at)) as Source_max_date,
count(distinct case when date(_airbyte_extracted_at) = (select max(date(_airbyte_extracted_at)) from `shopify-pubsub-project.Data_Warehouse_GoogleAds_Staging.click_view`) then concat(click_view_gclid,segments_ad_network_type) end ) as Source_pk_count
from `shopify-pubsub-project.pilgrim_bi_google_ads.click_view`

union all

select 
'customer' as table_name,
'customer' as source_table,
max(date(_airbyte_extracted_at)) as Source_max_date,
count(distinct case when date(_airbyte_extracted_at) = (select max(date(_airbyte_extracted_at)) from `shopify-pubsub-project.Data_Warehouse_GoogleAds_Staging.customer`) then concat(customer_id,segments_date) end ) as Source_pk_count
from `shopify-pubsub-project.pilgrim_bi_google_ads.customer`

union all

select 
'geographic_view' as table_name,
'geographic_view' as source_table,
max(date(segments_date)) as Source_max_date,
count(distinct case when date(segments_date) = (select max(date(segments_date)) from `shopify-pubsub-project.Data_Warehouse_GoogleAds_Staging.geographic_view`) then concat(ad_group_id,segments_date,geographic_view_location_type,geographic_view_country_criterion_id) end ) as Source_pk_count
from `shopify-pubsub-project.pilgrim_bi_google_ads.geographic_view`


union all

select 
'keyword_view' as table_name,
'keyword_view' as source_table,
max(date(segments_date)) as Source_max_date,
count(distinct case when date(segments_date) = (select max(date(segments_date)) from `shopify-pubsub-project.Data_Warehouse_GoogleAds_Staging.keyword_view`) then concat(ad_group_id,segments_date,ad_group_criterion_keyword_text,ad_group_criterion_keyword_match_type) end ) as Source_pk_count
from `shopify-pubsub-project.pilgrim_bi_google_ads.keyword_view`

union all

select 
'label' as table_name,
'label' as source_table,
max(date(_airbyte_extracted_at)) as Source_max_date,
count(distinct case when date(_airbyte_extracted_at) = (select max(date(_airbyte_extracted_at)) from `shopify-pubsub-project.Data_Warehouse_GoogleAds_Staging.label`) then label_id end ) as Source_pk_count
from `shopify-pubsub-project.pilgrim_bi_google_ads.label`

union all

select 
'user_interest' as table_name,
'user_interest' as source_table,
max(date(_airbyte_extracted_at)) as Source_max_date,
count(distinct case when date(_airbyte_extracted_at) = (select max(date(_airbyte_extracted_at)) from `shopify-pubsub-project.Data_Warehouse_GoogleAds_Staging.user_interest`) then user_interest_resource_name end ) as Source_pk_count
from `shopify-pubsub-project.pilgrim_bi_google_ads.user_interest`

union all

select 
'user_location_view' as table_name,
'user_location_view' as source_table,
max(date(_airbyte_extracted_at)) as Source_max_date,
count(distinct case when date(_airbyte_extracted_at) = (select max(date(_airbyte_extracted_at)) from `shopify-pubsub-project.Data_Warehouse_GoogleAds_Staging.user_location_view`) then concat(segments_date,segments_ad_network_type,user_location_view_resource_name) end ) as Source_pk_count
from `shopify-pubsub-project.pilgrim_bi_google_ads.user_location_view`
),

----------------------------------------------------------------------------------------------------------------------------------------------------------

Staging as
(
select
'user_location_view' as table_name,
max(date(_airbyte_extracted_at)) as Staging_max_date,
max(date(_airbyte_extracted_at)) as Latest_Valid_Date,
count(distinct case when date(_airbyte_extracted_at) = (select max(date(_airbyte_extracted_at)) from `shopify-pubsub-project.pilgrim_bi_google_ads.user_location_view`) then concat(segments_date,segments_ad_network_type,user_location_view_resource_name) end ) as Staging_pk_count
from  `shopify-pubsub-project.Data_Warehouse_GoogleAds_Staging.user_location_view`

union all

select
'user_interest' as table_name,
max(date(_airbyte_extracted_at)) as Staging_max_date,
max(date(_airbyte_extracted_at)) as Latest_Valid_Date,
count(distinct case when date(_airbyte_extracted_at) = (select max(date(_airbyte_extracted_at)) from `shopify-pubsub-project.pilgrim_bi_google_ads.user_interest`) then user_interest_resource_name end ) as Staging_pk_count
from  `shopify-pubsub-project.Data_Warehouse_GoogleAds_Staging.user_interest`

union all

select
'label' as table_name,
max(date(_airbyte_extracted_at)) as Staging_max_date,
max(date(_airbyte_extracted_at)) as Latest_Valid_Date,
count(distinct case when date(_airbyte_extracted_at) = (select max(date(_airbyte_extracted_at)) from `shopify-pubsub-project.pilgrim_bi_google_ads.label`) then label_id end ) as Staging_pk_count
from  `shopify-pubsub-project.Data_Warehouse_GoogleAds_Staging.label`

union all

select
'keyword_view' as table_name,
max(date(_airbyte_extracted_at)) as Staging_max_date,
max(date(_airbyte_extracted_at)) as Latest_Valid_Date,
count(distinct case when date(_airbyte_extracted_at) = (select max(date(_airbyte_extracted_at)) from `shopify-pubsub-project.pilgrim_bi_google_ads.keyword_view`) then concat(ad_group_id,segments_date,ad_group_criterion_keyword_text,ad_group_criterion_keyword_match_type) end ) as Staging_pk_count
from  `shopify-pubsub-project.Data_Warehouse_GoogleAds_Staging.keyword_view`

union all

select
'geographic_view' as table_name,
max(date(segments_date)) as Staging_max_date,
max(date(segments_date)) as Latest_Valid_Date,
count(distinct case when date(segments_date) = (select max(date(segments_date)) from `shopify-pubsub-project.pilgrim_bi_google_ads.geographic_view`) then concat(ad_group_id,segments_date,geographic_view_location_type,geographic_view_country_criterion_id) end ) as Staging_pk_count
from  `shopify-pubsub-project.Data_Warehouse_GoogleAds_Staging.geographic_view`

union all

select
'customer' as table_name,
max(date(_airbyte_extracted_at)) as Staging_max_date,
max(date(_airbyte_extracted_at)) as Latest_Valid_Date,
count(distinct case when date(_airbyte_extracted_at) = (select max(date(_airbyte_extracted_at)) from `shopify-pubsub-project.pilgrim_bi_google_ads.customer`) then concat(customer_id,segments_date) end ) as Staging_pk_count
from  `shopify-pubsub-project.Data_Warehouse_GoogleAds_Staging.customer`

union all

select 
'click_view' as table_name,
max(date(_airbyte_extracted_at)) as Staging_max_date,
max(date(_airbyte_extracted_at)) as Latest_Valid_Date,
count(distinct case when date(_airbyte_extracted_at) = (select max(date(_airbyte_extracted_at)) from `shopify-pubsub-project.pilgrim_bi_google_ads.click_view`) then concat(click_view_gclid,segments_ad_network_type) end ) as Staging_pk_count
from  `shopify-pubsub-project.Data_Warehouse_GoogleAds_Staging.click_view`

union all

select 
'campaign_label' as table_name,
max(date(_airbyte_extracted_at)) as Staging_max_date,
max(date(_airbyte_extracted_at)) as Latest_Valid_Date,
count(distinct case when date(_airbyte_extracted_at) = (select max(date(_airbyte_extracted_at)) from `shopify-pubsub-project.pilgrim_bi_google_ads.campaign_label`) then concat(campaign_id,label_id) end ) as Staging_pk_count
from  `shopify-pubsub-project.Data_Warehouse_GoogleAds_Staging.campaign_label`

union all

select 
'campaign_criterion' as table_name,
max(date(_airbyte_extracted_at)) as Staging_max_date,
max(date(_airbyte_extracted_at)) as Latest_Valid_Date,
count(distinct case when date(_airbyte_extracted_at) = (select max(date(_airbyte_extracted_at)) from `shopify-pubsub-project.pilgrim_bi_google_ads.campaign_criterion`) then concat(campaign_criterion_resource_name) end ) as Staging_pk_count
from  `shopify-pubsub-project.Data_Warehouse_GoogleAds_Staging.campaign_criterion`

union all

select 
'campaign_bidding_strategy' as table_name,
max(date(_airbyte_extracted_at)) as Staging_max_date,
max(date(_airbyte_extracted_at)) as Latest_Valid_Date,
count(distinct case when date(_airbyte_extracted_at) = (select max(date(_airbyte_extracted_at)) from `shopify-pubsub-project.pilgrim_bi_google_ads.campaign_bidding_strategy`) then concat(campaign_id,segments_date) end ) as Staging_pk_count
from  `shopify-pubsub-project.Data_Warehouse_GoogleAds_Staging.campaign_bidding_strategy`

union all

select 
'campaign' as table_name,
max(date(_airbyte_extracted_at)) as Staging_max_date,
max(date(_airbyte_extracted_at)) as Latest_Valid_Date,
count(distinct case when date(_airbyte_extracted_at) = (select max(date(_airbyte_extracted_at)) from `shopify-pubsub-project.pilgrim_bi_google_ads.campaign`) then concat(campaign_id,segments_date,segments_hour,segments_ad_network_type) end ) as Staging_pk_count
from  `shopify-pubsub-project.Data_Warehouse_GoogleAds_Staging.campaign`

union all

select 
'audience' as table_name,
max(date(_airbyte_extracted_at)) as Staging_max_date,
max(date(_airbyte_extracted_at)) as Latest_Valid_Date,
count(distinct case when date(_airbyte_extracted_at) = (select max(date(_airbyte_extracted_at)) from `shopify-pubsub-project.pilgrim_bi_google_ads.audience`) then audience_id end ) as Staging_pk_count
from  `shopify-pubsub-project.Data_Warehouse_GoogleAds_Staging.audience`

union all

select 
'ad_group_criterion' as table_name,
max(date(_airbyte_extracted_at)) as Staging_max_date,
max(date(_airbyte_extracted_at)) as Latest_Valid_Date,
count(distinct case when date(_airbyte_extracted_at) = (select max(date(_airbyte_extracted_at)) from `shopify-pubsub-project.pilgrim_bi_google_ads.ad_group_criterion`) then concat(ad_group_criterion_resource_name) end ) as Staging_pk_count
from  `shopify-pubsub-project.Data_Warehouse_GoogleAds_Staging.ad_group_criterion`

union all

select 
'ad_group_bidding_strategy' as table_name,
max(date(segments_date)) as Staging_max_date,
max(date(segments_date)) as Latest_Valid_Date,
count(distinct case when date(segments_date) = (select max(date(segments_date)) from `shopify-pubsub-project.pilgrim_bi_google_ads.ad_group_bidding_strategy`) then concat(ad_group_id,segments_date) end ) as Staging_pk_count
from  `shopify-pubsub-project.Data_Warehouse_GoogleAds_Staging.ad_group_bidding_strategy`

union all

select 
'ad_group_ad_App_ad_desc' as table_name,
max(date(segments_date)) as Staging_max_date,
(SELECT MAX(DATE(segments_date)) AS max_valid_date FROM `shopify-pubsub-project.pilgrim_bi_google_ads.ad_group_ad` WHERE ad_group_ad_ad_app_ad_descriptions IS NOT NULL) as Latest_Valid_Date,
0 as Staging_pk_count
from `shopify-pubsub-project.pilgrim_bi_google_ads.ad_group_ad`

union all

select 
'ad_group_ad_App_ad_head' as table_name,
max(date(segments_date)) as Staging_max_date,
(SELECT MAX(DATE(segments_date)) AS max_valid_date FROM `shopify-pubsub-project.pilgrim_bi_google_ads.ad_group_ad` WHERE ad_group_ad_ad_app_ad_headlines IS NOT NULL) as Latest_Valid_Date,
0 as Staging_pk_count
from `shopify-pubsub-project.pilgrim_bi_google_ads.ad_group_ad`

union all

select 
'ad_group_ad_App_ad_img' as table_name,
max(date(segments_date)) as Staging_max_date,
(SELECT MAX(DATE(segments_date)) AS max_valid_date FROM `shopify-pubsub-project.pilgrim_bi_google_ads.ad_group_ad` WHERE ad_group_ad_ad_app_ad_images IS NOT NULL) as Latest_Valid_Date,
0 as Staging_pk_count
from `shopify-pubsub-project.pilgrim_bi_google_ads.ad_group_ad`

union all

select 
'ad_group_ad_App_ad_yt_vids' as table_name,
max(date(segments_date)) as Staging_max_date,
(SELECT MAX(DATE(segments_date)) AS max_valid_date FROM `shopify-pubsub-project.pilgrim_bi_google_ads.ad_group_ad` WHERE ad_group_ad_ad_app_ad_youtube_videos IS NOT NULL) as Latest_Valid_Date,
0 as Staging_pk_count
from `shopify-pubsub-project.pilgrim_bi_google_ads.ad_group_ad`

union all

select 
'ad_group_ad_App_eng_ad_head' as table_name,
max(date(segments_date)) as Staging_max_date,
(SELECT MAX(DATE(segments_date)) AS max_valid_date FROM `shopify-pubsub-project.pilgrim_bi_google_ads.ad_group_ad` WHERE ad_group_ad_ad_app_engagement_ad_headlines IS NOT NULL) as Latest_Valid_Date,
0 as Staging_pk_count
from `shopify-pubsub-project.pilgrim_bi_google_ads.ad_group_ad`

union all

select 
'ad_group_ad_App_eng_ad_vids' as table_name,
max(date(segments_date)) as Staging_max_date,
(SELECT MAX(DATE(segments_date)) AS max_valid_date FROM `shopify-pubsub-project.pilgrim_bi_google_ads.ad_group_ad` WHERE ad_group_ad_ad_app_engagement_ad_videos IS NOT NULL) as Latest_Valid_Date,
0 as Staging_pk_count
from `shopify-pubsub-project.pilgrim_bi_google_ads.ad_group_ad`

union all

select 
'ad_group_ad_final_app_urls' as table_name,
max(date(segments_date)) as Staging_max_date,
(SELECT MAX(DATE(segments_date)) AS max_valid_date FROM `shopify-pubsub-project.pilgrim_bi_google_ads.ad_group_ad` WHERE ad_group_ad_ad_final_app_urls IS NOT NULL) as Latest_Valid_Date,
0 as Staging_pk_count
from `shopify-pubsub-project.pilgrim_bi_google_ads.ad_group_ad`

union all

select 
'ad_group_ad_final_app_urls' as table_name,
max(date(segments_date)) as Staging_max_date,
(SELECT MAX(DATE(segments_date)) AS max_valid_date FROM `shopify-pubsub-project.pilgrim_bi_google_ads.ad_group_ad` WHERE ad_group_ad_ad_final_app_urls IS NOT NULL) as Latest_Valid_Date,
0 as Staging_pk_count
from `shopify-pubsub-project.pilgrim_bi_google_ads.ad_group_ad`

union all

select 
'ad_group_ad_label' as table_name,
max(date(_airbyte_extracted_at)) as Staging_max_date,
max(date(_airbyte_extracted_at)) as Latest_Valid_Date,
count(distinct case when date(_airbyte_extracted_at) = (select max(date(_airbyte_extracted_at)) from `shopify-pubsub-project.Data_Warehouse_GoogleAds_Staging.ad_group_ad_label`) then concat(ad_group_ad_ad_id,label_id) end ) as Staging_pk_count
from `shopify-pubsub-project.pilgrim_bi_google_ads.ad_group_ad_label`

union all

select 
'ad_group_ad_legacy' as table_name,
max(date(segments_date)) as Staging_max_date,
max(date(segments_date)) as Latest_Valid_Date,
count(distinct case when date(segments_date) = (select max(date(segments_date)) from `shopify-pubsub-project.Data_Warehouse_GoogleAds_Staging.ad_group_ad_legacy`) then concat(ad_group_ad_ad_id,segments_date,segments_ad_network_type) end ) as Staging_pk_count
from `shopify-pubsub-project.pilgrim_bi_google_ads.ad_group_ad_legacy`

union all

select 
'ad_group_ad_respnsive_display_ad_head' as table_name,
max(date(segments_date)) as Staging_max_date,
(SELECT MAX(DATE(segments_date)) AS max_valid_date FROM `shopify-pubsub-project.pilgrim_bi_google_ads.ad_group_ad` WHERE ad_group_ad_ad_final_app_urls IS NOT NULL) as Latest_Valid_Date,
0 as Staging_pk_count
from `shopify-pubsub-project.pilgrim_bi_google_ads.ad_group_ad`

union all

select 
'ad_group_ad_rresponsive_ad_long_head' as table_name,
max(date(segments_date)) as Staging_max_date,
(SELECT MAX(DATE(segments_date)) AS max_valid_date FROM `shopify-pubsub-project.pilgrim_bi_google_ads.ad_group_ad` WHERE ad_group_ad_ad_video_responsive_ad_long_headlines IS NOT NULL) as Latest_Valid_Date,
0 as Staging_pk_count
from `shopify-pubsub-project.pilgrim_bi_google_ads.ad_group_ad`

union all

select 
'ad_group_ad_search_ad_desc' as table_name,
max(date(segments_date)) as Staging_max_date,
(SELECT MAX(DATE(segments_date)) AS max_valid_date FROM `shopify-pubsub-project.pilgrim_bi_google_ads.ad_group_ad` WHERE ad_group_ad_ad_responsive_search_ad_descriptions IS NOT NULL) as Latest_Valid_Date,
0 as Staging_pk_count
from `shopify-pubsub-project.pilgrim_bi_google_ads.ad_group_ad`

union all

select 
'ad_group_ad_respnsive_display_ad_head' as table_name,
max(date(segments_date)) as Staging_max_date,
(SELECT MAX(DATE(segments_date)) AS max_valid_date FROM `shopify-pubsub-project.pilgrim_bi_google_ads.ad_group_ad` WHERE ad_group_ad_ad_responsive_display_ad_headlines IS NOT NULL) as Latest_Valid_Date,
0 as Staging_pk_count
from `shopify-pubsub-project.Data_Warehouse_GoogleAds_Staging.ad_group_ad_respnsive_display_ad_head`

union all

select 
'ad_group_ad_rresponsive_ad_long_head' as table_name,
max(date(segments_date)) as Staging_max_date,
(SELECT MAX(DATE(segments_date)) AS max_valid_date FROM `shopify-pubsub-project.pilgrim_bi_google_ads.ad_group_ad` WHERE ad_group_ad_ad_video_responsive_ad_long_headlines IS NOT NULL) as Latest_Valid_Date,
0 as Staging_pk_count
from `shopify-pubsub-project.Data_Warehouse_GoogleAds_Staging.ad_group_ad_rresponsive_ad_long_head`

union all

select 
'ad_group_ad_search_ad_desc' as table_name,
max(date(segments_date)) as Staging_max_date,
(SELECT MAX(DATE(segments_date)) AS max_valid_date FROM `shopify-pubsub-project.pilgrim_bi_google_ads.ad_group_ad` WHERE ad_group_ad_ad_responsive_search_ad_descriptions IS NOT NULL) as Latest_Valid_Date,
0 as Staging_pk_count
from `shopify-pubsub-project.Data_Warehouse_GoogleAds_Staging.ad_group_ad_search_ad_desc`


union all

select 
'ad_group_ad_vid_resp_ad_call_to_actions' as table_name,
max(date(segments_date)) as Staging_max_date,
(SELECT MAX(DATE(segments_date)) AS max_valid_date FROM `shopify-pubsub-project.pilgrim_bi_google_ads.ad_group_ad` WHERE ad_group_ad_ad_video_responsive_ad_call_to_actions IS NOT NULL) as Latest_Valid_Date,
0 as Staging_pk_count
from `shopify-pubsub-project.Data_Warehouse_GoogleAds_Staging.ad_group_ad_vid_resp_ad_call_to_actions`

union all

select 
'ad_group_ad_video_responsive_ad_descriptions' as table_name,
max(date(segments_date)) as Staging_max_date,
(SELECT MAX(DATE(segments_date)) AS max_valid_date FROM `shopify-pubsub-project.pilgrim_bi_google_ads.ad_group_ad` WHERE ad_group_ad_ad_video_responsive_ad_descriptions IS NOT NULL) as Latest_Valid_Date,
0 as Staging_pk_count
from `shopify-pubsub-project.Data_Warehouse_GoogleAds_Staging.ad_group_ad_video_responsive_ad_descriptions`

union all

select 
'ad_group_ad_video_responsive_ad_headlines' as table_name,
max(date(segments_date)) as Staging_max_date,
(SELECT MAX(DATE(segments_date)) AS max_valid_date FROM `shopify-pubsub-project.pilgrim_bi_google_ads.ad_group_ad` WHERE ad_group_ad_ad_video_responsive_ad_headlines IS NOT NULL) as Latest_Valid_Date,
0 as Staging_pk_count
from `shopify-pubsub-project.Data_Warehouse_GoogleAds_Staging.ad_group_ad_video_responsive_ad_headlines`

union all

select 
'ad_group_ad_video_responsive_search_ad_headlines' as table_name,
max(date(segments_date)) as Staging_max_date,
(SELECT MAX(DATE(segments_date)) AS max_valid_date FROM `shopify-pubsub-project.pilgrim_bi_google_ads.ad_group_ad` WHERE ad_group_ad_ad_responsive_search_ad_headlines IS NOT NULL) as Latest_Valid_Date,
0 as Staging_pk_count
from `shopify-pubsub-project.Data_Warehouse_GoogleAds_Staging.ad_group_ad_video_responsive_search_ad_headlines`

union all

select 
'ad_group_ad_normal' as table_name,
max(date(segments_date)) as Staging_max_date,
max(date(segments_date)) as Latest_Valid_Date,
count(distinct case when date(segments_date) = (select max(date(segments_date)) from `shopify-pubsub-project.pilgrim_bi_google_ads.ad_group_ad`) then concat(ad_group_ad_ad_id,segments_date) end ) as Staging_pk_count
from  `shopify-pubsub-project.Data_Warehouse_GoogleAds_Staging.ad_group_ad_normal`

union all

select 
'account_performance_report' as table_name,
max(date(segments_date)) as Staging_max_date,
max(date(segments_date)) as Latest_Valid_Date,
count(distinct case when date(segments_date) = (select max(date(segments_date)) from `shopify-pubsub-project.pilgrim_bi_google_ads.account_performance_report`) then concat(customer_id,segments_date,segments_device,segments_ad_network_type) end ) as Staging_pk_count
from  `shopify-pubsub-project.Data_Warehouse_GoogleAds_Staging.account_performance_report`

union all

select 
'ad_group' as table_name,
max(date(segments_date)) as Staging_max_date,
max(date(segments_date)) as Latest_Valid_Date,
count(distinct case when date(segments_date) = (select max(date(segments_date)) from `shopify-pubsub-project.pilgrim_bi_google_ads.ad_group`) then concat(ad_group_id,segments_date) end ) as Staging_pk_count
from  `shopify-pubsub-project.Data_Warehouse_GoogleAds_Staging.ad_group`
)

select
So.table_name,
So.source_table,
Date(So.Source_max_date) as Source_max_date,
Date(St.Staging_max_date) as Staging_max_date,
Latest_Valid_Date,
Current_date() as Date1,
So.Source_pk_count,
St.Staging_pk_count,

from Sources as So
left join Staging as St
on So.table_name = St.table_name
-- where Source_max_date < Latest_Valid_Date
order by table_name