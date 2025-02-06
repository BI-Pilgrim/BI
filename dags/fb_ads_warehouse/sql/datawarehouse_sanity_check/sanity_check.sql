create or replace table `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.Sanity_check` as
with Sources as
  (

  select 
  'activities' as table_name,
  'activities' as source_table,
  max(date(event_time)) as Source_max_date,
  
  count(distinct case when date(event_time) = (select max(date(event_time)) from `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.activities`) then concat(object_id, event_time, account_id) end ) as Source_pk_count
  from `shopify-pubsub-project.pilgrim_bi_airbyte_facebook.activities`

--  union all

-- -- Since Orderitem table is derived from orders therefor count(distinct pk) of source and Stagingination are same
--   select 
--   'Order_item' as table_name,
--   max(order_date) as Source_max_date,
--  count(distinct case when date(order_date) = (select max(date(order_date)) from `shopify-pubsub-project.Data_Warehouse_Easyecom_Staging.Orderitem`) then suborder_id end ) as Source_pk_count
--   from `shopify-pubsub-project.Data_Warehouse_Easyecom_Staging.Orderitem`

 union all

  select 
  'ad_creatives' as table_name,
  'ad_creatives' as source_table,
  max(date(_airbyte_extracted_at)) as Source_max_date,
 count(distinct case when date(_airbyte_extracted_at) = (select max(date(_airbyte_extracted_at)) from `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.ad_creatives`) then id end ) as Source_pk_count
  from `shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ad_creatives`


 union all

  select 
  'ads_non_json' as table_name,
  'ads' as source_table,
  max(date(created_time)) as Source_max_date,
 count(distinct case when date(created_time) = (select max(date(created_time)) from `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.ads_non_json`) then id end ) as Source_pk_count
  from `shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads`

 union all

-- Since ads_recommendations table is derived from ads therefor count(distinct pk) of source and Stagingination are same
  select 
  'ads_recommendations' as table_name,
  'ads' as source_table,
  max(date(created_time)) as Source_max_date,
  null as Source_pk_count
  from `shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads`

 union all

-- Since ads_excluded_audience table is derived from ads therefor count(distinct pk) of source and Stagingination are same
  select 
  'ads_excluded_audience' as table_name,
  'ads' as source_table,
  max(date(created_time)) as Source_max_date,
  NULL as Source_pk_count
  from `shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads`

 union all

-- Since ads_insights_normal table is derived from ads_insights therefor count(distinct pk) of source and Stagingination are same
  select 
  'ads_insights_normal' as table_name,
  'ads' as source_table,
  max(date(date_start)) as Source_max_date,
 count(distinct case when date(date_start) = (select max(date(date_start)) from `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.ads_insights_normal`) then concat(ad_id,date_start) end ) as Source_pk_count
  from `shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads_insights`

--  union all

--   -- Since ads_insights_normal table is derived from ads_insights therefor count(distinct pk) of source and Stagingination are same
--   select 
--   'ads_insights_normal' as table_name,
--   max(date(date_start)) as Source_max_date,
--  count(distinct case when date(date_start) = (select max(date(date_start)) from `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.ads_insights_normal`) then concat(ad_id,date_start) end ) as Source_pk_count
--   from `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.ads_insights_normal`

  union all

  -- Since ads_insights_clicks table is derived from ads_insights therefor count(distinct pk) of source and Stagingination are same
  select 
  'ads_insights_clicks' as table_name,
  'ads_insights' as source_table,
  max(date(date_start)) as Source_max_date,
  NULL as Source_pk_count
  from `shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads_insights`

  union all

  -- Since ads_insights_clicks table is derived from ads_insights therefor count(distinct pk) of source and Stagingination are same
  select 
  'ads_insights_conversion_data' as table_name,
  'ads_insights' as source_table,
  max(date(date_start)) as Source_max_date,
  NULL as Source_pk_count
  from `shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads_insights`

  union all

  -- Since ads_insights_video_actions table is derived from ads_insights therefor count(distinct pk) of source and Stagingination are same
  select 
  'ads_insights_video_actions' as table_name,
  'ads_insights' as source_table,
  max(date(date_start)) as Source_max_date,
  NULL as Source_pk_count
  from `shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads_insights`

  union all

  -- Since ads_insights_video_p25_to_p100_details table is derived from ads_insights therefor count(distinct pk) of source and Stagingination are same
  select 
  'ads_insights_video_p25_to_p100_details' as table_name,
  'ads_insights' as source_table,
  max(date(date_start)) as Source_max_date,
  NULL as Source_pk_count
  from `shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads_insights`

  union all

  select 
  'ads_insights_action_carousel_card_conversion_values' as table_name,
  'ads_insights_action_carousel_card' as source_table,
  max(date(date_start)) as Source_max_date,
  NULL as Source_pk_count
  from `shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads_insights_action_carousel_card`

  union all

  select 
  'ads_insights_action_carousel_card_cost_per_conversion' as table_name,
  'ads_insights_action_carousel_card' as source_table,
  max(date(date_start)) as Source_max_date,
  NULL as Source_pk_count
  from `shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads_insights_action_carousel_card`

  union all

  select 
  'ads_insights_action_carousel_card_mobile_app_purchase_roas' as table_name,
  'ads_insights_action_carousel_card' as source_table,
  max(date(date_start)) as Source_max_date,
  NULL as Source_pk_count
  from `shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads_insights_action_carousel_card`

  union all

  select 
  'ads_insights_action_carousel_card_non_json' as table_name,
  'ads_insights_action_carousel_card' as source_table,
  max(date(date_start)) as Source_max_date,
 count(distinct case when date(date_start) = (select max(date(date_start)) from `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.ads_insights_action_carousel_card_non_json`) then concat(ad_id,date_start) end ) as Source_pk_count
  from `shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads_insights_action_carousel_card`

  union all

  select 
  'ads_insights_action_carousel_card_unique_actions_n_conversions_n_web_ctr' as table_name,
  'ads_insights_action_carousel_card' as source_table,
  max(date(date_start)) as Source_max_date,
  NULL as Source_pk_count
  from `shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads_insights_action_carousel_card`

  union all

  -- select 
  -- 'ads_insights_action_carousel_card' as table_name,
  -- max(date(date_start)) as Source_max_date,
  -- count(distinct case when date(date_start) = (select max(date(date_start)) from `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.ads_insights_action_carousel_card_unique_actions_n_conversions_n_web_ctr`) then NULL end ) as Source_pk_count
  -- from `shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads_insights_action_carousel_card`

  -- union all

  -- select 
  -- 'ads_insights' as table_name,
  -- max(date(date_start)) as Source_max_date,
  -- count(distinct case when date(date_start) = (select max(date(date_start)) from `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.activities`) then NULL end ) as Source_pk_count
  -- from `shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads_insights`

  -- union all

  select 
  'ads_insights_action_carousel_card_video_play_actions' as table_name,
  'ads_insights_action_carousel_card' as source_table,
  max(date(date_start)) as Source_max_date,
  NULL as Source_pk_count
  from `shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads_insights_action_carousel_card`

  union all

  select 
  'ads_insights_action_conversion_device_action_values' as table_name,
  'ads_insights_action_conversion_device' as source_table,
  max(date(date_start)) as Source_max_date,
  NULL as Source_pk_count
  from `shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads_insights_action_carousel_card`

  union all

  select 
  'ads_insights_action_conversion_device_actions' as table_name,
  'ads_insights_action_conversion_device' as source_table,
  max(date(date_start)) as Source_max_date,
  NULL as Source_pk_count
  from `shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads_insights_action_conversion_device`

  union all

  select 
  'ads_insights_action_conversion_device_cost_per_unique_action_type' as table_name,
  'ads_insights_action_conversion_device' as source_table,
  max(date(date_start)) as Source_max_date,
  NULL as Source_pk_count
  from `shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads_insights_action_conversion_device`


  union all

  select 
  'ads_insights_action_conversion_device_normal' as table_name,
  'ads_insights_action_conversion_device' as source_table,
  max(date_start) as Source_max_date,
  count(distinct case when date_start = (select max(date_start) from `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.ads_insights_action_conversion_device_normal`) then concat(ad_id,date_start,device_platform) end ) as Source_pk_count
  from`shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads_insights_action_conversion_device`

  union all

  select 
  'ads_insights_action_conversion_device_unique_actions' as table_name,
  'ads_insights_action_conversion_device' as source_table,
  max(date_start) as Source_max_date,
  NULL as Source_pk_count
  from `shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads_insights_action_conversion_device`

  union all

  select 
  'ads_insights_action_product_id_normal' as table_name,
  'ads_insights_action_product_id' as source_table,
  max(date_start) as Source_max_date,
 count(distinct case when date_start = (select max(date_start) from `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.ads_insights_action_product_id_normal`) then concat(ad_id,date_start,product_id) end ) as Source_pk_count
  from `shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads_insights_action_product_id`

  union all

  select 
  'ads_insights_action_product_id_action_values' as table_name,
  'ads_insights_action_product_id' as source_table,
  max(date_start) as Source_max_date,
  NULL as Source_pk_count
  from `shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads_insights_action_product_id`

  union all

  select 
  'ads_insights_action_product_id_actions' as table_name,
  'ads_insights_action_product_id' as source_table,
  max(date_start) as Source_max_date,
  NULL as Source_pk_count
  from `shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads_insights_action_product_id`


  union all

  select 
  'ads_insights_action_product_id_cost_per_action_type' as table_name,
  'ads_insights_action_product_id' as source_table,
  max(date_start) as Source_max_date,
  NULL as Source_pk_count
  from `shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads_insights_action_product_id`


  union all

  select 
  'ads_insights_action_product_id_purchase_roas' as table_name,
  'ads_insights_action_product_id' as source_table,
  max(date_start) as Source_max_date,
  NULL as Source_pk_count
  from `shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads_insights_action_product_id`


  union all

  select 
  'ads_insights_action_reaction_normal' as table_name,
  'ads_insights_action_reaction' as source_table,
  max(date_start) as Source_max_date,
 count(distinct case when date_start = (select max(date_start) from `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.ads_insights_action_reaction_normal`) then concat(ad_id,date_start) end ) as Source_pk_count
  from `shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads_insights_action_reaction`


  union all

  select 
  'ads_insights_action_reaction_action_values' as table_name,
  'ads_insights_action_reaction' as source_table,
  max(date_start) as Source_max_date,
  NULL as Source_pk_count
  from `shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads_insights_action_reaction`


  union all

  select 
  'ads_insights_action_reaction_actions' as table_name,
  'ads_insights_action_reaction' as source_table,
  max(date_start) as Source_max_date,
  NULL as Source_pk_count
  from `shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads_insights_action_reaction`


  union all

  select 
  'ads_insights_action_reaction_conversion_values' as table_name,
  'ads_insights_action_reaction' as source_table,
  max(date_start) as Source_max_date,
  NULL as Source_pk_count
  from `shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads_insights_action_reaction`


  union all

  select 
  'ads_insights_action_reaction_unique_actions' as table_name,
  'ads_insights_action_reaction' as source_table,
  max(date_start) as Source_max_date,
  NULL as Source_pk_count
  from `shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads_insights_action_reaction`


  union all

  select 
  'ads_insights_action_type_normal' as table_name,
  'ads_insights_action_type' as source_table,
  max(date_start) as Source_max_date,
  count(distinct case when date_start = (select max(date_start) from `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.ads_insights_action_type_normal`) then concat(ad_id,date_start) end ) as Source_pk_count
  from `shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads_insights_action_type`


  union all

  select 
  'ads_insights_action_type_action_values' as table_name,
  'ads_insights_action_type' as source_table,
  max(date_start) as Source_max_date,
  NULL as Source_pk_count
  from `shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads_insights_action_type`


  union all

  select 
  'ads_insights_action_type_actions' as table_name,
  'ads_insights_action_type' as source_table,
  max(date_start) as Source_max_date,
  NULL as Source_pk_count
  from `shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads_insights_action_type`


  union all

  select 
  'ads_insights_action_type_cost_per_action_type' as table_name,
  'ads_insights_action_type' as source_table,
  max(date_start) as Source_max_date,
  NULL as Source_pk_count
  from `shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads_insights_action_type`


  union all

  select 
  'ads_insights_action_type_cost_per_unique_action_type' as table_name,
  'ads_insights_action_type' as source_table,
  max(date_start) as Source_max_date,
  NULL as Source_pk_count
  from `shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads_insights_action_type`


  union all

  select 
  'ads_insights_action_type_unique_actions' as table_name,
  'ads_insights_action_type' as source_table,
  max(date_start) as Source_max_date,
  NULL as Source_pk_count
  from `shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads_insights_action_type`


  union all

  select 
  'ads_insights_action_video_sound_normal' as table_name,
  'ads_insights_action_video_sound' as source_table,
  max(date_start) as Source_max_date,
 count(distinct case when date_start = (select max(date_start) from `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.ads_insights_action_video_sound_normal`) then concat(ad_id,date_start) end ) as Source_pk_count
  from `shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads_insights_action_video_sound`


  union all

  select 
  'ads_insights_action_video_sound_action_values' as table_name,
  'ads_insights_action_video_sound' as source_table,
  max(date_start) as Source_max_date,
  NULL as Source_pk_count
  from `shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads_insights_action_video_sound`


  union all

  select 
  'ads_insights_action_video_sound_actions' as table_name,
  'ads_insights_action_video_sound' as source_table,
  max(date_start) as Source_max_date,
  NULL as Source_pk_count
  from `shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads_insights_action_video_sound`


  union all

  select 
  'ads_insights_action_video_type_normal' as table_name,
  'ads_insights_action_video_type' as source_table,
  max(date_start) as Source_max_date,
  count(distinct case when date_start = (select max(date_start) from `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.ads_insights_action_video_type_normal`) then concat(ad_id,date_start) end ) as Source_pk_count
  from `shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads_insights_action_video_type`


  union all

  select 
  'ads_insights_action_video_type_action_values' as table_name,
  'ads_insights_action_video_type' as source_table,
  max(date_start) as Source_max_date,
  NULL as Source_pk_count
  from `shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads_insights_action_video_type`


  union all

  select 
  'ads_insights_action_video_type_actions' as table_name,
  'ads_insights_action_video_type' as source_table,
  max(date_start) as Source_max_date,
  NULL as Source_pk_count
  from `shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads_insights_action_video_type`


  union all

  select 
  'ads_insights_action_video_type_unique_actions' as table_name,
  'ads_insights_action_video_type' as source_table,
  max(date_start) as Source_max_date,
  NULL as Source_pk_count
  from `shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads_insights_action_video_type`


  union all

  select 
  'ads_insights_age_and_gender_normal' as table_name,
  'ads_insights_age_and_gender' as source_table,
  max(date_start) as Source_max_date,
 count(distinct case when date_start = (select max(date_start) from `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.ads_insights_age_and_gender_normal`) then concat(ad_id,date_start,gender,age) end ) as Source_pk_count
  from `shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads_insights_age_and_gender`


  union all

  select 
  'ads_insights_age_and_gender_action_values' as table_name,
  'ads_insights_age_and_gender' as source_table,
  max(date_start) as Source_max_date,
  NULL as Source_pk_count
  from `shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads_insights_age_and_gender`


  union all

  select 
  'ads_insights_age_and_gender_actions' as table_name,
  'ads_insights_age_and_gender' as source_table,
  max(date_start) as Source_max_date,
  NULL as Source_pk_count
  from `shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads_insights_age_and_gender`


  union all

  select 
  'ads_insights_age_and_gender_unique_actions' as table_name,
  'ads_insights_age_and_gender' as source_table,
  max(date_start) as Source_max_date,
  NULL as Source_pk_count
  from `shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads_insights_age_and_gender`


  union all

  select 
  'ads_insights_country_normal' as table_name,
  'ads_insights_country' as source_table,
  max(date_start) as Source_max_date,
  count(distinct case when date_start = (select max(date_start) from `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.ads_insights_country_normal`) then concat(ad_id,date_start,country) end ) as Source_pk_count
  from `shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads_insights_country`


  union all

  select 
  'ads_insights_country_action_values' as table_name,
  'ads_insights_country' as source_table,
  max(date_start) as Source_max_date,
  NULL as Source_pk_count
  from `shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads_insights_country`


  union all

  select 
  'ads_insights_country_actions' as table_name,
  'ads_insights_country' as source_table,
  max(date_start) as Source_max_date,
  NULL as Source_pk_count
  from `shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads_insights_country`


  union all

  select 
  'ads_insights_country_unique_actions' as table_name,
  'ads_insights_country' as source_table,
  max(date_start) as Source_max_date,
  NULL as Source_pk_count
  from `shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads_insights_country`


  union all

  select 
  'ads_insights_delivery_device_normal' as table_name,
  'ads_insights_delivery_device' as source_table,
  max(date_start) as Source_max_date,
  count(distinct case when date_start = (select max(date_start) from `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.ads_insights_delivery_device_normal`) then concat(ad_id,date_start,device_platform) end ) as Source_pk_count
  from `shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads_insights_delivery_device`


  union all

  select 
  'ads_insights_delivery_device_actions' as table_name,
  'ads_insights_delivery_device' as source_table,
  max(date_start) as Source_max_date,
  NULL as Source_pk_count
  from `shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads_insights_delivery_device`


  union all

  select 
  'ads_insights_delivery_device_action_values' as table_name,
  'ads_insights_delivery_device' as source_table,
  max(date_start) as Source_max_date,
  NULL as Source_pk_count
  from `shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads_insights_delivery_device`


  union all

  select 
  'ads_insights_delivery_device_cost_per_unique_action_type' as table_name,
  'ads_insights_delivery_device' as source_table,
  max(date_start) as Source_max_date,
  NULL as Source_pk_count
  from `shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads_insights_delivery_device`


  union all

  select 
  'ads_insights_delivery_device_unique_actions' as table_name,
  'ads_insights_delivery_device' as source_table,
  max(date_start) as Source_max_date,
  NULL as Source_pk_count
  from `shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads_insights_delivery_device`


  union all

  select 
  'ads_insights_delivery_platform_normal' as table_name,
  'ads_insights_delivery_platform' as source_table,
  max(date_start) as Source_max_date,
  count(distinct case when date_start = (select max(date_start) from `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.ads_insights_delivery_platform_normal`) then concat(ad_id,date_start,publisher_platform) end ) as Source_pk_count
  from `shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads_insights_delivery_platform`


  union all

  select 
  'ads_insights_delivery_platform_and_device_platform_normal' as table_name,
  'ads_insights_delivery_platform_and_device_platform' as source_table,
  max(date_start) as Source_max_date,
  count(distinct case when date_start = (select max(date_start) from `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.ads_insights_delivery_platform_and_device_platform_normal`) then concat(ad_id,date_start,publisher_platform,device_platform) end ) as Source_pk_count
  from `shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads_insights_delivery_platform_and_device_platform`


  union all

  select 
  'ads_insights_delivery_platform_and_device_platform_actions' as table_name,
  'ads_insights_delivery_platform_and_device_platform' as source_table,
  max(date_start) as Source_max_date,
  NULL as Source_pk_count
  from `shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads_insights_delivery_platform_and_device_platform`


  union all

  select 
  'ads_insights_delivery_platform_and_device_platform_unique_actions' as table_name,
  'ads_insights_delivery_platform_and_device_platform' as source_table,
  max(date_start) as Source_max_date,
  NULL as Source_pk_count
  from`shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads_insights_delivery_platform_and_device_platform`


  union all

  select 
  'ads_insights_delivery_platform_and_device_platform_cost_per_action_type' as table_name,
  'ads_insights_delivery_platform_and_device_platform' as source_table,
  max(date_start) as Source_max_date,
  NULL as Source_pk_count
  from `shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads_insights_delivery_platform_and_device_platform`


  union all

  select 
  'ads_insights_delivery_platform_unique_actions' as table_name,
  'ads_insights_delivery_platform' as source_table,
  max(date_start) as Source_max_date,
  NULL as Source_pk_count
  from `shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads_insights_delivery_platform`


  union all

  select 
  'ads_insights_delivery_platform_cost_per_action_type' as table_name,
  'ads_insights_delivery_platform' as source_table,
  max(date_start) as Source_max_date,
  NULL as Source_pk_count
  from`shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads_insights_delivery_platform`



  union all

  select 
  'ads_insights_delivery_platform_actions' as table_name,
  'ads_insights_delivery_platform' as source_table,
  max(date_start) as Source_max_date,
  NULL as Source_pk_count
  from `shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads_insights_delivery_platform`


  union all

  select 
  'ads_insights_delivery_platform_action_values' as table_name,
  'ads_insights_delivery_platform' as source_table,
  max(date_start) as Source_max_date,
  NULL as Source_pk_count
  from `shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads_insights_delivery_platform`


  union all

  select 
  'ads_insights_delivery_platform_cost_per_unique_action_type' as table_name,
  'ads_insights_delivery_platform' as source_table,
  max(date_start) as Source_max_date,
  NULL as Source_pk_count
  from `shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads_insights_delivery_platform`


  union all

  select 
  'ads_insights_demographics_age_normal' as table_name,
  'ads_insights_demographics_age' as source_table,
  max(date_start) as Source_max_date,
  count(distinct case when date_start = (select max(date_start) from `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.ads_insights_demographics_age_normal`) then concat(ad_id,date_start,age) end ) as Source_pk_count
  from `shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads_insights_demographics_age`


  union all

  select 
  'ads_insights_demographics_age_actions' as table_name,
  'ads_insights_demographics_age' as source_table,
  max(date_start) as Source_max_date,
  NULL as Source_pk_count
  from `shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads_insights_demographics_age`


  union all

  select 
  'ads_insights_demographics_age_action_values' as table_name,
  'ads_insights_demographics_age' as source_table,
  max(date_start) as Source_max_date,
  NULL as Source_pk_count
  from `shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads_insights_demographics_age`


  union all

  select 
  'ads_insights_demographics_age_unique_actions' as table_name,
  'ads_insights_demographics_age' as source_table,
  max(date_start) as Source_max_date,
  NULL as Source_pk_count
  from `shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads_insights_demographics_age`


  union all

  select 
  'ads_insights_demographics_country_normal' as table_name,
  'ads_insights_demographics_country' as source_table,
  max(date_start) as Source_max_date,
  count(distinct case when date_start = (select max(date_start) from `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.ads_insights_demographics_country_normal`) then concat(ad_id,date_start,country) end ) as Source_pk_count
  from `shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads_insights_demographics_country`


  union all

  select 
  'ads_insights_demographics_country_actions' as table_name,
  'ads_insights_demographics_country' as source_table,
  max(date_start) as Source_max_date,
  NULL as Source_pk_count
  from `shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads_insights_demographics_country`


  union all

  select 
  'ads_insights_demographics_country_action_values' as table_name,
  'ads_insights_demographics_country' as source_table,
  max(date_start) as Source_max_date,
  NULL as Source_pk_count
  from `shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads_insights_demographics_country`


  union all

  select 
  'ads_insights_demographics_dma_region_normal' as table_name,
  'ads_insights_demographics_dma_region' as source_table,
  max(date_start) as Source_max_date,
  count(distinct case when date_start = (select max(date_start) from `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.ads_insights_demographics_dma_region_normal`) then concat(ad_id,date_start,dma) end ) as Source_pk_count
  from `shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads_insights_demographics_dma_region`
),

---------------------------------------------------------------------------------------------------------------------------------------------------------
Staging as 
(
  select 
  'ads_insights_demographics_dma_region_normal' as table_name,
  max(date_start) as Staging_max_date,
  max(date_start) as Latest_Valid_Date,
  count(distinct case when date_start = (select max(date_start) from `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.ads_insights_demographics_dma_region_normal`) then concat(ad_id,date_start,dma) end ) as Staging_pk_count
  from `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.ads_insights_demographics_dma_region_normal`

  union all

  select 
  'ads_insights_demographics_country_action_values' as table_name,
  max(date_start) as Staging_max_date,
  (SELECT MAX(DATE(date_start)) AS max_valid_date FROM `shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads_insights` 
  WHERE action_values IS NOT NULL
  ) as Latest_Valid_Date,
  NULL as Staging_pk_count
  from `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.ads_insights_demographics_country_action_values`

  union all

  select 
  'ads_insights_demographics_country_actions' as table_name,
  max(date_start) as Staging_max_date,
  (SELECT MAX(DATE(date_start)) AS max_valid_date FROM `shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads_insights_demographics_country` 
  WHERE actions IS NOT NULL
  ) as Latest_Valid_Date,
  NULL as Staging_pk_count
  from `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.ads_insights_demographics_country_actions`

  union all

  select 
  'ads_insights_demographics_country_normal' as table_name,
  max(date_start) as Staging_max_date,
  max(date_start) as Latest_Valid_Date,
  count(distinct case when date_start = (select max(date_start) from `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.ads_insights_demographics_country_normal`) then concat(ad_id,date_start,country) end ) as Staging_pk_count
  from `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.ads_insights_demographics_country_normal`

  union all

  select 
  'ads_insights_demographics_age_unique_actions' as table_name,
  max(date_start) as Staging_max_date,
  (SELECT MAX(DATE(date_start)) AS max_valid_date FROM `shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads_insights_demographics_age` 
  WHERE unique_actions IS NOT NULL
  ) as Latest_Valid_Date,
  NULL as Staging_pk_count
  from `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.ads_insights_demographics_age_unique_actions`

  union all

  select 
  'ads_insights_demographics_age_action_values' as table_name,
  max(date_start) as Staging_max_date,
  (SELECT MAX(DATE(date_start)) AS max_valid_date FROM `shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads_insights_demographics_age` 
  WHERE action_values IS NOT NULL
  ) as Latest_Valid_Date,
  NULL as Staging_pk_count
  from `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.ads_insights_demographics_age_action_values`

  union all

  select 
  'ads_insights_demographics_age_actions' as table_name,
  max(date_start) as Staging_max_date,
  (SELECT MAX(DATE(date_start)) AS max_valid_date FROM `shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads_insights_demographics_age` 
  WHERE actions IS NOT NULL
  ) as Latest_Valid_Date,
  NULL as Staging_pk_count
  from `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.ads_insights_demographics_age_actions`

  union all

  select 
  'ads_insights_demographics_age_normal' as table_name,
  max(date_start) as Staging_max_date,
  max(date_start) as Latest_Valid_Date,
  count(distinct case when date_start = (select max(date_start) from `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.ads_insights_demographics_age_normal`) then concat(ad_id,date_start,age) end ) as Staging_pk_count
  from `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.ads_insights_demographics_age_normal`

  union all

  select 
  'ads_insights_delivery_platform_cost_per_unique_action_type' as table_name,
  max(date_start) as Staging_max_date,
  (SELECT MAX(DATE(date_start)) AS max_valid_date FROM `shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads_insights_delivery_platform` 
  WHERE cost_per_unique_action_type IS NOT NULL
  ) as Latest_Valid_Date,
  NULL as Staging_pk_count
  from `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.ads_insights_delivery_platform_cost_per_unique_action_type`

  union all

  select 
  'ads_insights_delivery_platform_action_values' as table_name,
  max(date_start) as Staging_max_date,
  (SELECT MAX(DATE(date_start)) AS max_valid_date FROM `shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads_insights_delivery_platform` 
  WHERE action_values IS NOT NULL
  ) as Latest_Valid_Date,
  NULL as Staging_pk_count
  from `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.ads_insights_delivery_platform_action_values`

  union all

  select 
  'ads_insights_delivery_platform_actions' as table_name,
  max(date_start) as Staging_max_date,
  (SELECT MAX(DATE(date_start)) AS max_valid_date FROM `shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads_insights_delivery_platform` 
  WHERE actions IS NOT NULL
  ) as Latest_Valid_Date,
  NULL as Staging_pk_count
  from `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.ads_insights_delivery_platform_actions`

  union all

  select 
  'ads_insights_delivery_platform_cost_per_action_type' as table_name,
  max(date_start) as Staging_max_date,
  (SELECT MAX(DATE(date_start)) AS max_valid_date FROM `shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads_insights_delivery_platform` 
  WHERE cost_per_action_type IS NOT NULL
  ) as Latest_Valid_Date,
  NULL as Staging_pk_count
  from `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.ads_insights_delivery_platform_cost_per_action_type`

  union all

  select 
  'ads_insights_delivery_platform_unique_actions' as table_name,
  max(date_start) as Staging_max_date,
  (SELECT MAX(DATE(date_start)) AS max_valid_date FROM `shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads_insights_delivery_platform` 
  WHERE unique_actions IS NOT NULL
  ) as Latest_Valid_Date,
  NULL as Staging_pk_count
  from `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.ads_insights_delivery_platform_unique_actions`

  union all

  select 
  'ads_insights_delivery_platform_and_device_platform_cost_per_action_type' as table_name,
  max(date_start) as Staging_max_date,
  (SELECT MAX(DATE(date_start)) AS max_valid_date FROM `shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads_insights_delivery_platform_and_device_platform` 
  WHERE cost_per_action_type IS NOT NULL
  ) as Latest_Valid_Date,
  NULL as Staging_pk_count
  from `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.ads_insights_delivery_platform_and_device_platform_cost_per_action_type`

  union all

  select 
  'ads_insights_delivery_platform_and_device_platform_unique_actions' as table_name,
  max(date_start) as Staging_max_date,
  (SELECT MAX(DATE(date_start)) AS max_valid_date FROM `shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads_insights_delivery_platform_and_device_platform` 
  WHERE unique_actions IS NOT NULL
  ) as Latest_Valid_Date,
  NULL as Staging_pk_count
  from `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.ads_insights_delivery_platform_and_device_platform_unique_actions`

  union all

  select 
  'ads_insights_delivery_platform_and_device_platform_actions' as table_name,
  max(date_start) as Staging_max_date,
  (SELECT MAX(DATE(date_start)) AS max_valid_date FROM `shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads_insights_delivery_platform_and_device_platform` 
  WHERE actions IS NOT NULL
  ) as Latest_Valid_Date,
  NULL as Staging_pk_count
  from `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.ads_insights_delivery_platform_and_device_platform_actions`

  union all

  select 
  'ads_insights_delivery_platform_and_device_platform_normal' as table_name,
  max(date_start) as Staging_max_date,
  (SELECT MAX(DATE(date_start)) AS max_valid_date FROM `shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads_insights_delivery_platform_and_device_platform` 
  WHERE actions IS NOT NULL
  ) as Latest_Valid_Date,
  count(distinct case when date_start = (select max(date_start) from `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.ads_insights_delivery_platform_and_device_platform_normal`) then concat(ad_id,date_start,publisher_platform,device_platform) end ) as Staging_pk_count
  from `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.ads_insights_delivery_platform_and_device_platform_normal`

  union all

  select 
  'ads_insights_delivery_platform_normal' as table_name,
  max(date_start) as Staging_max_date,
  max(date_start) as Latest_Valid_Date,
  count(distinct case when date_start = (select max(date_start) from `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.ads_insights_delivery_platform_normal`) then concat(ad_id,date_start,publisher_platform) end ) as Staging_pk_count
  from `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.ads_insights_delivery_platform_normal`

  union all

  select 
  'ads_insights_delivery_device_unique_actions' as table_name,
  max(date_start) as Staging_max_date,
  (SELECT MAX(DATE(date_start)) AS max_valid_date FROM `shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads_insights_delivery_device` 
  WHERE actions IS NOT NULL
  ) as Latest_Valid_Date,
  NULL as Staging_pk_count
  from `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.ads_insights_delivery_device_unique_actions`

  union all

  select 
  'ads_insights_delivery_device_cost_per_unique_action_type' as table_name,
  max(date_start) as Staging_max_date,
  (SELECT MAX(DATE(date_start)) AS max_valid_date FROM `shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads_insights_delivery_device` 
  WHERE cost_per_unique_action_type IS NOT NULL
  ) as Latest_Valid_Date,
  NULL as Staging_pk_count
  from `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.ads_insights_delivery_device_cost_per_unique_action_type`

  union all

  select 
  'ads_insights_delivery_device_action_values' as table_name,
  max(date_start) as Staging_max_date,
  (SELECT MAX(DATE(date_start)) AS max_valid_date FROM `shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads_insights_delivery_device` 
  WHERE action_values IS NOT NULL
  ) as Latest_Valid_Date,
  NULL as Staging_pk_count
  from `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.ads_insights_delivery_device_action_values`

  union all

  select 
  'ads_insights_delivery_device_actions' as table_name,
  max(date_start) as Staging_max_date,
  (SELECT MAX(DATE(date_start)) AS max_valid_date FROM `shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads_insights_delivery_device` 
  WHERE actions IS NOT NULL
  ) as Latest_Valid_Date,
  NULL as Staging_pk_count
  from `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.ads_insights_delivery_device_actions`

  union all

  select 
  'ads_insights_delivery_device_normal' as table_name,
  max(date_start) as Staging_max_date,
  max(date_start) as Latest_Valid_Date,
  count(distinct case when date_start = (select max(date_start) from `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.ads_insights_delivery_device_normal`) then concat(ad_id,date_start,device_platform) end ) as Staging_pk_count
  from `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.ads_insights_delivery_device_normal`

  union all

  select 
  'ads_insights_country_unique_actions' as table_name,
  max(date_start) as Staging_max_date,
  (SELECT MAX(DATE(date_start)) AS max_valid_date FROM `shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads_insights_country` 
  WHERE unique_actions IS NOT NULL
  ) as Latest_Valid_Date,
  NULL as Staging_pk_count
  from `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.ads_insights_country_unique_actions`

  union all

  select 
  'ads_insights_country_actions' as table_name,
  max(date_start) as Staging_max_date,
  (SELECT MAX(DATE(date_start)) AS max_valid_date FROM `shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads_insights_country` 
  WHERE actions IS NOT NULL
  ) as Latest_Valid_Date,
  NULL as Staging_pk_count
  from `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.ads_insights_country_actions`

  union all

  select 
  'ads_insights_country_action_values' as table_name,
  max(date_start) as Staging_max_date,
  (SELECT MAX(DATE(date_start)) AS max_valid_date FROM `shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads_insights_country` 
  WHERE action_values IS NOT NULL
  ) as Latest_Valid_Date,
  NULL as Staging_pk_count
  from `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.ads_insights_country_action_values`

  union all

  select 
  'ads_insights_country_normal' as table_name,
  max(date_start) as Staging_max_date,
  max(date_start) as Latest_Valid_Date,
  count(distinct case when date_start = (select max(date_start) from `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.ads_insights_country_normal`) then concat(ad_id,date_start,country) end ) as Staging_pk_count
  from `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.ads_insights_country_normal`

  union all

  select 
  'ads_insights_age_and_gender_unique_actions' as table_name,
  max(date_start) as Staging_max_date,
  (SELECT MAX(DATE(date_start)) AS max_valid_date FROM `shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads_insights_age_and_gender` 
  WHERE unique_actions IS NOT NULL
  ) as Latest_Valid_Date,
  NULL as Staging_pk_count
  from `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.ads_insights_age_and_gender_unique_actions`

  union all

  select 
  'ads_insights_age_and_gender_actions' as table_name,
  max(date_start) as Staging_max_date,
  (SELECT MAX(DATE(date_start)) AS max_valid_date FROM `shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads_insights_age_and_gender` 
  WHERE actions IS NOT NULL
  ) as Latest_Valid_Date,
  NULL as Staging_pk_count
  from `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.ads_insights_age_and_gender_actions`

  union all

  select 
  'ads_insights_age_and_gender_action_values' as table_name,
  max(date_start) as Staging_max_date,
  (SELECT MAX(DATE(date_start)) AS max_valid_date FROM `shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads_insights_age_and_gender` 
  WHERE action_values IS NOT NULL
  ) as Latest_Valid_Date,
  NULL as Staging_pk_count
  from `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.ads_insights_age_and_gender_action_values`

  union all

  select 
  'ads_insights_age_and_gender_normal' as table_name,
  max(date_start) as Staging_max_date,
  max(date_start) as Latest_Valid_Date,
  count(distinct case when date_start = (select max(date_start) from `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.ads_insights_age_and_gender_normal`) then concat(ad_id,date_start,gender,age) end ) as Staging_pk_count
  from `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.ads_insights_age_and_gender_normal`

  union all

  select 
  'ads_insights_action_video_type_unique_actions' as table_name,
  max(date_start) as Staging_max_date,
  (SELECT MAX(DATE(date_start)) AS max_valid_date FROM `shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads_insights_action_video_type` 
  WHERE unique_actions IS NOT NULL
  ) as Latest_Valid_Date,
  NULL as Staging_pk_count
  from `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.ads_insights_action_video_type_unique_actions`

  union all

  select 
  'ads_insights_action_video_type_actions' as table_name,
  max(date_start) as Staging_max_date,
  (SELECT MAX(DATE(date_start)) AS max_valid_date FROM `shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads_insights_action_video_type` 
  WHERE actions IS NOT NULL
  ) as Latest_Valid_Date,
  NULL as Staging_pk_count
  from `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.ads_insights_action_video_type_actions`

  union all

  select 
  'ads_insights_action_video_type_action_values' as table_name,
  max(date_start) as Staging_max_date,
  (SELECT MAX(DATE(date_start)) AS max_valid_date FROM `shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads_insights_action_video_type` 
  WHERE action_values IS NOT NULL
  ) as Latest_Valid_Date,
  NULL as Staging_pk_count
  from `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.ads_insights_action_video_type_action_values`

  -- union all

  -- select 
  -- 'ads_insights' as table_name,
  -- max(date(date_start)) as Staging_max_date,
  -- count(distinct case when date(date_start) = (select max(date(date_start)) from `shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads_insights`) then NULL end ) as Staging_pk_count
  -- from  `shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads_insights_action_carousel_card`

  union all

  select 
  'ads_insights_action_video_type_normal' as table_name,
  max(date_start) as Staging_max_date,
  max(date_start) as Latest_Valid_Date,
  count(distinct case when date_start = (select max(date_start) from `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.ads_insights_action_video_type_normal`) then concat(ad_id,date_start) end ) as Staging_pk_count
  from `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.ads_insights_action_video_type_normal`

  union all

  -- select 
  -- 'ads_insights_action_carousel_card' as table_name,
  -- max(date(date_start)) as Staging_max_date,
  -- (SELECT MAX(DATE(date_start)) AS max_valid_date FROM `shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads_insights_action_video_type` 
  -- WHERE action_values IS NOT NULL
  -- ) as Latest_Valid_Date,
  -- count(distinct case when date(date_start) = (select max(date(date_start)) from `shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads_insights_action_carousel_card`) then NULL end ) as Staging_pk_count
  -- from  `shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads_insights_action_carousel_card`

  -- union all

  select 
  'ads_insights_action_carousel_card_unique_actions_n_conversions_n_web_ctr' as table_name,
  max(date(date_start)) as Staging_max_date,
  (SELECT MAX(DATE(date_start)) AS max_valid_date FROM `shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads_insights_action_carousel_card` 
  WHERE conversions IS NOT NULL
  and unique_actions IS NOT NULL
  ) as Latest_Valid_Date,
  NULL as Staging_pk_count
  from `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.ads_insights_action_carousel_card_unique_actions_n_conversions_n_web_ctr`

  union all

  select 
  'activities' as table_name,
  max(date(event_time)) as Staging_max_date,
  max(date(event_time)) as Latest_Valid_Date,
  count(distinct case when date(event_time) = (select max(date(event_time)) from `shopify-pubsub-project.pilgrim_bi_airbyte_facebook.activities`) then concat(object_id, event_time, account_id) end ) as Staging_pk_count
  from  `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.activities`


--   union all

--   select 
--   'Order_item' as table_name,
--   max(order_date) as Staging_max_date,
--  count(distinct case when date(order_date) = (select max(date(order_date)) from `shopify-pubsub-project.Data_Warehouse_Easyecom_Staging.Orderitem`) then suborder_id end ) as Staging_pk_count
--   from `shopify-pubsub-project.Data_Warehouse_Easyecom_Staging.Orderitem`

  union all
  select
  'ad_creatives' as table_name,
  max(date(_airbyte_extracted_at)) as Staging_max_date,
  max(date(_airbyte_extracted_at)) as Latest_Valid_Date,
  count(distinct case when date(_airbyte_extracted_at) = (select max(date(_airbyte_extracted_at)) from `shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ad_creatives`) then id end ) as Staging_pk_count
  from  `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.ad_creatives`


  union all

  select
  'ads_non_json' as table_name,
  max(date(created_time)) as Staging_max_date,
  max(date(_airbyte_extracted_at)) as Latest_Valid_Date,
  count(distinct case when date(created_time) = (select max(date(created_time)) from `shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads`) then id end ) as Staging_pk_count
  from  `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.ads_non_json`

  union all

  select 
  'ads_recommendations' as table_name,
  max(date(created_time)) as Staging_max_date,
  (SELECT MAX(DATE(created_time)) AS max_valid_date FROM `shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads` WHERE recommendations IS NOT NULL) as Latest_Valid_Date,
 NULL as Staging_pk_count
  from `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.ads_recommendations`

  union all

  select 
  'ads_excluded_audience' as table_name,
  max(date(created_time)) as Staging_max_date,
  (SELECT MAX(DATE(created_time)) AS max_valid_date FROM `shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads` WHERE targeting IS NOT NULL) as Latest_Valid_Date,
  NULL as Staging_pk_count
  from `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.ads_excluded_audience`

  union all

  select 
  'ads_insights_normal' as table_name,
  max(date(date_start)) as Staging_max_date,
  max(date(date_start)) as Latest_Valid_Date,
  count(distinct case when date(date_start) = (select max(date(date_start)) from `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.ads_insights_normal`) then concat(ad_id,date_start) end ) as Staging_pk_count
  from `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.ads_insights_normal`

  union all

  select 
  'ads_insights_clicks' as table_name,
  max(date(date_start)) as Staging_max_date,
  (SELECT MAX(DATE(date_start)) AS max_valid_date FROM `shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads_insights` 
  WHERE outbound_clicks IS NOT NULL
  AND unique_outbound_clicks IS NOT NULL) as Latest_Valid_Date,
  NULL as Staging_pk_count
  from `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.ads_insights_clicks`

  union all

  select 
  'ads_insights_conversion_data' as table_name,
  max(date(date_start)) as Staging_max_date,
  (SELECT MAX(DATE(date_start)) AS max_valid_date FROM `shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads_insights` 
  WHERE conversion_values IS NOT NULL
  and conversions is not null
  and cost_per_conversion is not null
  and website_ctr is not null) as Latest_Valid_Date,
  NULL as Staging_pk_count
  from `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.ads_insights_conversion_data`

  union all

  select 
  'ads_insights_video_actions' as table_name,
  max(date(date_start)) as Staging_max_date,
  (SELECT MAX(DATE(date_start)) AS max_valid_date FROM `shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads_insights` 
  WHERE video_play_actions IS NOT NULL
  or video_play_curve_actions is not null
  or unique_actions is not null
  or action_values is not null) as Latest_Valid_Date,
  NULL as Staging_pk_count
  from `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.ads_insights_video_actions`

  union all

  select 
  'ads_insights_video_p25_to_p100_details' as table_name,
  max(date(date_start)) as Staging_max_date,
  (SELECT MAX(DATE(date_start)) AS max_valid_date FROM `shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads_insights` 
  WHERE video_p100_watched_actions IS NOT NULL
  or video_p25_watched_actions is not null
  or video_p50_watched_actions is not null
  or video_p75_watched_actions is not null
  or video_p75_watched_actions is not null) as Latest_Valid_Date,
  NULL as Staging_pk_count
  from `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.ads_insights_video_p25_to_p100_details`

  union all

  select 
  'ads_insights_action_carousel_card_conversion_values' as table_name,
  max(date(date_start)) as Staging_max_date,
  (SELECT MAX(DATE(date_start)) AS max_valid_date FROM `shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads_insights_action_carousel_card` WHERE conversion_values IS NOT NULL) as Latest_Valid_Date,
  null as Staging_pk_count
  from `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.ads_insights_action_carousel_card_conversion_values`

  union all

  select 
  'ads_insights_action_carousel_card_cost_per_conversion' as table_name,
  max(date(date_start)) as Staging_max_date,
  (SELECT MAX(DATE(date_start)) AS max_valid_date FROM `shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads_insights_action_carousel_card` WHERE cost_per_conversion IS NOT NULL) as Latest_Valid_Date,
  NULL as Staging_pk_count
  from `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.ads_insights_action_carousel_card_cost_per_conversion`

   union all

  select 
  'ads_insights_action_carousel_card_mobile_app_purchase_roas' as table_name,
  max(date(date_start)) as Staging_max_date,
  (SELECT MAX(DATE(date_start)) AS max_valid_date FROM `shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads_insights_action_carousel_card` WHERE mobile_app_purchase_roas IS NOT NULL) as Latest_Valid_Date,
  NULL as Staging_pk_count
  from `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.ads_insights_action_carousel_card_mobile_app_purchase_roas`

  union all

  select 
  'ads_insights_action_carousel_card_non_json' as table_name,
  max(date(date_start)) as Staging_max_date,
  max(date(date_start)) as Latest_Valid_Date,
  count(distinct case when date(date_start) = (select max(date(date_start)) from `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.ads_insights_action_carousel_card_non_json`) then concat(ad_id,date_start) end ) as Staging_pk_count
  from `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.ads_insights_action_carousel_card_non_json`

  union all

  select 
  'ads_insights_action_carousel_card_video_play_actions' as table_name,
  max(date(date_start)) as Staging_max_date,
  (SELECT MAX(DATE(date_start)) AS max_valid_date FROM `shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads_insights_action_carousel_card` WHERE video_play_actions IS NOT NULL) as Latest_Valid_Date,
  NULL as Staging_pk_count
  from `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.ads_insights_action_carousel_card_video_play_actions`

  union all

  select 
  'ads_insights_action_conversion_device_action_values' as table_name,
  max(date(date_start)) as Staging_max_date,
  (SELECT MAX(DATE(date_start)) AS max_valid_date FROM `shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads_insights_action_conversion_device` WHERE action_values IS NOT NULL) as Latest_Valid_Date,
  NULL as Staging_pk_count
  from `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.ads_insights_action_conversion_device_action_values`

  union all

  select 
  'ads_insights_action_conversion_device_actions' as table_name,
  max(date(date_start)) as Staging_max_date,
  (SELECT MAX(DATE(date_start)) AS max_valid_date FROM `shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads_insights_action_conversion_device` WHERE actions IS NOT NULL) as Latest_Valid_Date,
  NULL as Staging_pk_count
  from `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.ads_insights_action_conversion_device_actions`


  union all

  select 
  'ads_insights_action_conversion_device_cost_per_unique_action_type' as table_name,
  max(date(date_start)) as Staging_max_date,
  (SELECT MAX(DATE(date_start)) AS max_valid_date FROM `shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads_insights_action_conversion_device` WHERE cost_per_unique_action_type IS NOT NULL) as Latest_Valid_Date,
  NULL as Staging_pk_count
  from `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.ads_insights_action_conversion_device_cost_per_unique_action_type`


  union all

  select 
  'ads_insights_action_conversion_device_normal' as table_name,
  max(date_start) as Staging_max_date,
  max(date_start) as Latest_Valid_Date,
  count(distinct case when date_start = (select max(date_start) from `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.ads_insights_action_conversion_device_normal`) then concat(ad_id,date_start,device_platform) end ) as Staging_pk_count
  from `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.ads_insights_action_conversion_device_normal`


  union all

  select 
  'ads_insights_action_conversion_device_unique_actions' as table_name,
  max(date_start) as Staging_max_date,
  (SELECT MAX(DATE(date_start)) AS max_valid_date FROM `shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads_insights_action_conversion_device` WHERE unique_actions IS NOT NULL) as Latest_Valid_Date,
  NULL as Staging_pk_count
  from `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.ads_insights_action_conversion_device_unique_actions`

  union all

  select 
  'ads_insights_action_product_id_normal' as table_name,
  max(date_start) as Staging_max_date,
  max(date_start) as Latest_Valid_Date,
  count(distinct case when date_start = (select max(date_start) from `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.ads_insights_action_product_id_normal`) then concat(ad_id,date_start,product_id) end ) as Staging_pk_count
  from `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.ads_insights_action_product_id_normal`

  union all

  select 
  'ads_insights_action_product_id_action_values' as table_name,
  max(date_start) as Staging_max_date,
  (SELECT MAX(DATE(date_start)) AS max_valid_date FROM `shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads_insights_action_product_id` WHERE action_values IS NOT NULL) as Latest_Valid_Date,
  NULL as Staging_pk_count
  from `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.ads_insights_action_product_id_action_values`

  union all

  select 
  'ads_insights_action_product_id_actions' as table_name,
  max(date_start) as Staging_max_date,
  (SELECT MAX(DATE(date_start)) AS max_valid_date FROM `shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads_insights_action_product_id` WHERE action_values IS NOT NULL) as Latest_Valid_Date,
  NULL as Staging_pk_count
  from `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.ads_insights_action_product_id_actions`


  union all

  select 
  'ads_insights_action_product_id_cost_per_action_type' as table_name,
  max(date_start) as Staging_max_date,
  (SELECT MAX(DATE(date_start)) AS max_valid_date FROM `shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads_insights_action_product_id` WHERE cost_per_action_type IS NOT NULL) as Latest_Valid_Date,
  NULL as Staging_pk_count
  from `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.ads_insights_action_product_id_cost_per_action_type`


  union all

  select 
  'ads_insights_action_product_id_purchase_roas' as table_name,
  max(date_start) as Staging_max_date,
  (SELECT MAX(DATE(date_start)) AS max_valid_date FROM `shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads_insights_action_product_id` WHERE purchase_roas IS NOT NULL) as Latest_Valid_Date,
  NULL as Staging_pk_count
  from `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.ads_insights_action_product_id_purchase_roas`


  union all

  select 
  'ads_insights_action_reaction_normal' as table_name,
  max(date_start) as Staging_max_date,
  max(date_start) as Latest_Valid_Date,
  count(distinct case when date_start = (select max(date_start) from `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.ads_insights_action_reaction_normal`) then concat(ad_id,date_start) end ) as Staging_pk_count
  from `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.ads_insights_action_reaction_normal`


  union all

  select 
  'ads_insights_action_reaction_action_values' as table_name,
  max(date_start) as Staging_max_date,
  (SELECT MAX(DATE(date_start)) AS max_valid_date FROM `shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads_insights_action_reaction` WHERE action_values IS NOT NULL) as Latest_Valid_Date,
  NULL as Staging_pk_count
  from `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.ads_insights_action_reaction_action_values`


  union all

  select 
  'ads_insights_action_reaction_actions' as table_name,
  max(date_start) as Staging_max_date,
  (SELECT MAX(DATE(date_start)) AS max_valid_date FROM `shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads_insights_action_reaction` WHERE actions IS NOT NULL) as Latest_Valid_Date,
  NULL as Staging_pk_count
  from `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.ads_insights_action_reaction_actions`


  union all

  select 
  'ads_insights_action_reaction_conversion_values' as table_name,
  max(date_start) as Staging_max_date,
  (SELECT MAX(DATE(date_start)) AS max_valid_date FROM `shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads_insights_action_reaction` WHERE conversion_values IS NOT NULL) as Latest_Valid_Date,
  NULL as Staging_pk_count
  from `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.ads_insights_action_reaction_conversion_values`


  union all

  select 
  'ads_insights_action_reaction_unique_actions' as table_name,
  max(date_start) as Staging_max_date,
  (SELECT MAX(DATE(date_start)) AS max_valid_date FROM `shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads_insights_action_reaction` WHERE unique_actions IS NOT NULL) as Latest_Valid_Date,
  NULL as Staging_pk_count
  from `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.ads_insights_action_reaction_unique_actions`


  union all

  select 
  'ads_insights_action_type_normal' as table_name,
  max(date_start) as Staging_max_date,
  max(date_start) as Latest_Valid_Date,
  count(distinct case when date_start = (select max(date_start) from `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.ads_insights_action_type_normal`) then concat(ad_id,date_start) end ) as Staging_pk_count
  from `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.ads_insights_action_type_normal`


  union all

  select 
  'ads_insights_action_type_action_values' as table_name,
  max(date_start) as Staging_max_date,
  (SELECT MAX(DATE(date_start)) AS max_valid_date FROM `shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads_insights_action_type` WHERE action_values IS NOT NULL) as Latest_Valid_Date,
  NULL as Staging_pk_count
  from `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.ads_insights_action_type_action_values`


  union all

  select 
  'ads_insights_action_type_actions' as table_name,
  max(date_start) as Staging_max_date,
  (SELECT MAX(DATE(date_start)) AS max_valid_date FROM `shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads_insights_action_type` WHERE actions IS NOT NULL) as Latest_Valid_Date,
  NULL as Staging_pk_count
  from `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.ads_insights_action_type_actions`


  union all

  select 
  'ads_insights_action_type_cost_per_action_type' as table_name,
  max(date_start) as Staging_max_date,
  (SELECT MAX(DATE(date_start)) AS max_valid_date FROM `shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads_insights_action_type` WHERE cost_per_action_type IS NOT NULL) as Latest_Valid_Date,
  NULL as Staging_pk_count
  from `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.ads_insights_action_type_cost_per_action_type`


  union all

  select 
  'ads_insights_action_type_cost_per_unique_action_type' as table_name,
  max(date_start) as Staging_max_date,
  (SELECT MAX(DATE(date_start)) AS max_valid_date FROM `shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads_insights_action_type` WHERE cost_per_unique_action_type IS NOT NULL) as Latest_Valid_Date,
  NULL as Staging_pk_count
  from `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.ads_insights_action_type_cost_per_unique_action_type`


  union all

  select 
  'ads_insights_action_type_unique_actions' as table_name,
  max(date_start) as Staging_max_date,
  (SELECT MAX(DATE(date_start)) AS max_valid_date FROM `shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads_insights_action_type` WHERE unique_actions IS NOT NULL) as Latest_Valid_Date,
  NULL as Staging_pk_count
  from `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.ads_insights_action_type_unique_actions`


  union all

  select 
  'ads_insights_action_video_sound_normal' as table_name,
  max(date_start) as Staging_max_date,
  max(date_start) Latest_Valid_Date,
  count(distinct case when date_start = (select max(date_start) from `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.ads_insights_action_video_sound_normal`) then concat(ad_id,date_start) end ) as Staging_pk_count
  from `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.ads_insights_action_video_sound_normal`


  union all

  select 
  'ads_insights_action_video_sound_action_values' as table_name,
  max(date_start) as Staging_max_date,
  (SELECT MAX(DATE(date_start)) AS max_valid_date FROM `shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads_insights_action_type` WHERE unique_actions IS NOT NULL) as Latest_Valid_Date,
  NULL as Staging_pk_count
  from `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.ads_insights_action_video_sound_action_values`


  union all

  select 
  'ads_insights_action_video_sound_actions' as table_name,
  max(date_start) as Staging_max_date,
  (SELECT MAX(DATE(date_start)) AS max_valid_date FROM `shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads_insights_action_video_sound` WHERE actions IS NOT NULL) as Latest_Valid_Date,
  NULL as Staging_pk_count
  from `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.ads_insights_action_video_sound_actions`
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