CREATE OR REPLACE TABLE `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.ad_sets`
PARTITION BY DATE_TRUNC(_airbyte_extracted_at, DAY)
AS
SELECT
  bid_amount,
  daily_budget,
  lifetime_budget,
  budget_remaining,
  id,
  name,
  account_id,
  campaign_id,
  effective_status,
  _airbyte_extracted_at,
  end_time,
  start_time,
  created_time,
  updated_time,
  -- targeting,
  JSON_EXTRACT_SCALAR(targeting, '$.age_max') AS age_max,
  JSON_EXTRACT_SCALAR(targeting, '$.age_min') AS age_min,
  JSON_EXTRACT(targeting, '$.brand_safety_content_filter_levels') AS brand_safety_content_filter_levels,
  -- JSON_EXTRACT(targeting, '$.excluded_custom_audiences') AS excluded_custom_audiences,
  JSON_EXTRACT_SCALAR(excluded_audience, '$.id') AS excluded_audience_id,
  JSON_EXTRACT_SCALAR(excluded_audience, '$.name') AS excluded_audience_name,
  JSON_EXTRACT(targeting, '$.genders') AS genders,
  -- JSON_EXTRACT(targeting, '$.geo_locations') AS geo_locations,
  JSON_EXTRACT(targeting, '$.geo_locations.countries') AS geo_location_countries,
  JSON_EXTRACT(targeting, '$.geo_locations.location_types') AS geo_location_types,
  JSON_EXTRACT_SCALAR(targeting, '$.targeting_automation.advantage_audience') AS advantage_audience,
  JSON_EXTRACT_SCALAR(bid_info, '$.ACTIONS') AS bid_info_actions,
  JSON_EXTRACT_SCALAR(promoted_object,'$.custom_event_type') AS promoted_object_custom_event_type,
  JSON_EXTRACT_SCALAR(promoted_object,'$.pixel_id') AS promoted_object_pixel_id,
FROM shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ad_sets,
UNNEST(JSON_EXTRACT_ARRAY(targeting, '$.excluded_custom_audiences')) AS excluded_audience
