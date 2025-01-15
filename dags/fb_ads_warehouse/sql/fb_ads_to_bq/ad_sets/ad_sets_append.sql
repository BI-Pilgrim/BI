MERGE INTO `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.ad_sets` AS TARGET
USING
(
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
  FROM
  (
    SELECT
      *,
      ROW_NUMBER() OVER (PARTITION BY campaign_id ORDER BY _airbyte_extracted_at DESC) AS row_num
      FROM
        shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ad_sets,
        UNNEST(JSON_EXTRACT_ARRAY(targeting, '$.excluded_custom_audiences')) AS excluded_audience
      WHERE
        DATE(_airbyte_extracted_at) >= DATE_SUB(CURRENT_DATE("Asia/Kolkata"), INTERVAL 10 DAY)
  )
  WHERE row_num = 1 -- Keep only the most recent row per label_id
) AS SOURCE
ON
  TARGET.id = SOURCE.id
WHEN MATCHED AND TARGET._airbyte_extracted_at < SOURCE._airbyte_extracted_at
THEN UPDATE SET
    TARGET.bid_amount = SOURCE.bid_amount,
    TARGET.daily_budget = SOURCE.daily_budget,
    TARGET.lifetime_budget = SOURCE.lifetime_budget,
    TARGET.budget_remaining = SOURCE.budget_remaining,
    TARGET.id = SOURCE.id,
    TARGET.name = SOURCE.name,
    TARGET.account_id = SOURCE.account_id,
    TARGET.campaign_id = SOURCE.campaign_id,
    TARGET.effective_status = SOURCE.effective_status,
    TARGET._airbyte_extracted_at = SOURCE._airbyte_extracted_at,
    TARGET.end_time = SOURCE.end_time,
    TARGET.start_time = SOURCE.start_time,
    TARGET.created_time = SOURCE.created_time,
    TARGET.updated_time = SOURCE.updated_time,
    TARGET.age_max = SOURCE.age_max,
    TARGET.age_min = SOURCE.age_min,
    TARGET.brand_safety_content_filter_levels = SOURCE.brand_safety_content_filter_levels,
    TARGET.excluded_audience_id = SOURCE.excluded_audience_id,
    TARGET.excluded_audience_name = SOURCE.excluded_audience_name,
    TARGET.genders = SOURCE.genders,
    TARGET.geo_location_countries = SOURCE.geo_location_countries,
    TARGET.geo_location_types = SOURCE.geo_location_types,
    TARGET.advantage_audience = SOURCE.advantage_audience,
    TARGET.bid_info_actions = SOURCE.bid_info_actions,
    TARGET.promoted_object_custom_event_type = SOURCE.promoted_object_custom_event_type,
    TARGET.promoted_object_pixel_id = SOURCE.promoted_object_pixel_id
WHEN NOT MATCHED
THEN INSERT
(
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
  age_max,
  age_min,
  brand_safety_content_filter_levels,
  excluded_audience_id,
  excluded_audience_name,
  genders,
  geo_location_countries,
  geo_location_types,
  advantage_audience,
  bid_info_actions,
  promoted_object_custom_event_type,
  promoted_object_pixel_id
)
VALUES
(
  SOURCE.bid_amount,
  SOURCE.daily_budget,
  SOURCE.lifetime_budget,
  SOURCE.budget_remaining,
  SOURCE.id,
  SOURCE.name,
  SOURCE.account_id,
  SOURCE.campaign_id,
  SOURCE.effective_status,
  SOURCE._airbyte_extracted_at,
  SOURCE.end_time,
  SOURCE.start_time,
  SOURCE.created_time,
  SOURCE.updated_time,
  SOURCE.age_max,
  SOURCE.age_min,
  SOURCE.brand_safety_content_filter_levels,
  SOURCE.excluded_audience_id,
  SOURCE.excluded_audience_name,
  SOURCE.genders,
  SOURCE.geo_location_countries,
  SOURCE.geo_location_types,
  SOURCE.advantage_audience,
  SOURCE.bid_info_actions,
  SOURCE.promoted_object_custom_event_type,
  SOURCE.promoted_object_pixel_id
)
