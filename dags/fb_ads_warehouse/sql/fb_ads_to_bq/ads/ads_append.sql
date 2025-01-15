MERGE INTO `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.ads` AS TARGET
USING
(
  SELECT
    _airbyte_extracted_at,
    id,
    name,
    status,
    adset_id,
    bid_type,
    account_id,
    bid_amount,
    campaign_id,
    created_time,
    source_ad_id,
    updated_time,
    effective_status,
    last_updated_by_app_id,
      -- TARGETING,
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
    JSON_EXTRACT_SCALAR(targeting, '$.targeting_relaxation_types.custom_audience') AS targeting_relaxation_custom_audience,
    JSON_EXTRACT_SCALAR(targeting, '$.targeting_relaxation_types.lookalike') AS targeting_relaxation_lookalike,


      -- Extract individual fields from the JSON objects
    JSON_EXTRACT_SCALAR(value, '$.blame_field') AS blame_field,
    CAST(JSON_EXTRACT_SCALAR(value, '$.code') AS INT64) AS code,
    JSON_EXTRACT_SCALAR(value, '$.confidence') AS confidence,
    JSON_EXTRACT_SCALAR(value, '$.importance') AS importance,
    JSON_EXTRACT_SCALAR(value, '$.message') AS message,
    JSON_EXTRACT_SCALAR(value, '$.title') AS title,


    JSON_EXTRACT_scalar(creative, '$.id') as creative_id
  FROM
  (
    SELECT
      *,
      ROW_NUMBER() OVER (PARTITION BY adset_id ORDER BY _airbyte_extracted_at DESC) AS row_num
      FROM
        shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads,
        UNNEST(JSON_EXTRACT_ARRAY(targeting, '$.excluded_custom_audiences')) AS excluded_audience,
        UNNEST(JSON_EXTRACT_ARRAY(recommendations)) AS value
      WHERE
        DATE(_airbyte_extracted_at) >= DATE_SUB(CURRENT_DATE("Asia/Kolkata"), INTERVAL 10 DAY)
  )
  WHERE row_num = 1 -- Keep only the most recent row per label_id
) AS SOURCE
ON
  TARGET.id = SOURCE.id
WHEN MATCHED AND TARGET._airbyte_extracted_at < SOURCE._airbyte_extracted_at
THEN UPDATE SET
TARGET._airbyte_extracted_at = SOURCE._airbyte_extracted_at,
TARGET.id = SOURCE.id,
TARGET.name = SOURCE.name,
TARGET.status = SOURCE.status,
TARGET.adset_id = SOURCE.adset_id,
TARGET.bid_type = SOURCE.bid_type,
TARGET.account_id = SOURCE.account_id,
TARGET.bid_amount = SOURCE.bid_amount,
TARGET.campaign_id = SOURCE.campaign_id,
TARGET.created_time = SOURCE.created_time,
TARGET.source_ad_id = SOURCE.source_ad_id,
TARGET.updated_time = SOURCE.updated_time,
TARGET.effective_status = SOURCE.effective_status,
TARGET.last_updated_by_app_id = SOURCE.last_updated_by_app_id,
TARGET.age_max = SOURCE.age_max,
TARGET.age_min = SOURCE.age_min,
TARGET.brand_safety_content_filter_levels = SOURCE.brand_safety_content_filter_levels,
TARGET.excluded_audience_id = SOURCE.excluded_audience_id,
TARGET.excluded_audience_name = SOURCE.excluded_audience_name,
TARGET.genders = SOURCE.genders,
TARGET.geo_location_countries = SOURCE.geo_location_countries,
TARGET.geo_location_types = SOURCE.geo_location_types,
TARGET.advantage_audience = SOURCE.advantage_audience,
TARGET.targeting_relaxation_custom_audience = SOURCE.targeting_relaxation_custom_audience,
TARGET.targeting_relaxation_lookalike = SOURCE.targeting_relaxation_lookalike,
TARGET.blame_field = SOURCE.blame_field,
TARGET.code = SOURCE.code,
TARGET.confidence = SOURCE.confidence,
TARGET.importance = SOURCE.importance,
TARGET.message = SOURCE.message,
TARGET.title = SOURCE.title
WHEN NOT MATCHED
THEN INSERT
(
  _airbyte_extracted_at,
  id,
  name,
  status,
  adset_id,
  bid_type,
  account_id,
  bid_amount,
  campaign_id,
  created_time,
  source_ad_id,
  updated_time,
  effective_status,
  last_updated_by_app_id,
  age_max,
  age_min,
  brand_safety_content_filter_levels,
  excluded_audience_id,
  excluded_audience_name,
  genders,
  geo_location_countries,
  geo_location_types,
  advantage_audience,
  targeting_relaxation_custom_audience,
  targeting_relaxation_lookalike,
  blame_field,
  code,
  confidence,
  importance,
  message,
  title
)
VALUES
(
SOURCE._airbyte_extracted_at,
SOURCE.id,
SOURCE.name,
SOURCE.status,
SOURCE.adset_id,
SOURCE.bid_type,
SOURCE.account_id,
SOURCE.bid_amount,
SOURCE.campaign_id,
SOURCE.created_time,
SOURCE.source_ad_id,
SOURCE.updated_time,
SOURCE.effective_status,
SOURCE.last_updated_by_app_id,
SOURCE.age_max,
SOURCE.age_min,
SOURCE.brand_safety_content_filter_levels,
SOURCE.excluded_audience_id,
SOURCE.excluded_audience_name,
SOURCE.genders,
SOURCE.geo_location_countries,
SOURCE.geo_location_types,
SOURCE.advantage_audience,
SOURCE.targeting_relaxation_custom_audience,
SOURCE.targeting_relaxation_lookalike,
SOURCE.blame_field,
SOURCE.code,
SOURCE.confidence,
SOURCE.importance,
SOURCE.message,
SOURCE.title
)
