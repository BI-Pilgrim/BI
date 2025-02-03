merge into `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.ads_excluded_audience` as target
using
(
select
_airbyte_extracted_at,
id,
created_time,
JSON_EXTRACT_SCALAR(excluded_audience, '$.id') AS excluded_audience_id,
JSON_EXTRACT_SCALAR(excluded_audience, '$.name') AS excluded_audience_name,
JSON_EXTRACT(targeting, '$.genders') AS genders,
JSON_EXTRACT(targeting, '$.geo_locations') AS geo_locations,
JSON_EXTRACT(targeting, '$.geo_locations.countries') AS geo_location_countries,
JSON_EXTRACT(targeting, '$.geo_locations.location_types') AS geo_location_types,
JSON_EXTRACT_SCALAR(targeting, '$.targeting_automation.advantage_audience') AS advantage_audience,
JSON_EXTRACT_SCALAR(targeting, '$.targeting_relaxation_types.custom_audience') AS targeting_relaxation_custom_audience,
JSON_EXTRACT_SCALAR(targeting, '$.targeting_relaxation_types.lookalike') AS targeting_relaxation_lookalike,
FROM
(
  select
  *,
  row_number() over(partition by id,created_time order by created_time desc) as rn,
  from shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads,
  UNNEST(JSON_EXTRACT_ARRAY(targeting, '$.excluded_custom_audiences')) AS excluded_audience
  -- UNNEST(JSON_EXTRACT_ARRAY(recommendations)) AS value
)
where rn = 1 and date(created_time) >= date_sub(current_date("Asia/Kolkata"), INTERVAL 10 day)
) as source
on target.id = source.id
when matched and target.created_time < source.created_time
then update set
target._airbyte_extracted_at = source._airbyte_extracted_at,
target.id = source.id,
target.created_time = source.created_time,
target.excluded_audience_id = source.excluded_audience_id,
target.excluded_audience_name = source.excluded_audience_name,
target.genders = source.genders,
target.geo_locations = source.geo_locations,
target.geo_location_countries = source.geo_location_countries,
target.geo_location_types = source.geo_location_types,
target.advantage_audience = source.advantage_audience,
target.targeting_relaxation_custom_audience = source.targeting_relaxation_custom_audience,
target.targeting_relaxation_lookalike = source.targeting_relaxation_lookalike
when not matched
then insert
(
_airbyte_extracted_at,
id,
created_time,
excluded_audience_id,
excluded_audience_name,
genders,
geo_locations,
geo_location_countries,
geo_location_types,
advantage_audience,
targeting_relaxation_custom_audience,
targeting_relaxation_lookalike
)
values
(
source._airbyte_extracted_at,
source.id,
source.created_time,
source.excluded_audience_id,
source.excluded_audience_name,
source.genders,
source.geo_locations,
source.geo_location_countries,
source.geo_location_types,
source.advantage_audience,
source.targeting_relaxation_custom_audience,
source.targeting_relaxation_lookalike
)