CREATE OR REPLACE TABLE `shopify-pubsub-project.Data_Warehouse_GoogleAds_Staging.geographic_view`
PARTITION BY DATE_TRUNC(_airbyte_extracted_at, DAY)
AS
SELECT
  _airbyte_extracted_at,
  ad_group_id,
  customer_descriptive_name,
  customer_id,
  geographic_view_country_criterion_id,
  geographic_view_location_type,
  segments_date,
FROM
  `shopify-pubsub-project.pilgrim_bi_google_ads.geographic_view`
