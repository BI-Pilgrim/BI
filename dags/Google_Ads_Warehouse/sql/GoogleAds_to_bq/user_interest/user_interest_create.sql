CREATE OR REPLACE TABLE `shopify-pubsub-project.Data_Warehouse_GoogleAds_Staging.user_interest`
PARTITION BY DATE_TRUNC(_airbyte_extracted_at, DAY)
AS
SELECT
  _airbyte_extracted_at,
  user_interest_launched_to_all,
  user_interest_name,
  user_interest_resource_name,
  user_interest_taxonomy_type,
  user_interest_user_interest_id,
  user_interest_user_interest_parent,
FROM
  `shopify-pubsub-project.pilgrim_bi_google_ads.user_interest`
