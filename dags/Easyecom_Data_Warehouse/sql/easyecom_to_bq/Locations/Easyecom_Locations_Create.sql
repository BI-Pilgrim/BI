
CREATE or replace TABLE `shopify-pubsub-project.Data_Warehouse_Easyecom_Staging.Locations`
PARTITION BY DATE_TRUNC(ee_extracted_at,day)
-- CLUSTER BY 
OPTIONS(
 description = "Locations table is partitioned on order date at day level",
 require_partition_filter = False
 )
 AS


select

  CAST(company_id AS STRING) AS company_id,
  CAST(location_key AS STRING) AS location_key,
  CAST(location_name AS STRING) AS location_name,
  CAST(is_store AS STRING) AS is_store,
  CAST(city AS STRING) AS city,
  CAST(state AS STRING) AS state,
  CAST(country AS STRING) AS country,
  CAST(zip AS STRING) AS zip,
  CAST(copy_master_from_primary AS STRING) AS copy_master_from_primary,
  CAST(address AS STRING) AS address,
  CAST(api_token AS STRING) AS api_token,
  CAST(user_id AS STRING) AS user_id,
  CAST(phone_number AS STRING) AS phone_number,
  CAST(billing_street AS STRING) AS billing_street,
  CAST(billing_state AS STRING) AS billing_state,
  CAST(billing_zipcode AS STRING) AS billing_zipcode,
  CAST(billing_country AS STRING) AS billing_country,
  CAST(pickup_street AS STRING) AS pickup_street,
  CAST(pickup_state AS STRING) AS pickup_state,
  CAST(pickup_zipcode AS STRING) AS pickup_zipcode,
  CAST(pickup_country AS STRING) AS pickup_country,
  CAST(stockHandle AS STRING) AS stockHandle,
  ee_extracted_at,
  FROM `shopify-pubsub-project.easycom.locations`