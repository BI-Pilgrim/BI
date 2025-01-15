MERGE INTO `` AS TARGET
USING
(
SELECT
  CAST(name AS STRING) AS name,
  CAST(sku AS STRING) AS sku,
  CAST(master_sku AS STRING) AS master_sku,
  CAST(mrp AS FLOAT64) AS mrp,
  CAST(site_uid AS STRING) AS site_uid,
  CAST(listing_ref_number AS STRING) AS listing_ref_number,
  CAST(uid AS STRING) AS uid,
  CAST(identifier AS STRING) AS identifier,
  CAST(title AS STRING) AS title,
  ee_extracted_at,
FROM
(
SELECT
*,
ROW_NUMBER() OVER(PARTITON BY ORDER BY _airbyte_extracted_at) as row_num
FROM `shopify-pubsub-project.easycom.marketplace_listings`
WHERE DATE(_airbyte_extracted_at) >= DATE_SUB(CURRENT_DATE("Asia/Kolkata"), INTERVAL 10 DAY)
)
WHERE row_num = 1 -- Keep only the most recent row per customer_id and segments_date
) AS SOURCE
ON SOURCE. = TARGET.
WHEN MATCHED AND TARGET.ee_extracted_at < SOURCE.ee_extracted_at
THEN UPDATE SET
TARGET.name = SOURCE.name,
TARGET.sku = SOURCE.sku,
TARGET.master_sku = SOURCE.master_sku,
TARGET.mrp = SOURCE.mrp,
TARGET.site_uid = SOURCE.site_uid,
TARGET.listing_ref_number = SOURCE.listing_ref_number,
TARGET.uid = SOURCE.uid,
TARGET.identifier = SOURCE.identifier,
TARGET.title = SOURCE.title,
TARGET.ee_extracted_at = SOURCE.ee_extracted_at,
WHEN NOT MATCHED
THEN INSERT
(
  name,
  sku,
  master_sku,
  mrp,
  site_uid,
  listing_ref_number,
  uid,
  identifier,
  title,
  ee_extracted_at
)
VALUES
(
SOURCE.name,
SOURCE.sku,
SOURCE.master_sku,
SOURCE.mrp,
SOURCE.site_uid,
SOURCE.listing_ref_number,
SOURCE.uid,
SOURCE.identifier,
SOURCE.title,
SOURCE.ee_extracted_at
)
