

CREATE OR REPLACE TABLE `shopify-pubsub-project.Data_Warehouse_Shopify_Staging.Fulfillments`
PARTITION BY DATE_TRUNC(fulfillment_created_at, DAY)
CLUSTER BY fulfillment_id
OPTIONS(
  description = "fulfillments table is partitioned on fulfillment created at",
  require_partition_filter = FALSE
)
 AS 
SELECT  
_airbyte_extracted_at,
id as fulfillment_id,
name as fulfillment_name,
status,
service,
order_id,
shop_url,
created_at as fulfillment_created_at,
CAST(JSON_EXTRACT_SCALAR(line_item, '$.current_quantity') AS INT64) AS current_quantity,
CAST(JSON_EXTRACT_SCALAR(line_item, '$.fulfillable_quantity') AS INT64) AS fulfillable_quantity,
JSON_EXTRACT_SCALAR(line_item, '$.fulfillment_service') AS fulfillment_service,
JSON_EXTRACT_SCALAR(line_item, '$.fulfillment_status') AS fulfillment_status,
CAST(JSON_EXTRACT_SCALAR(line_item, '$.gift_card') AS BOOL) AS gift_card,
JSON_EXTRACT_SCALAR(line_item, '$.name') AS product_name,
CAST(JSON_EXTRACT_SCALAR(line_item, '$.price') AS FLOAT64) AS product_price,
CAST(JSON_EXTRACT_SCALAR(line_item, '$.product_id') AS INT64) AS product_id,
JSON_EXTRACT_SCALAR(line_item, '$.sku') AS product_sku,
MAX(
    CASE
      WHEN JSON_EXTRACT_SCALAR(tax_line, '$.title') = "CGST" THEN CAST(JSON_EXTRACT_SCALAR(tax_line, '$.price') AS FLOAT64)
      ELSE NULL
    END
  ) AS cgst,
  -- Extract SGST
  MAX(
    CASE
      WHEN JSON_EXTRACT_SCALAR(tax_line, '$.title') = "SGST" THEN CAST(JSON_EXTRACT_SCALAR(tax_line, '$.price') AS FLOAT64)
      ELSE NULL
    END
  ) AS sgst,
  -- Extract IGST (if present)
  MAX(
    CASE
      WHEN JSON_EXTRACT_SCALAR(tax_line, '$.title') = "IGST" THEN CAST(JSON_EXTRACT_SCALAR(tax_line, '$.price') AS FLOAT64)
      ELSE NULL
    END
  ) AS igst,

updated_at as fulfillment_updated_at,
location_id,
tracking_url,
notify_customer,
shipment_status,
tracking_number,
tracking_company,
admin_graphql_api_id
from 
`shopify-pubsub-project.pilgrim_bi_airbyte.fulfillments`,
UNNEST(JSON_EXTRACT_ARRAY(line_items)) AS line_item,
UNNEST(JSON_EXTRACT_ARRAY(line_item, '$.tax_lines')) AS tax_line
GROUP BY
_airbyte_extracted_at,
fulfillment_id,
fulfillment_name,
status,
service,
order_id,
shop_url,
fulfillment_created_at,
current_quantity,
fulfillable_quantity,
fulfillment_service,
fulfillment_status,
gift_card,
product_name,
product_price,
product_id,
product_sku,
fulfillment_updated_at,
location_id,
tracking_url,
notify_customer,
shipment_status,
tracking_number,
tracking_company,
admin_graphql_api_id
