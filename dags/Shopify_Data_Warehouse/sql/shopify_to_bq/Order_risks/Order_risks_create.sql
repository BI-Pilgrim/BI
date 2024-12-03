
CREATE or replace TABLE `shopify-pubsub-project.Data_Warehouse_Shopify_Staging.Order_risks`
PARTITION BY DATE_TRUNC(Order_risk_updated_at,day)
CLUSTER BY Order_risk_id
OPTIONS(
 description = "Order risk table is partitioned on Order_risk_updated_at",
 require_partition_filter = False
 )
 AS 
SELECT 
distinct
_airbyte_extracted_at,
id as Order_risk_id,
order_id,
shop_url,
updated_at as Order_risk_updated_at,
JSON_VALUE(JSON_EXTRACT(assessments, '$[0]'), '$.risk_level') AS risk_level,
recommendation,
admin_graphql_api_id

from `pilgrim_bi_airbyte.order_risks`
