

MERGE INTO `shopify-pubsub-project.Data_Warehouse_Shopify_Staging.Order_risks` AS target  
USING (
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

from `shopify-pubsub-project.pilgrim_bi_airbyte.order_risks`
WHERE DATE(_airbyte_extracted_at) >= DATE_SUB(CURRENT_DATE("Asia/Kolkata"), INTERVAL 10 DAY)
) AS source

ON target.Order_risk_id = source.Order_risk_id

WHEN MATCHED AND source._airbyte_extracted_at > target._airbyte_extracted_at 
THEN UPDATE SET
  
target._airbyte_extracted_at = source._airbyte_extracted_at,
target.Order_risk_id = source.Order_risk_id,
target.order_id = source.order_id,
target.shop_url = source.shop_url,
target.Order_risk_updated_at = source.Order_risk_updated_at,
target.risk_level = source.risk_level,
target.recommendation = source.recommendation,
target.admin_graphql_api_id = source.admin_graphql_api_id


WHEN NOT MATCHED THEN INSERT (
_airbyte_extracted_at,
Order_risk_id,
order_id,
shop_url,
Order_risk_updated_at,
risk_level,
recommendation,
admin_graphql_api_id
    
  )
  VALUES (
source._airbyte_extracted_at,
source.Order_risk_id,
source.order_id,
source.shop_url,
source.Order_risk_updated_at,
source.risk_level,
source.recommendation,
source.admin_graphql_api_id
  );
