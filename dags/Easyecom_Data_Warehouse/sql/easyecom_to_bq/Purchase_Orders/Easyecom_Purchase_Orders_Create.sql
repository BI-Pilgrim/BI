CREATE or replace TABLE `shopify-pubsub-project.Data_Warehouse_Easyecom_Staging.Purchase_Orders`
PARTITION BY DATE_TRUNC(ee_extracted_at,day)
-- CLUSTER BY 
OPTIONS(
 description = "Purchase_Orders table is partitioned on order date at day level",
 require_partition_filter = False
 )
 AS
select *
from
(
select distinct
*,
row_number() over(partition by po_id order by ee_extracted_at desc) as rn
from shopify-pubsub-project.easycom.purchase_orders
)
where rn =1