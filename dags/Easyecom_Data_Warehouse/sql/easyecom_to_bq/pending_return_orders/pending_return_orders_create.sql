create or replace table `shopify-pubsub-project.Data_Warehouse_Easyecom_Staging.pending_return_orders`
as
select *
from
(
select 
*,
row_number() over(partition by invoice_id order by ee_extracted_at desc) as rn
from shopify-pubsub-project.easycom.pending_return_orders
order by invoice_id
)
where rn =1;