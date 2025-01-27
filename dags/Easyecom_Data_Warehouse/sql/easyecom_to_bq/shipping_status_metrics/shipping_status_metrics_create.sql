create or replace table `shopify-pubsub-project.Data_Warehouse_Easyecom_Staging.shipping_status_metrics`
as
select
*
from
(
select distinct 
*,
row_number() over(partition by analysis_date,status order by analysis_date desc) as rn
from shopify-pubsub-project.easycom.shipping_status_metrics
)
where rn =1;