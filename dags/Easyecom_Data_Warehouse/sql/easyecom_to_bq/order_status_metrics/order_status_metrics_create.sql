create or replace table `shopify-pubsub-project.Data_Warehouse_Easyecom_Staging.order_status_metrics`
partition by date_trunc(analysis_date, day)
as
select distinct *
from shopify-pubsub-project.easycom.order_status_metrics