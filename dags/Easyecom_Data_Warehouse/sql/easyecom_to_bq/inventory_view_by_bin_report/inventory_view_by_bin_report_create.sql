create or replace table `shopify-pubsub-project.Data_Warehouse_Easyecom_Staging.inventory_view_by_bin_report`
as
select
*
from
(
select
*,
row_number() over(partition by SKU order by ee_extracted_at desc) as rn
from shopify-pubsub-project.easycom.inventory_view_by_bin_report
)
where rn =1;