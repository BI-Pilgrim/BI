create or replace table `shopify-pubsub-project.Data_Warehouse_Easyecom_Staging.grn_details_report`
partition by date_trunc(created_on, day)
as
select *
from
(
select distinct
*,
row_number() over(partition by poNo, companyProductId, receivedQty order by created_on desc) as rn
from shopify-pubsub-project.easycom.grn_details_report
)
where rn = 1;