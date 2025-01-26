CREATE or replace TABLE `shopify-pubsub-project.Data_Warehouse_Easyecom_Staging.Reports`
PARTITION BY DATE_TRUNC(created_on,day)
-- CLUSTER BY 
OPTIONS(
 description = "Reports table is partitioned on order date at day level",
 require_partition_filter = False
 )
 AS


select
  report_id,
  report_type,
  start_date,
  end_date,
  created_on,
  status,
  csv_url,
  inventory_type,
  ee_extracted_at,
from
(
select distinct
*,
row_number() over(partition by report_id order by created_on desc) as rn
from `shopify-pubsub-project.easycom.reports`
)
where rn = 1