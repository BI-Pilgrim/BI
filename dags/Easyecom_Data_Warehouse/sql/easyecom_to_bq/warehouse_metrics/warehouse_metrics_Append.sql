merge into `shopify-pubsub-project.Data_Warehouse_Easyecom_Staging.warehouse_metrics` as target
using
(
select *
from
(
select
*,
row_number() over(partition by location_key,warehouse_name order by analysis_date desc) as rn
from shopify-pubsub-project.easycom.warehouse_metrics
)
where rn = 1 and `analysis_date` >= DATE_SUB(CURRENT_DATE("Asia/Kolkata"), INTERVAL 10 DAY)
) as source
on target.location_key = source.location_key
and target.warehouse_name = source.warehouse_name
when matched and target.analysis_date < source.analysis_date
then update set
target.location_key = source.location_key,
target.`date` = source.`date`,
target.warehouse_name = source.warehouse_name,
target.total_orders = source.total_orders,
target.delayed_orders = source.delayed_orders,
target.compliance_rate = source.compliance_rate,
target.avg_delay_hours = source.avg_delay_hours,
target.median_delay_hours = source.median_delay_hours,
target.analysis_date = source.analysis_date
when not matched
then insert
(
location_key,
`date`,
warehouse_name,
total_orders,
delayed_orders,
compliance_rate,
avg_delay_hours,
median_delay_hours,
analysis_date
)
values
(
source.location_key,
source.`date`,
source.warehouse_name,
source.total_orders,
source.delayed_orders,
source.compliance_rate,
source.avg_delay_hours,
source.median_delay_hours,
source.analysis_date
)