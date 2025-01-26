MERGE INTO `shopify-pubsub-project.Data_Warehouse_Easyecom_Staging.daily_metrics` AS target
USING
(
select 
*
from
(
select
*,
row_number() over(partition by analysis_date order by analysis_date desc) as rn
from `shopify-pubsub-project.easycom.daily_metrics`
)
where rn =1 and date(analysis_date) >= DATE_SUB(CURRENT_DATE("Asia/Kolkata"), INTERVAL 10 DAY) 
) as source
on target.analysis_date = source.analysis_date
when matched and target.analysis_date < source.analysis_date
then update set
target.date = source.date,
target.total_orders = source.total_orders,
target.delayed_dispatch_count = source.delayed_dispatch_count,
target.compliance_rate = source.compliance_rate,
target.avg_delay_hours = source.avg_delay_hours,
target.median_delay_hours = source.median_delay_hours,
target.analysis_date = source.analysis_date
when not matched
then insert
(
`date`,
total_orders,
delayed_dispatch_count,
compliance_rate,
avg_delay_hours,
median_delay_hours,
analysis_date
)
values
(
source.date,
source.total_orders,
source.delayed_dispatch_count,
source.compliance_rate,
source.avg_delay_hours,
source.median_delay_hours,
source.analysis_date
)