merge into `shopify-pubsub-project.Data_Warehouse_Easyecom_Staging.shipping_status_metrics` as target
using
(
select *
from
(
select
*,
row_number() over() as rn
from shopify-pubsub-project.easycom.shipping_status_metrics
)
where rn = 1 and analysis_date >= DATE_SUB(CURRENT_DATE("Asia/Kolkata"), INTERVAL 10 DAY)
) as source
on target.analysis_date = source.analysis_date

when matched and target.analysis_date < source.analysis_date
then update set
target.`date` = source.`date`,
target.status = source.status,
target.count = source.count,
target.analysis_date = source.analysis_date
when not matched
then insert
(
`date`,
status,
count,
analysis_date
)
values
(
source.`date`,
source.status,
source.count,
source.analysis_date
)