merge into `shopify-pubsub-project.Data_Warehouse_Nykaa_Staging.appointment_adherence` as target
using
(
select distinct
*
from
(
select
parent,
wh_remarks,
confirm_appointment_date,
po_no,
wh_code,
brand_name,
upper(month) as month,
week,
qty,
pg_extracted_at,
RIGHT(TRIM(pg_mail_subject), 6) AS reporting_week,
DATE(pg_mail_recieved_at) AS mail_received_at,
FORMAT_TIMESTAMP('%A', pg_mail_recieved_at) AS day_name,
row_number() over(partition by po_no,confirm_appointment_date,wh_remarks,RIGHT(TRIM(pg_mail_subject), 6) order by confirm_appointment_date desc,pg_extracted_at desc) as rn,
from shopify-pubsub-project.pilgrim_bi_nykaa.appointment_adherence
-- where po_no = 'NHO1218153'
)
where rn = 1 and mail_received_at >= date_sub(current_date("Asia/Kolkata"), INTERVAL 10 day)
) as source

on target.po_no = source.po_no
and target.confirm_appointment_date = source.confirm_appointment_date
and target.wh_remarks = source.wh_remarks
and target.reporting_week = source.reporting_week 

when matched and target.mail_received_at < source.mail_received_at
then update set

target.parent = source.parent,
target.wh_remarks = source.wh_remarks,
target.confirm_appointment_date = source.confirm_appointment_date,
target.po_no = source.po_no,
target.wh_code = source.wh_code,
target.brand_name = source.brand_name,
target.month = source.month,
target.week = source.week,
target.qty = source.qty,
target.pg_extracted_at = source.pg_extracted_at,
target.reporting_week = source.reporting_week,
target.mail_received_at = source.mail_received_at,
target.day_name = source.day_name

when not matched
then insert
(
parent,
wh_remarks,
confirm_appointment_date,
po_no,
wh_code,
brand_name,
month,
week,
qty,
pg_extracted_at,
reporting_week,
mail_received_at,
day_name

)
values
(
source.parent,
source.wh_remarks,
source.confirm_appointment_date,
source.po_no,
source.wh_code,
source.brand_name,
source.month,
source.week,
source.qty,
source.pg_extracted_at,
source.reporting_week,
source.mail_received_at,
source.day_name

)