create or replace table `shopify-pubsub-project.Data_Warehouse_Nykaa_Staging.appointment_adherence`
partition by date_trunc(pg_extracted_at, day)
as
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
where rn = 1
order by po_no,confirm_appointment_date,wh_remarks,reporting_week, confirm_appointment_date desc, pg_extracted_at desc