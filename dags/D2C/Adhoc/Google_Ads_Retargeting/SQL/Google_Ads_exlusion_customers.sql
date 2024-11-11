insert into `shopify-pubsub-project.Pre_Analytics.Google_ads_retargeting_exclusion_customer`

with latest_customers as
  (select 
  distinct
  customer_first_name,
  customer_last_name,
  'in' as ship_country,
  shipping_pincode,
  customer_email,
  customer_phone

  from `pilgrim-dw.halo_115.global_reports_project_level_report_order_items`
  where channel = 'Shopify' and order_date>=current_date()-2
),

old_customers as
 (select 
  distinct
  customer_email
from `shopify-pubsub-project.Pre_Analytics.Google_ads_retargeting_exclusion_customer`)

select 
L.*
from latest_customers as L
left join old_customers as O
on L.customer_email = O.customer_email
where O.customer_email is null
