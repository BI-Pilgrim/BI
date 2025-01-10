create or replace table `shopify-pubsub-project.Data_Warehouse_Easyecom_Staging.Sanity_check` as
with Sources as
  (



      select 
  'Orders' as Source_table_name,
  max(order_date) as Source_max_date,
  count(distinct case when date(order_date) = (select max(date(order_date)) from `shopify-pubsub-project.Data_Warehouse_Easyecom_Staging.Orders`) then invoice_id end ) as Source_pk_count
  from `shopify-pubsub-project.easycom.orders`

 union all

-- Since Orderitem table is derived from orders therefor count(distinct pk) of source and destination are same
  select 
  'Order_item' as Source_table_name,
  max(order_date) as Source_max_date,
 count(distinct case when date(order_date) = (select max(date(order_date)) from `shopify-pubsub-project.Data_Warehouse_Easyecom_Staging.Orderitem`) then suborder_id end ) as Source_pk_count
  from `shopify-pubsub-project.Data_Warehouse_Easyecom_Staging.Orderitem`


),

Destination as 
(
    select 
  'Orders' as Dest_table_name,
  max(order_date) as Dest_max_date,
  count(distinct case when date(order_date) = (select max(date(order_date)) from `shopify-pubsub-project.easycom.orders`) then invoice_id end ) as Dest_pk_count
  from  `shopify-pubsub-project.Data_Warehouse_Easyecom_Staging.Orders`

  
  union all

  select 
  'Order_item' as Dest_table_name,
  max(order_date) as Dest_max_date,
 count(distinct case when date(order_date) = (select max(date(order_date)) from `shopify-pubsub-project.Data_Warehouse_Easyecom_Staging.Orderitem`) then suborder_id end ) as Dest_pk_count
  from `shopify-pubsub-project.Data_Warehouse_Easyecom_Staging.Orderitem`

)


  select 
    S.Source_table_name,
    Date(S.Source_max_date) as Source_max_date,
    Date(D.Dest_max_date) as Dest_max_date,
    Current_date() as Date1,
    S.Source_pk_count,
    D.Dest_pk_count,

  from Sources as S
  left join Destination as D
  on S.Source_table_name = D.Dest_table_name

