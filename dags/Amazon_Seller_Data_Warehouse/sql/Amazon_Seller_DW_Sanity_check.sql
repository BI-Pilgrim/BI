create or replace table `shopify-pubsub-project.Data_Warehouse_Amazon_Seller_Staging.Amazon_seller_Sanity_check` as
with Sources as
  (
    select 
  'ALL_ORDERS_DATA_BY_LAST_UPDATE_GENERAL' as Source_table_name,
  max(purchase_date) as Source_max_date,
  count(distinct case when date(purchase_date) = (select max(date(purchase_date)) from `shopify-pubsub-project.Data_Warehouse_Amazon_Seller_Staging.ALL_ORDERS_DATA_BY_LAST_UPDATE_GENERAL`) then amazon_order_id end ) as Source_pk_count
  from `shopify-pubsub-project.pilgrim_bi_airbyte_amazon_seller.GET_FLAT_FILE_ALL_ORDERS_DATA_BY_LAST_UPDATE_GENERAL`

 union all

select 
  'ALL_ORDERS_DATA_BY_ORDER_DATE_GENERAL' as Source_table_name,
  max(purchase_date) as Source_max_date,
  count(distinct case when date(purchase_date) = (select max(date(purchase_date)) from `shopify-pubsub-project.Data_Warehouse_Amazon_Seller_Staging.ALL_ORDERS_DATA_BY_ORDER_DATE_GENERAL`) then amazon_order_id end ) as Source_pk_count
  from `shopify-pubsub-project.pilgrim_bi_airbyte_amazon_seller.GET_FLAT_FILE_ALL_ORDERS_DATA_BY_ORDER_DATE_GENERAL`


 union all

select 
  'ALL_ORDERS_DATA_BY_ORDER_DATE_GENERAL_XML' as Source_table_name,
  max(PurchaseDate) as Source_max_date,
  count(distinct case when date(PurchaseDate) = (select max(date(PurchaseDate)) from `shopify-pubsub-project.Data_Warehouse_Amazon_Seller_Staging.ALL_ORDERS_DATA_BY_ORDER_DATE_GENERAL_XML`) then AmazonOrderID end ) as Source_pk_count
  from `shopify-pubsub-project.pilgrim_bi_airbyte_amazon_seller.GET_XML_ALL_ORDERS_DATA_BY_ORDER_DATE_GENERAL`


  union all

select 
  'MERCHANT_CANCELLED_LISTINGS_DATA' as Source_table_name,
  max(dataEndTime) as Source_max_date,
  count(distinct case when date(dataEndTime) = (select max(date(dataEndTime)) from `shopify-pubsub-project.Data_Warehouse_Amazon_Seller_Staging.MERCHANT_CANCELLED_LISTINGS_DATA`) then product_id end ) as Source_pk_count
  from `shopify-pubsub-project.pilgrim_bi_airbyte_amazon_seller.GET_MERCHANT_CANCELLED_LISTINGS_DATA`



),
-----------------------------------------------------------------------------------------------------
Destination as 
(
    select 
  'ALL_ORDERS_DATA_BY_LAST_UPDATE_GENERAL' as Dest_table_name,
  max(purchase_date) as Dest_max_date,
  count(distinct case when date(purchase_date) = (select max(date(purchase_date)) from `shopify-pubsub-project.pilgrim_bi_airbyte_amazon_seller.GET_FLAT_FILE_ALL_ORDERS_DATA_BY_LAST_UPDATE_GENERAL`) then Amazon_Order_id end ) as Dest_pk_count
  from  `shopify-pubsub-project.Data_Warehouse_Amazon_Seller_Staging.ALL_ORDERS_DATA_BY_LAST_UPDATE_GENERAL`

  
  union all

  select 
  'ALL_ORDERS_DATA_BY_ORDER_DATE_GENERAL' as Dest_table_name,
  max(purchase_date) as Dest_max_date,
 count(distinct case when date(purchase_date) = (select max(date(purchase_date)) from `shopify-pubsub-project.pilgrim_bi_airbyte_amazon_seller.GET_FLAT_FILE_ALL_ORDERS_DATA_BY_ORDER_DATE_GENERAL`) then Amazon_Order_id end ) as Dest_pk_count
  from `shopify-pubsub-project.Data_Warehouse_Amazon_Seller_Staging.ALL_ORDERS_DATA_BY_ORDER_DATE_GENERAL`


union all

  select 
  'ALL_ORDERS_DATA_BY_ORDER_DATE_GENERAL_XML' as Dest_table_name,
  max(PurchaseDate) as Dest_max_date,
 count(distinct case when date(PurchaseDate) = (select max(date(PurchaseDate)) from `shopify-pubsub-project.pilgrim_bi_airbyte_amazon_seller.GET_XML_ALL_ORDERS_DATA_BY_ORDER_DATE_GENERAL`) then amazon_order_id end ) as Dest_pk_count
  from `shopify-pubsub-project.Data_Warehouse_Amazon_Seller_Staging.ALL_ORDERS_DATA_BY_ORDER_DATE_GENERAL_XML`


union all

  select 
  'MERCHANT_CANCELLED_LISTINGS_DATA' as Dest_table_name,
  max(dataEndTime) as Dest_max_date,
 count(distinct case when date(dataEndTime) = (select max(date(dataEndTime)) from `shopify-pubsub-project.pilgrim_bi_airbyte_amazon_seller.GET_MERCHANT_CANCELLED_LISTINGS_DATA`) then product_id end ) as Dest_pk_count
  from `shopify-pubsub-project.Data_Warehouse_Amazon_Seller_Staging.MERCHANT_CANCELLED_LISTINGS_DATA`
