create or replace table `shopify-pubsub-project.Data_Warehouse_Shopify_Staging.Sanity_check` as
with Sources as
  (
    select 
  'Orders' as Source_table_name,
  max(created_at) as Source_max_date,
  count(distinct case when date(created_at) = (select max(date(Order_created_at)) from `shopify-pubsub-project.Data_Warehouse_Shopify_Staging.Orders`) then id end ) as Source_pk_count
  from `shopify-pubsub-project.pilgrim_bi_airbyte.orders`

 union all

select 
  'Draft Orders' as Source_table_name,
  max(created_at) as Source_max_date,
  count(distinct case when date(created_at) = (select max(date(Draft_order_created_at)) from `shopify-pubsub-project.Data_Warehouse_Shopify_Staging.Draft_orders`) then id end ) as Source_pk_count
  from `shopify-pubsub-project.pilgrim_bi_airbyte.draft_orders`


 union all

select 
  'Products' as Source_table_name,
  max(created_at) as Source_max_date,
  count(distinct case when date(created_at) = (select max(date(Product_created_at)) from `shopify-pubsub-project.Data_Warehouse_Shopify_Staging.Products`) then id end ) as Source_pk_count
  from `shopify-pubsub-project.pilgrim_bi_airbyte.products`


  union all

select 
  'Customers' as Source_table_name,
  max(created_at) as Source_max_date,
  count(distinct case when date(created_at) = (select max(date(Customer_created_at)) from `shopify-pubsub-project.Data_Warehouse_Shopify_Staging.Customers`) then id end ) as Source_pk_count
  from `shopify-pubsub-project.pilgrim_bi_airbyte.customers`


  union all

select 
  'Refund Orders' as Source_table_name,
  max(created_at) as Source_max_date,
  count(distinct case when date(created_at) = (select max(date(Refund_created_at)) from `shopify-pubsub-project.Data_Warehouse_Shopify_Staging.Refund_Orders`) then id end ) as Source_pk_count
  from `shopify-pubsub-project.pilgrim_bi_airbyte.order_refunds`


union all

select 
  'Transactions' as Source_table_name,
  max(created_at) as Source_max_date,
  count(distinct case when date(created_at) = (select max(date(Trans_created_at)) from `shopify-pubsub-project.Data_Warehouse_Shopify_Staging.Transactions`) then id end ) as Source_pk_count
  from `shopify-pubsub-project.pilgrim_bi_airbyte.transactions`


union all

select 
  'Pages' as Source_table_name,
  max(created_at) as Source_max_date,
  count(distinct case when date(created_at) = (select max(date(Page_created_at)) from `shopify-pubsub-project.Data_Warehouse_Shopify_Staging.Pages`) then id end ) as Source_pk_count
  from `shopify-pubsub-project.pilgrim_bi_airbyte.pages`

union all

select 
  'Metafield Pages' as Source_table_name,
  max(created_at) as Source_max_date,
  count(distinct case when date(created_at) = (select max(date(Metafield_page_created_at)) from `shopify-pubsub-project.Data_Warehouse_Shopify_Staging.Metafield_pages`) then id end ) as Source_pk_count
  from `shopify-pubsub-project.pilgrim_bi_airbyte.metafield_pages`


union all

select 
  'Inventory Level' as Source_table_name,
  max(created_at) as Source_max_date,
  count(distinct case when date(created_at) = (select max(date(inventory_created_at)) from `shopify-pubsub-project.Data_Warehouse_Shopify_Staging.Inventory_level`) then id end ) as Source_pk_count
  from `shopify-pubsub-project.pilgrim_bi_airbyte.inventory_levels`


union all

select 
  'Location' as Source_table_name,
  max(created_at) as Source_max_date,
  count(distinct case when date(created_at) = (select max(date(location_created_at)) from `shopify-pubsub-project.Data_Warehouse_Shopify_Staging.Locations`) then id end ) as Source_pk_count
  from `shopify-pubsub-project.pilgrim_bi_airbyte.locations`

union all

select 
  'Articles' as Source_table_name,
  max(created_at) as Source_max_date,
  count(distinct case when date(created_at) = (select max(date(Article_created_at)) from `shopify-pubsub-project.Data_Warehouse_Shopify_Staging.Articles`) then id end ) as Source_pk_count
  from `shopify-pubsub-project.pilgrim_bi_airbyte.articles`


union all

select 
  'Discount Code' as Source_table_name,
  max(created_at) as Source_max_date,
  count(distinct case when date(created_at) = (select max(date(Discount_code_created_at)) from `shopify-pubsub-project.Data_Warehouse_Shopify_Staging.Discount_Code`) then id end ) as Source_pk_count
  from `shopify-pubsub-project.pilgrim_bi_airbyte.discount_codes`

union all

select 
  'Metafield Orders' as Source_table_name,
  max(created_at) as Source_max_date,
  count(distinct case when date(created_at) = (select max(date(Order_created_at)) from `shopify-pubsub-project.Data_Warehouse_Shopify_Staging.Metafield_orders`) then id end ) as Source_pk_count
  from `shopify-pubsub-project.pilgrim_bi_airbyte.metafield_orders`


union all

select 
  'Abandoned Checkout' as Source_table_name,
  max(created_at) as Source_max_date,
  count(distinct case when date(created_at) = (select max(date(aband_created_at)) from `shopify-pubsub-project.Data_Warehouse_Shopify_Staging.Abandoned_checkout`) then id end ) as Source_pk_count
  from `shopify-pubsub-project.pilgrim_bi_airbyte.abandoned_checkouts`
  

union all

select 
  'Product Variants' as Source_table_name,
  max(created_at) as Source_max_date,
  count(distinct case when date(created_at) = (select max(date(variant_created_at)) from `shopify-pubsub-project.Data_Warehouse_Shopify_Staging.Product_variants`) then id end ) as Source_pk_count
  from `shopify-pubsub-project.pilgrim_bi_airbyte.product_variants`

union all

select 
  'Metafield Articles' as Source_table_name,
  max(created_at) as Source_max_date,
  count(distinct case when date(created_at) = (select max(date(Metafield_article_created_at)) from `shopify-pubsub-project.Data_Warehouse_Shopify_Staging.Metafield_Articles`) then id end ) as Source_pk_count
  from `shopify-pubsub-project.pilgrim_bi_airbyte.metafield_articles`
  

 union all

select 
  'Customer Address' as Source_table_name,
  max(updated_at) as Source_max_date,
  count(distinct case when date(updated_at) = (select max(date(Cust_updated_at)) from `shopify-pubsub-project.Data_Warehouse_Shopify_Staging.Customer_Address`) then id end ) as Source_pk_count
  from `shopify-pubsub-project.pilgrim_bi_airbyte.customer_address` 
  ),
-----------------------------------------------------------------------------------------------------
Destination as 
(
    select 
  'Orders' as Dest_table_name,
  max(Order_created_at) as Dest_max_date,
  count(distinct case when date(Order_created_at) = (select max(date(created_at)) from `shopify-pubsub-project.pilgrim_bi_airbyte.orders`) then order_id end ) as Dest_pk_count
  from  `shopify-pubsub-project.Data_Warehouse_Shopify_Staging.Orders`

  
  union all

  select 
  'Draft Orders' as Dest_table_name,
  max(Draft_order_created_at) as Dest_max_date,
 count(distinct case when date(Draft_order_created_at) = (select max(date(created_at)) from `shopify-pubsub-project.pilgrim_bi_airbyte.draft_orders`) then Draft_order_id end ) as Dest_pk_count
  from `shopify-pubsub-project.Data_Warehouse_Shopify_Staging.Draft_orders`


union all

  select 
  'Products' as Dest_table_name,
  max(Product_created_at) as Dest_max_date,
 count(distinct case when date(Product_created_at) = (select max(date(created_at)) from `shopify-pubsub-project.pilgrim_bi_airbyte.products`) then Product_id end ) as Dest_pk_count
  from `shopify-pubsub-project.Data_Warehouse_Shopify_Staging.Products`


union all

  select 
  'Customers' as Dest_table_name,
  max(Customer_created_at) as Dest_max_date,
 count(distinct case when date(Customer_created_at) = (select max(date(created_at)) from `shopify-pubsub-project.pilgrim_bi_airbyte.customers`) then customer_id end ) as Dest_pk_count
  from `shopify-pubsub-project.Data_Warehouse_Shopify_Staging.Customers`


union all

  select 
  'Refund Orders' as Dest_table_name,
  max(Refund_created_at) as Dest_max_date,
 count(distinct case when date(Refund_created_at) = (select max(date(created_at)) from `shopify-pubsub-project.pilgrim_bi_airbyte.order_refunds`) then Refund_id end ) as Dest_pk_count
  from `shopify-pubsub-project.Data_Warehouse_Shopify_Staging.Refund_Orders`


union all

  select 
  'Transactions' as Dest_table_name,
  max(Trans_created_at) as Dest_max_date,
 count(distinct case when date(Trans_created_at) = (select max(date(created_at)) from `shopify-pubsub-project.pilgrim_bi_airbyte.transactions`) then Trans_id end ) as Dest_pk_count
  from `shopify-pubsub-project.Data_Warehouse_Shopify_Staging.Transactions`


union all

  select 
  'Pages' as Dest_table_name,
  max(Page_created_at) as Dest_max_date,
 count(distinct case when date(Page_created_at) = (select max(date(created_at)) from `shopify-pubsub-project.pilgrim_bi_airbyte.pages`) then Page_id end ) as Dest_pk_count
  from `shopify-pubsub-project.Data_Warehouse_Shopify_Staging.Pages`


union all

  select 
  'Metafield Pages' as Dest_table_name,
  max(Metafield_page_created_at) as Dest_max_date,
 count(distinct case when date(Metafield_page_created_at) = (select max(date(created_at)) from `shopify-pubsub-project.pilgrim_bi_airbyte.metafield_pages`) then Metafield_page_id end ) as Dest_pk_count
  from `shopify-pubsub-project.Data_Warehouse_Shopify_Staging.Metafield_pages`

union all

  select 
  'Inventory Level' as Dest_table_name,
  max(inventory_created_at) as Dest_max_date,
 count(distinct case when date(inventory_created_at) = (select max(date(created_at)) from `shopify-pubsub-project.pilgrim_bi_airbyte.inventory_levels`) then inventory_level_id end ) as Dest_pk_count
  from `shopify-pubsub-project.Data_Warehouse_Shopify_Staging.Inventory_level`


 union all

  select 
  'Location' as Dest_table_name,
  max(location_created_at) as Dest_max_date,
 count(distinct case when date(location_created_at) = (select max(date(created_at)) from `shopify-pubsub-project.pilgrim_bi_airbyte.locations`) then location_id end ) as Dest_pk_count
  from `shopify-pubsub-project.Data_Warehouse_Shopify_Staging.Locations` 

 union all

  select 
  'Articles' as Dest_table_name,
  max(Article_created_at) as Dest_max_date,
 count(distinct case when date(Article_created_at) = (select max(date(created_at)) from `shopify-pubsub-project.pilgrim_bi_airbyte.articles`) then id end ) as Dest_pk_count
  from `shopify-pubsub-project.Data_Warehouse_Shopify_Staging.Articles` 


 union all

  select 
  'Discount Code' as Dest_table_name,
  max(Discount_code_created_at) as Dest_max_date,
 count(distinct case when date(Discount_code_created_at) = (select max(date(created_at)) from `shopify-pubsub-project.pilgrim_bi_airbyte.discount_codes`) then Discount_code_id end ) as Dest_pk_count
  from `shopify-pubsub-project.Data_Warehouse_Shopify_Staging.Discount_Code` 


  union all

  select 
  'Metafield Orders' as Dest_table_name,
  max(Order_created_at) as Dest_max_date,
 count(distinct case when date(Order_created_at) = (select max(date(created_at)) from `shopify-pubsub-project.pilgrim_bi_airbyte.metafield_orders`) then Metafield_order_id end ) as Dest_pk_count
  from `shopify-pubsub-project.Data_Warehouse_Shopify_Staging.Metafield_orders` 


union all

  select 
  'Abandoned Checkout' as Dest_table_name,
  max(aband_created_at) as Dest_max_date,
 count(distinct case when date(aband_created_at) = (select max(date(created_at)) from `shopify-pubsub-project.pilgrim_bi_airbyte.abandoned_checkouts`) then abandoned_checkout_id end ) as Dest_pk_count
  from `shopify-pubsub-project.Data_Warehouse_Shopify_Staging.Abandoned_checkout` 

union all

  select 
  'Product Variants' as Dest_table_name,
  max(variant_created_at) as Dest_max_date,
 count(distinct case when date(variant_created_at) = (select max(date(created_at)) from `shopify-pubsub-project.pilgrim_bi_airbyte.product_variants`) then variant_id end ) as Dest_pk_count
  from `shopify-pubsub-project.Data_Warehouse_Shopify_Staging.Product_variants` 
  
union all

  select 
  'Metafield Articles' as Dest_table_name,
  max(Metafield_article_created_at) as Dest_max_date,
 count(distinct case when date(Metafield_article_created_at) = (select max(date(created_at)) from `shopify-pubsub-project.pilgrim_bi_airbyte.metafield_articles`) then Metafield_article_id end ) as Dest_pk_count
  from `shopify-pubsub-project.Data_Warehouse_Shopify_Staging.Metafield_Articles` 


union all

  select 
  'Customer Address' as Dest_table_name,
  max(Cust_updated_at) as Dest_max_date,
 count(distinct case when date(Cust_updated_at) = (select max(date(updated_at)) from `shopify-pubsub-project.pilgrim_bi_airbyte.customer_address`) then customer_id end ) as Dest_pk_count
  from `shopify-pubsub-project.Data_Warehouse_Shopify_Staging.Customer_Address` 


)


  select 
    S.Source_table_name,
    Date(S.Source_max_date) as Source_max_date,
    Date(D.Dest_max_date) as Dest_max_date,
    Current_date() as Latest_date,
    S.Source_pk_count,
    D.Dest_pk_count,

  from Sources as S
  left join Destination as D
  on S.Source_table_name = D.Dest_table_name

