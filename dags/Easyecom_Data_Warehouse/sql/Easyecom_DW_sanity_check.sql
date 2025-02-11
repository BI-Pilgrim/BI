create or replace table `shopify-pubsub-project.Data_Warehouse_Easyecom_Staging.Sanity_check` as
with Sources as
  (
  --     select 
  -- 'activities' as table_name,
  -- 'activities' as source_table,
  -- max(date(event_time)) as Source_max_date,
  
  -- count(distinct case when date(event_time) = (select max(date(event_time)) from `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.activities`) then concat(object_id, event_time, account_id) end ) as Source_pk_count
  -- from `shopify-pubsub-project.pilgrim_bi_airbyte_facebook.activities`

  select 
  'Orders' as table_name,
  'Orders' as source_table,
  max(date(order_date)) as Source_max_date,
  count(distinct case when date(order_date) = (select max(date(order_date)) from `shopify-pubsub-project.Data_Warehouse_Easyecom_Staging.Orders`) then invoice_id end ) as Source_pk_count
  from `shopify-pubsub-project.easycom.orders`

 union all

-- Since Orderitem table is derived from orders therefor count(distinct pk) of source and destination are same
  select 
  'Order_item' as table_name,
  'Orders' as source_table,
  max(date(order_date)) as Source_max_date,
  0 as Source_pk_count
  from `shopify-pubsub-project.Data_Warehouse_Easyecom_Staging.Orderitem`

 union all

  select 
  'master_products' as table_name,
  'master_products' as source_table,
  max(date(created_at)) as Source_max_date,
  count(distinct case when date(created_at) = (select max(date(created_at)) from `shopify-pubsub-project.Data_Warehouse_Easyecom_Staging.Master_products`) then cp_id end ) as Source_pk_count
  from `shopify-pubsub-project.easycom.master_products`

  union all

  select 
  'locations' as table_name,
  'locations' as source_table,
  max(date(ee_extracted_at)) as Source_max_date,
  count(distinct case when date(ee_extracted_at) = (select max(date(ee_extracted_at)) from `shopify-pubsub-project.Data_Warehouse_Easyecom_Staging.Locations`) then location_key end ) as Source_pk_count
  from `shopify-pubsub-project.easycom.locations`

  union all

  select 
  'inventory_snapshot' as table_name,
  'inventory_snapshot' as source_table,
  max(date(entry_date)) as Source_max_date,
  count(distinct case when date(entry_date) = (select max(date(entry_date)) from `shopify-pubsub-project.Data_Warehouse_Easyecom_Staging.Inventory_Snapshot`) then id end ) as Source_pk_count
  from `shopify-pubsub-project.easycom.inventory_snapshot`

  union all

  select 
  'inventory_aging_report' as table_name,
  'inventory_aging_report' as source_table,
  max(date(end_date)) as Source_max_date,
  count(distinct case when date(end_date) = (select max(date(end_date)) from `shopify-pubsub-project.Data_Warehouse_Easyecom_Staging.Inventory_Aging_report`) then concat(sku,location) end ) as Source_pk_count
  from `shopify-pubsub-project.easycom.inventory_aging_report`

  union all

  select 
  'countries' as table_name,
  'countries' as source_table,
  max(date(ee_extracted_at)) as Source_max_date,
  count(distinct case when date(ee_extracted_at) = (select max(date(ee_extracted_at)) from `shopify-pubsub-project.Data_Warehouse_Easyecom_Staging.Countries`) then country_id end ) as Source_pk_count
  from `shopify-pubsub-project.easycom.countries`

  union all

  select 
  'marketplace' as table_name,
  'marketplace' as source_table,
  max(date(ee_extracted_at)) as Source_max_date,
  count(distinct case when date(ee_extracted_at) = (select max(date(ee_extracted_at)) from `shopify-pubsub-project.Data_Warehouse_Easyecom_Staging.Marketplace`) then marketplace_id end ) as Source_pk_count
  from `shopify-pubsub-project.easycom.marketplace`

  union all

  -- select 
  -- 'mini_sales_report' as table_name,
  -- 'marketplace' as source_table,
  -- max(date(start_date)) as Source_max_date,
  -- count(distinct case when date(start_date) = (select max(date(start_date)) from `shopify-pubsub-project.Data_Warehouse_Easyecom_Staging.Mini_Sales_report`) then concat(Order_Number, Suborder_No, report_id) end ) as Source_pk_count
  -- from `shopify-pubsub-project.easycom.mini_sales_report`  

  -- union all

  select 
  'all_return_orders' as table_name,
  'all_return_orders' as source_table,
  max(date(order_date)) as Source_max_date,
  count(distinct case when date(order_date) = (select max(order_date) from `shopify-pubsub-project.Data_Warehouse_Easyecom_Staging.All_Return_Orders`) then order_id end ) as Source_pk_count
  from `shopify-pubsub-project.easycom.all_return_orders`  

 union all

-- Since All_Return_Order_items table is derived from All_Return_Order therefor count(distinct pk) of source and destination are same
  select 
  'All_Return_Order_items' as table_name,
  'all_return_orders' as source_table,
  max(date(order_date)) as Source_max_date,
  0 as Source_pk_count
  from `shopify-pubsub-project.Data_Warehouse_Easyecom_Staging.All_Return_Orders`

  union all

  select 
  'status_wise_stock_report' as table_name,
  'status_wise_stock_report' as source_table,
  max(date(created_on)) as Source_max_date,
  count(distinct case when date(created_on) = (select max(date(created_on)) from `shopify-pubsub-project.Data_Warehouse_Easyecom_Staging.Status_Wise_Stock_report`) then concat(SKU, Company_Token) end ) as Source_pk_count
  from `shopify-pubsub-project.easycom.status_wise_stock_report`  


  union all

  select 
  'states' as table_name,
  'states' as source_table,
  max(date(ee_extracted_at)) as Source_max_date,
  count(distinct case when ee_extracted_at = (select max(ee_extracted_at) from `shopify-pubsub-project.Data_Warehouse_Easyecom_Staging.States`) then state_id end ) as Source_pk_count
  from `shopify-pubsub-project.easycom.states` 


  union all

  -- select 
  -- 'order_status_metrics' as table_name,
  -- max(date(date)) as Source_max_date,
  -- count(distinct case when date = (select max(date) from `shopify-pubsub-project.Data_Warehouse_Easyecom_Staging.order_status_metrics`) then concat(date,status) end ) as Source_pk_count
  -- from `shopify-pubsub-project.easycom.order_status_metrics`

  -- union all

  select 
  'pending_returns_report' as table_name,
  'pending_returns_report' as source_table,
  max(date(end_date)) as Source_max_date,
  count(distinct case when end_date = (select max(end_date) from `shopify-pubsub-project.Data_Warehouse_Easyecom_Staging.Pending_Returns_report`) then concat(Order_Number, Order_Item_ID) end ) as Source_pk_count
  from `shopify-pubsub-project.easycom.pending_returns_report`  

  union all

  select 
  'purchase_orders' as table_name,
  'purchase_orders' as source_table,
  max(date(ee_extracted_at)) as Source_max_date,
  count(distinct case when ee_extracted_at = (select max(ee_extracted_at) from `shopify-pubsub-project.Data_Warehouse_Easyecom_Staging.Purchase_Orders`) then concat(po_id, ee_extracted_at) end ) as Source_pk_count
  from `shopify-pubsub-project.easycom.purchase_orders`

  -- union all

  -- select 
  -- 'warehouse_metrics' as table_name,
  -- 'warehouse_metrics' as source_table,
  -- max(date(analysis_date)) as Source_max_date,
  -- count(distinct case when date(analysis_date) = (select max(date(analysis_date)) from `shopify-pubsub-project.Data_Warehouse_Easyecom_Staging.warehouse_metrics`) then concat(location_key, warehouse_name) end ) as Source_pk_count
  -- from `shopify-pubsub-project.easycom.warehouse_metrics`


  union all

  select 
  'kits' as table_name,
  'kits' as source_table,
  max(date(add_date)) as Source_max_date,
  count(distinct case when date(add_date) = (select max(date(add_date)) from `shopify-pubsub-project.Data_Warehouse_Easyecom_Staging.Kits`) then product_id end ) as Source_pk_count
  from `shopify-pubsub-project.easycom.kits`


  -- union all

  -- select 
  -- 'shipping_status_metrics' as table_name,
  -- 'shipping_status_metrics' as source_table,
  -- max(date(analysis_date)) as Source_max_date,
  -- count(distinct case when date(analysis_date) = (select max(date(analysis_date)) from `shopify-pubsub-project.Data_Warehouse_Easyecom_Staging.shipping_status_metrics`) then concat(analysis_date, status) end ) as Source_pk_count
  -- from `shopify-pubsub-project.easycom.shipping_status_metrics`


  union all

  select 
  'reports' as table_name,
  'reports' as source_table,
  max(date(created_on)) as Source_max_date,
  count(distinct case when date(created_on) = (select max(date(created_on)) from `shopify-pubsub-project.Data_Warehouse_Easyecom_Staging.Reports`) then report_id end ) as Source_pk_count
  from `shopify-pubsub-project.easycom.reports`

  union all

  select 
  'pending_return_orders' as table_name,
  'pending_return_orders' as source_table,
  max(date(import_date)) as Source_max_date,
  count(distinct case when date(import_date) = (select max(date(import_date)) from `shopify-pubsub-project.Data_Warehouse_Easyecom_Staging.pending_return_orders`) then invoice_id end ) as Source_pk_count
  from `shopify-pubsub-project.easycom.pending_return_orders`


  union all

  select 
  'vendors' as table_name,
  'vendors' as source_table,
  max(date(ee_extracted_at)) as Source_max_date,
  count(distinct case when date(ee_extracted_at) = (select max(date(ee_extracted_at)) from `shopify-pubsub-project.Data_Warehouse_Easyecom_Staging.Vendors`) then vendor_c_id end ) as Source_pk_count
  from `shopify-pubsub-project.easycom.vendors`


  union all

  select 
  'inventory_view_by_bin_report' as table_name,
  'inventory_view_by_bin_report' as source_table,
  max(date(created_on)) as Source_max_date,
  count(distinct case when date(created_on) = (select max(date(created_on)) from `shopify-pubsub-project.Data_Warehouse_Easyecom_Staging.inventory_view_by_bin_report`) then SKU end ) as Source_pk_count
  from `shopify-pubsub-project.easycom.inventory_view_by_bin_report`


  union all

  select 
  'inventory_details' as table_name,
  'inventory_details' as source_table,
  max(date(creation_date)) as Source_max_date,
  count(distinct case when date(creation_date) = (select max(date(creation_date)) from `shopify-pubsub-project.Data_Warehouse_Easyecom_Staging.Inventory_details`) then SKU end ) as Source_pk_count
  from `shopify-pubsub-project.easycom.inventory_details`


  -- union all

  -- select 
  -- 'daily_metrics' as table_name,
  -- 'inventory_details' as source_table,
  -- max(date(analysis_date)) as Source_max_date,
  -- count(distinct case when date(analysis_date) = (select max(date(analysis_date)) from `shopify-pubsub-project.Data_Warehouse_Easyecom_Staging.daily_metrics`) then analysis_date end ) as Source_pk_count
  -- from `shopify-pubsub-project.easycom.daily_metrics`


  union all

  select 
  'customers' as table_name,
  'customers' as source_table,
  max(date(ee_extracted_at)) as Source_max_date,
  count(distinct case when date(ee_extracted_at) = (select max(date(ee_extracted_at)) from `shopify-pubsub-project.Data_Warehouse_Easyecom_Staging.Customers`) then c_id end ) as Source_pk_count
  from `shopify-pubsub-project.easycom.customers`


  union all

  select 
  'grn_details' as table_name,
  'grn_details' as source_table,
  max(date(grn_invoice_date)) as Source_max_date,
  count(distinct case when date(grn_invoice_date) = (select max(date(grn_invoice_date)) from `shopify-pubsub-project.Data_Warehouse_Easyecom_Staging.grn_details`) then grn_id end ) as Source_pk_count
  from `shopify-pubsub-project.easycom.grn_details`


  union all

  select 
  'marketplace_listings' as table_name,
  'marketplace_listings' as source_table,
  max(date(ee_extracted_at)) as Source_max_date,
  count(distinct case when date(ee_extracted_at) = (select max(date(ee_extracted_at)) from `shopify-pubsub-project.Data_Warehouse_Easyecom_Staging.Marketplace_listings`) then concat(sku,site_uid) end ) as Source_pk_count
  from `shopify-pubsub-project.easycom.marketplace_listings`


  union all

  select 
  'grn_details_report' as table_name,
  'grn_details' as source_table,
  max(date(created_on)) as Source_max_date,
  0 as Source_pk_count
  from `shopify-pubsub-project.easycom.grn_details_report`
),

---------------------------------------------------------------------------------------------------------------------------------------------------------
Staging as 
(
  select 
  'Orders' as table_name,
  max(date(order_date)) as Staging_max_date,
  max(date(order_date)) as Latest_Valid_Date,
  count(distinct case when date(order_date) = (select max(date(order_date)) from `shopify-pubsub-project.Data_Warehouse_Easyecom_Staging.Orders`) then invoice_id end ) as Staging_pk_count
  from  `shopify-pubsub-project.Data_Warehouse_Easyecom_Staging.Orders`

  union all

  select 
  'Order_item' as table_name,
  max(date(order_date)) as Staging_max_date,
  (SELECT MAX(date(order_date)) as max_valid_date
  FROM `shopify-pubsub-project.Data_Warehouse_Easyecom_Staging.Orderitem`
  WHERE order_id IS NOT NULL
   and suborder_id IS NOT NULL
   and invoice_id IS NOT NULL
) as Latest_Valid_Date,
  0 as Staging_pk_count
  from `shopify-pubsub-project.Data_Warehouse_Easyecom_Staging.Orderitem`

  union all

  select 
  'master_products' as table_name,
  max(date(created_at)) as Staging_max_date,
  max(date(created_at)) as Latest_Valid_Date,
  count(distinct case when date(created_at) = (select max(date(created_at)) from `shopify-pubsub-project.easycom.master_products`) then cp_id end) as Staging_pk_count
  from `shopify-pubsub-project.Data_Warehouse_Easyecom_Staging.Master_products`

  union all

  select 
  'locations' as table_name,
  max(date(ee_extracted_at)) as Staging_max_date,
  max(date(ee_extracted_at)) as Latest_Valid_Date,
  count(distinct case when date(ee_extracted_at) = (select max(date(ee_extracted_at)) from `shopify-pubsub-project.easycom.locations`) then location_key end ) as Staging_pk_count
  from  `shopify-pubsub-project.Data_Warehouse_Easyecom_Staging.Locations`

  union all

  select 
  'inventory_snapshot' as table_name,
  max(date(entry_date)) as Staging_max_date,
  max(date(entry_date)) as Latest_Valid_Date,
  count(distinct case when date(entry_date) = (select max(date(entry_date)) from `shopify-pubsub-project.easycom.inventory_snapshot`) then id end ) as Staging_pk_count
  from  `shopify-pubsub-project.Data_Warehouse_Easyecom_Staging.Inventory_Snapshot`

  union all

  select 
  'inventory_aging_report' as table_name,
  max(date(end_date)) as Staging_max_date,
  max(date(end_date)) as Latest_Valid_Date,
  count(distinct case when date(end_date) = (select max(date(end_date)) from `shopify-pubsub-project.easycom.inventory_aging_report`) then concat(sku,location) end ) as Staging_pk_count
  from  `shopify-pubsub-project.Data_Warehouse_Easyecom_Staging.Inventory_Aging_report`

  union all

  select 
  'countries' as table_name,
  max(date(ee_extracted_at)) as Staging_max_date,
  max(date(ee_extracted_at)) as Latest_Valid_Date,
  count(distinct case when date(ee_extracted_at) = (select max(date(ee_extracted_at)) from `shopify-pubsub-project.easycom.countries`) then country_id end ) as Staging_pk_count
  from  `shopify-pubsub-project.Data_Warehouse_Easyecom_Staging.Countries`

  union all

  select 
  'marketplace' as table_name,
  max(date(ee_extracted_at)) as Staging_max_date,
  max(date(ee_extracted_at)) as Latest_Valid_Date,
  count(distinct case when date(ee_extracted_at) = (select max(date(ee_extracted_at)) from `shopify-pubsub-project.easycom.marketplace`) then marketplace_id end ) as Staging_pk_count
  from  `shopify-pubsub-project.Data_Warehouse_Easyecom_Staging.Marketplace`

  union all

  select 
  'mini_sales_report' as table_name,
  max(date(start_date)) as Staging_max_date,
  max(date(start_date)) as Latest_Valid_Date,
  count(distinct case when date(start_date) = (select max(date(start_date)) from `shopify-pubsub-project.easycom.mini_sales_report`) then concat(Order_Number, Suborder_No, report_id) end ) as Staging_pk_count
  from  `shopify-pubsub-project.Data_Warehouse_Easyecom_Staging.Mini_Sales_report`

  union all

  select 
  'all_return_orders' as table_name,
  max(date(order_date)) as Staging_max_date,
  max(date(order_date)) as Latest_Valid_Date,
  count(distinct case when date(order_date) = (select max(order_date) from `shopify-pubsub-project.easycom.all_return_orders`) then order_id end ) as Staging_pk_count
  from  `shopify-pubsub-project.Data_Warehouse_Easyecom_Staging.All_Return_Orders`

  union all

  select 
  'All_Return_Order_items' as table_name,
  max(date(order_date)) as Staging_max_date,
  (SELECT MAX(DATE(order_date))  
  FROM `shopify-pubsub-project.Data_Warehouse_Easyecom_Staging.Orderitem`
  WHERE order_id IS NOT NULL
  ) as Latest_Valid_Date,
 count(distinct case when date(order_date) = (select max(order_date) from `shopify-pubsub-project.Data_Warehouse_Easyecom_Staging.All_Return_Order_items`) then order_id end ) as Staging_pk_count
  from `shopify-pubsub-project.Data_Warehouse_Easyecom_Staging.All_Return_Order_items`

  union all

  select 
  'status_wise_stock_report' as table_name,
  max(date(created_on)) as Staging_max_date,
  max(date(created_on)) as Latest_Valid_Date,
  count(distinct case when date(created_on) = (select max(date(created_on)) from `shopify-pubsub-project.easycom.status_wise_stock_report`) then concat(SKU, Company_Token) end ) as Staging_pk_count
  from  `shopify-pubsub-project.Data_Warehouse_Easyecom_Staging.Status_Wise_Stock_report`

  union all

  select 
  'states' as table_name,
  max(date(ee_extracted_at)) as Staging_max_date,
  max(date(ee_extracted_at)) as Latest_Valid_Date,
  count(distinct case when date(ee_extracted_at) = (select max(date(ee_extracted_at)) from `shopify-pubsub-project.easycom.states`) then state_id end ) as Staging_pk_count
  from  `shopify-pubsub-project.Data_Warehouse_Easyecom_Staging.States`  

  -- union all

  -- select 
  -- 'order_status_metrics' as table_name,
  -- max(date(date)) as Staging_max_date,
  -- count(distinct case when date(date) = (select max(date(date)) from `shopify-pubsub-project.easycom.order_status_metrics`) then concat(date,status) end ) as Staging_pk_count
  -- from  `shopify-pubsub-project.Data_Warehouse_Easyecom_Staging.order_status_metrics` 

  union all

  select 
  'pending_returns_report' as table_name,
  max(date(end_date)) as Staging_max_date,
  max(date(end_date)) as Latest_Valid_Date,
  count(distinct case when date(end_date) = (select max(date(end_date)) from `shopify-pubsub-project.easycom.pending_returns_report`) then concat(Order_Number, Order_Item_ID) end ) as Staging_pk_count
  from  `shopify-pubsub-project.Data_Warehouse_Easyecom_Staging.Pending_Returns_report`  

  union all

  select 
  'purchase_orders' as table_name,
  max(date(ee_extracted_at)) as Staging_max_date,
  max(date(ee_extracted_at)) as Latest_Valid_Date,
  count(distinct case when date(ee_extracted_at) = (select max(date(ee_extracted_at)) from `shopify-pubsub-project.easycom.purchase_orders`) then concat(po_id,ee_extracted_at) end ) as Staging_pk_count
  from  `shopify-pubsub-project.Data_Warehouse_Easyecom_Staging.Purchase_Orders` 


  -- union all

  -- select 
  -- 'warehouse_metrics' as table_name,
  -- max(date(analysis_date)) as Staging_max_date,
  -- count(distinct case when date(analysis_date) = (select max(date(analysis_date)) from `shopify-pubsub-project.easycom.warehouse_metrics`) then concat(location_key, warehouse_name) end ) as Staging_pk_count
  -- from  `shopify-pubsub-project.Data_Warehouse_Easyecom_Staging.warehouse_metrics`


  union all

  select 
  'kits' as table_name,
  max(date(add_date)) as Staging_max_date,
  max(add_date) as Latest_Valid_Date,
  count(distinct case when date(add_date) = (select max(date(add_date)) from `shopify-pubsub-project.easycom.kits`) then product_id end ) as Staging_pk_count
  from  `shopify-pubsub-project.Data_Warehouse_Easyecom_Staging.Kits` 


  -- union all

  -- select 
  -- 'shipping_status_metrics' as table_name,
  -- max(date(analysis_date)) as Staging_max_date,
  -- count(distinct case when date(analysis_date) = (select max(date(analysis_date)) from `shopify-pubsub-project.easycom.shipping_status_metrics`) then concat(analysis_date, status) end ) as Staging_pk_count
  -- from  `shopify-pubsub-project.Data_Warehouse_Easyecom_Staging.shipping_status_metrics`


  union all

  select 
  'reports' as table_name,
  max(date(created_on)) as Staging_max_date,
  max(date(created_on)) as Latest_Valid_Date,
  count(distinct case when date(created_on) = (select max(date(created_on)) from `shopify-pubsub-project.easycom.reports`) then report_id end ) as Staging_pk_count
  from  `shopify-pubsub-project.Data_Warehouse_Easyecom_Staging.Reports` 


  union all

  select 
  'pending_return_orders' as table_name,
  max(date(import_date)) as import_date,
  max(import_date) as Latest_Valid_Date,
  count(distinct case when date(import_date) = (select max(date(import_date)) from `shopify-pubsub-project.easycom.pending_return_orders`) then invoice_id end ) as Staging_pk_count
  from  `shopify-pubsub-project.Data_Warehouse_Easyecom_Staging.pending_return_orders` 


  union all

  select 
  'vendors' as table_name,
  max(date(ee_extracted_at)) as import_date,
  max(date(ee_extracted_at)) as Latest_Valid_Date,
  count(distinct case when date(ee_extracted_at) = (select max(date(ee_extracted_at)) from `shopify-pubsub-project.easycom.vendors`) then vendor_c_id end ) as Staging_pk_count
  from  `shopify-pubsub-project.Data_Warehouse_Easyecom_Staging.Vendors` 


  union all

  select 
  'inventory_view_by_bin_report' as table_name,
  max(date(created_on)) as import_date,
  max(date(created_on)) as Latest_Valid_Date,
  count(distinct case when date(created_on) = (select max(date(created_on)) from `shopify-pubsub-project.easycom.inventory_view_by_bin_report`) then SKU end ) as Staging_pk_count
  from  `shopify-pubsub-project.Data_Warehouse_Easyecom_Staging.inventory_view_by_bin_report` 


  union all

  select 
  'inventory_details' as table_name,
  max(date(creation_date)) as import_date,
  max(date(creation_date)) as Latest_Valid_Date,
  count(distinct case when date(creation_date) = (select max(date(creation_date)) from `shopify-pubsub-project.easycom.inventory_details`) then concat(product_id,company_product_id) end ) as Staging_pk_count
  from  `shopify-pubsub-project.Data_Warehouse_Easyecom_Staging.Inventory_details`


  -- union all

  -- select 
  -- 'daily_metrics' as table_name,
  -- max(date(analysis_date)) as import_date,
  -- count(distinct case when date(analysis_date) = (select max(date(analysis_date)) from `shopify-pubsub-project.easycom.daily_metrics`) then analysis_date end ) as Staging_pk_count
  -- from  `shopify-pubsub-project.Data_Warehouse_Easyecom_Staging.daily_metrics`


  union all

  select 
  'customers' as table_name,
  max(date(ee_extracted_at)) as import_date,
  max(date(ee_extracted_at)) as Latest_Valid_Date,
  count(distinct case when date(ee_extracted_at) = (select max(date(ee_extracted_at)) from `shopify-pubsub-project.easycom.customers`) then c_id end ) as Staging_pk_count
  from  `shopify-pubsub-project.Data_Warehouse_Easyecom_Staging.Customers`


  union all

  select 
  'grn_details' as table_name,
  max(date(grn_created_at)) as import_date,
  max(date(grn_created_at)) as Latest_Valid_Date,
  count(distinct case when date(grn_created_at) = (select max(date(grn_created_at)) from `shopify-pubsub-project.easycom.grn_details`) then grn_id end ) as Staging_pk_count
  from  `shopify-pubsub-project.Data_Warehouse_Easyecom_Staging.grn_details`


  union all

  select 
  'marketplace_listings' as table_name,
  max(date(ee_extracted_at)) as import_date,
  max(date(ee_extracted_at)) as Latest_Valid_Date,
  count(distinct case when date(ee_extracted_at) = (select max(date(ee_extracted_at)) from `shopify-pubsub-project.easycom.marketplace_listings`) then concat(sku,site_uid) end ) as Staging_pk_count
  from  `shopify-pubsub-project.Data_Warehouse_Easyecom_Staging.Marketplace_listings`


  union all

  select 
  'grn_details_report' as table_name,
  max(date(created_on)) as import_date,
  max(date(created_on)) as Latest_Valid_Date,
  0 as Staging_pk_count
  from  `shopify-pubsub-project.Data_Warehouse_Easyecom_Staging.grn_details_report`
)


  select
    So.table_name,
    So.source_table,
    Date(So.Source_max_date) as Source_max_date,
    Date(St.Staging_max_date) as Staging_max_date,
    Latest_Valid_Date,
    Current_date() as Date1,
    So.Source_pk_count,
    St.Staging_pk_count,

  from Sources as So
  left join Staging as St
  on So.table_name = St.table_name
  -- where Source_max_date < Latest_Valid_Date
  order by table_name