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

 union all

  select 
  'master_products' as Source_table_name,
  max(created_at) as Source_max_date,
  count(distinct case when date(created_at) = (select max(date(created_at)) from `shopify-pubsub-project.Data_Warehouse_Easyecom_Staging.Master_products`) then cp_id end) as Source_pk_count
  from `shopify-pubsub-project.easycom.master_products`

  union all

  select 
  'locations' as Source_table_name,
  max(ee_extracted_at) as Source_max_date,
  count(distinct case when date(ee_extracted_at) = (select max(date(ee_extracted_at)) from `shopify-pubsub-project.Data_Warehouse_Easyecom_Staging.Locations`) then location_key end ) as Source_pk_count
  from `shopify-pubsub-project.easycom.locations`

  union all

  select 
  'inventory_snapshot' as Source_table_name,
  max(entry_date) as Source_max_date,
  count(distinct case when date(entry_date) = (select max(date(entry_date)) from `shopify-pubsub-project.Data_Warehouse_Easyecom_Staging.Inventory_Snapshot`) then id end ) as Source_pk_count
  from `shopify-pubsub-project.easycom.inventory_snapshot`

  union all

  select 
  'inventory_aging_report' as Source_table_name,
  max(date(end_date)) as Source_max_date,
  count(distinct case when date(end_date) = (select max(date(end_date)) from `shopify-pubsub-project.Data_Warehouse_Easyecom_Staging.Inventory_Aging_report`) then concat(sku,location) end ) as Source_pk_count
  from `shopify-pubsub-project.easycom.inventory_aging_report`

  union all

  select 
  'countries' as Source_table_name,
  max(date(ee_extracted_at)) as Source_max_date,
  count(distinct case when date(ee_extracted_at) = (select max(date(ee_extracted_at)) from `shopify-pubsub-project.Data_Warehouse_Easyecom_Staging.Countries`) then country_id end ) as Source_pk_count
  from `shopify-pubsub-project.easycom.countries`

  union all

  select 
  'marketplace' as Source_table_name,
  max(date(ee_extracted_at)) as Source_max_date,
  count(distinct case when date(ee_extracted_at) = (select max(date(ee_extracted_at)) from `shopify-pubsub-project.Data_Warehouse_Easyecom_Staging.Marketplace`) then marketplace_id end ) as Source_pk_count
  from `shopify-pubsub-project.easycom.marketplace`

  union all

  select 
  'mini_sales_report' as Source_table_name,
  max(date(start_date)) as Source_max_date,
  count(distinct case when date(start_date) = (select max(date(start_date)) from `shopify-pubsub-project.Data_Warehouse_Easyecom_Staging.Mini_Sales_report`) then concat(Order_Number, Suborder_No, report_id) end ) as Source_pk_count
  from `shopify-pubsub-project.easycom.mini_sales_report`  

  union all

  select 
  'all_return_orders' as Source_table_name,
  max(date(order_date)) as Source_max_date,
  count(distinct case when date(order_date) = (select max(order_date) from `shopify-pubsub-project.Data_Warehouse_Easyecom_Staging.All_Return_Orders`) then order_id end ) as Source_pk_count
  from `shopify-pubsub-project.easycom.all_return_orders`  

 union all

-- Since All_Return_Order_items table is derived from All_Return_Order therefor count(distinct pk) of source and destination are same
  select 
  'All_Return_Order_items' as Source_table_name,
  max(order_date) as Source_max_date,
 count(distinct case when date(order_date) = (select max(order_date) from `shopify-pubsub-project.Data_Warehouse_Easyecom_Staging.All_Return_Orders`) then order_id end ) as Source_pk_count
  from `shopify-pubsub-project.Data_Warehouse_Easyecom_Staging.All_Return_Orders`

  union all

  select 
  'status_wise_stock_report' as Source_table_name,
  max(date(end_date)) as Source_max_date,
  count(distinct case when end_date = (select max(end_date) from `shopify-pubsub-project.Data_Warehouse_Easyecom_Staging.Status_Wise_Stock_report`) then concat(SKU, Company_Token) end ) as Source_pk_count
  from `shopify-pubsub-project.easycom.status_wise_stock_report`  


  union all

  select 
  'states' as Source_table_name,
  max(date(ee_extracted_at)) as Source_max_date,
  count(distinct case when ee_extracted_at = (select max(ee_extracted_at) from `shopify-pubsub-project.Data_Warehouse_Easyecom_Staging.States`) then state_id end ) as Source_pk_count
  from `shopify-pubsub-project.easycom.states` 


  union all

  select 
  'order_status_metrics' as Source_table_name,
  max(date(date)) as Source_max_date,
  count(distinct case when date = (select max(date) from `shopify-pubsub-project.Data_Warehouse_Easyecom_Staging.order_status_metrics`) then concat(date,status) end ) as Source_pk_count
  from `shopify-pubsub-project.easycom.order_status_metrics`

  union all

  select 
  'pending_returns_report' as Source_table_name,
  max(date(end_date)) as Source_max_date,
  count(distinct case when end_date = (select max(end_date) from `shopify-pubsub-project.Data_Warehouse_Easyecom_Staging.Pending_Returns_report`) then concat(Order_Number, Order_Item_ID) end ) as Source_pk_count
  from `shopify-pubsub-project.easycom.pending_returns_report`  

  union all

  select 
  'purchase_orders' as Source_table_name,
  max(date(ee_extracted_at)) as Source_max_date,
  count(distinct case when ee_extracted_at = (select max(ee_extracted_at) from `shopify-pubsub-project.Data_Warehouse_Easyecom_Staging.Purchase_Orders`) then concat(po_id, ee_extracted_at) end ) as Source_pk_count
  from `shopify-pubsub-project.easycom.purchase_orders`

  union all

  select 
  'warehouse_metrics' as Source_table_name,
  max(date(analysis_date)) as Source_max_date,
  count(distinct case when date(analysis_date) = (select max(date(analysis_date)) from `shopify-pubsub-project.Data_Warehouse_Easyecom_Staging.warehouse_metrics`) then concat(location_key, warehouse_name) end ) as Source_pk_count
  from `shopify-pubsub-project.easycom.warehouse_metrics`


  union all

  select 
  'kits' as Source_table_name,
  max(date(add_date)) as Source_max_date,
  count(distinct case when date(add_date) = (select max(date(add_date)) from `shopify-pubsub-project.Data_Warehouse_Easyecom_Staging.Kits`) then product_id end ) as Source_pk_count
  from `shopify-pubsub-project.easycom.kits`


  union all

  select 
  'shipping_status_metrics' as Source_table_name,
  max(date(analysis_date)) as Source_max_date,
  count(distinct case when date(analysis_date) = (select max(date(analysis_date)) from `shopify-pubsub-project.Data_Warehouse_Easyecom_Staging.shipping_status_metrics`) then concat(analysis_date, status) end ) as Source_pk_count
  from `shopify-pubsub-project.easycom.shipping_status_metrics`


  union all

  select 
  'reports' as Source_table_name,
  max(date(created_on)) as Source_max_date,
  count(distinct case when date(created_on) = (select max(date(created_on)) from `shopify-pubsub-project.Data_Warehouse_Easyecom_Staging.Reports`) then report_id end ) as Source_pk_count
  from `shopify-pubsub-project.easycom.reports`

  union all

  select 
  'pending_return_orders' as Source_table_name,
  max(date(import_date)) as Source_max_date,
  count(distinct case when date(import_date) = (select max(date(import_date)) from `shopify-pubsub-project.Data_Warehouse_Easyecom_Staging.pending_return_orders`) then invoice_id end ) as Source_pk_count
  from `shopify-pubsub-project.easycom.pending_return_orders`


  union all

  select 
  'vendors' as Source_table_name,
  max(date(ee_extracted_at)) as Source_max_date,
  count(distinct case when date(ee_extracted_at) = (select max(date(ee_extracted_at)) from `shopify-pubsub-project.Data_Warehouse_Easyecom_Staging.Vendors`) then vendor_c_id end ) as Source_pk_count
  from `shopify-pubsub-project.easycom.vendors`


  union all

  select 
  'inventory_view_by_bin_report' as Source_table_name,
  max(date(created_on)) as Source_max_date,
  count(distinct case when date(created_on) = (select max(date(created_on)) from `shopify-pubsub-project.Data_Warehouse_Easyecom_Staging.inventory_view_by_bin_report`) then SKU end ) as Source_pk_count
  from `shopify-pubsub-project.easycom.inventory_view_by_bin_report`


  union all

  select 
  'inventory_details' as Source_table_name,
  max(date(creation_date)) as Source_max_date,
  count(distinct case when date(creation_date) = (select max(date(creation_date)) from `shopify-pubsub-project.Data_Warehouse_Easyecom_Staging.Inventory_details`) then SKU end ) as Source_pk_count
  from `shopify-pubsub-project.easycom.inventory_details`


  union all

  select 
  'daily_metrics' as Source_table_name,
  max(date(analysis_date)) as Source_max_date,
  count(distinct case when date(analysis_date) = (select max(date(analysis_date)) from `shopify-pubsub-project.Data_Warehouse_Easyecom_Staging.daily_metrics`) then analysis_date end ) as Source_pk_count
  from `shopify-pubsub-project.easycom.daily_metrics`


  union all

  select 
  'customers' as Source_table_name,
  max(date(ee_extracted_at)) as Source_max_date,
  count(distinct case when date(ee_extracted_at) = (select max(date(ee_extracted_at)) from `shopify-pubsub-project.Data_Warehouse_Easyecom_Staging.Customers`) then c_id end ) as Source_pk_count
  from `shopify-pubsub-project.easycom.customers`


  union all

  select 
  'grn_details' as Source_table_name,
  max(date(grn_invoice_date)) as Source_max_date,
  count(distinct case when date(grn_invoice_date) = (select max(date(grn_invoice_date)) from `shopify-pubsub-project.Data_Warehouse_Easyecom_Staging.grn_details`) then grn_id end ) as Source_pk_count
  from `shopify-pubsub-project.easycom.grn_details`


  union all

  select 
  'marketplace_listings' as Source_table_name,
  max(date(ee_extracted_at)) as Source_max_date,
  count(distinct case when date(ee_extracted_at) = (select max(date(ee_extracted_at)) from `shopify-pubsub-project.Data_Warehouse_Easyecom_Staging.Marketplace_listings`) then concat(sku,site_uid) end ) as Source_pk_count
  from `shopify-pubsub-project.easycom.marketplace_listings`


  union all

  select 
  'grn_details_report' as Source_table_name,
  max(date(created_on)) as Source_max_date,
  count(distinct case when date(created_on) = (select max(date(created_on)) from `shopify-pubsub-project.Data_Warehouse_Easyecom_Staging.grn_details_report`) then concat(poNo, companyProductId, receivedQty) end ) as Source_pk_count
  from `shopify-pubsub-project.easycom.grn_details_report`
),

---------------------------------------------------------------------------------------------------------------------------------------------------------
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

  union all

  select 
  'master_products' as Dest_table_name,
  max(created_at) as Dest_max_date,
  count(distinct case when date(created_at) = (select max(date(created_at)) from `shopify-pubsub-project.easycom.master_products`) then cp_id end ) as Dest_pk_count
  from  `shopify-pubsub-project.Data_Warehouse_Easyecom_Staging.Master_products`

  union all

  select 
  'locations' as Dest_table_name,
  max(ee_extracted_at) as Dest_max_date,
  count(distinct case when date(ee_extracted_at) = (select max(date(ee_extracted_at)) from `shopify-pubsub-project.easycom.locations`) then location_key end ) as Dest_pk_count
  from  `shopify-pubsub-project.Data_Warehouse_Easyecom_Staging.Locations`

  union all

  select 
  'inventory_snapshot' as Dest_table_name,
  max(entry_date) as Dest_max_date,
  count(distinct case when date(entry_date) = (select max(date(entry_date)) from `shopify-pubsub-project.easycom.inventory_snapshot`) then id end ) as Dest_pk_count
  from  `shopify-pubsub-project.Data_Warehouse_Easyecom_Staging.Inventory_Snapshot`

  union all

  select 
  'inventory_aging_report' as Dest_table_name,
  max(date(end_date)) as Dest_max_date,
  count(distinct case when date(end_date) = (select max(date(end_date)) from `shopify-pubsub-project.easycom.inventory_aging_report`) then concat(sku,location) end ) as Dest_pk_count
  from  `shopify-pubsub-project.Data_Warehouse_Easyecom_Staging.Inventory_Aging_report`

  union all

  select 
  'countries' as Dest_table_name,
  max(date(ee_extracted_at)) as Dest_max_date,
  count(distinct case when date(ee_extracted_at) = (select max(date(ee_extracted_at)) from `shopify-pubsub-project.easycom.countries`) then country_id end ) as Dest_pk_count
  from  `shopify-pubsub-project.Data_Warehouse_Easyecom_Staging.Countries`

  union all

  select 
  'marketplace' as Dest_table_name,
  max(date(ee_extracted_at)) as Dest_max_date,
  count(distinct case when date(ee_extracted_at) = (select max(date(ee_extracted_at)) from `shopify-pubsub-project.easycom.marketplace`) then marketplace_id end ) as Dest_pk_count
  from  `shopify-pubsub-project.Data_Warehouse_Easyecom_Staging.Marketplace`

  union all

  select 
  'mini_sales_report' as Dest_table_name,
  max(date(start_date)) as Dest_max_date,
  count(distinct case when date(start_date) = (select max(date(start_date)) from `shopify-pubsub-project.easycom.mini_sales_report`) then concat(Order_Number, Suborder_No, report_id) end ) as Dest_pk_count
  from  `shopify-pubsub-project.Data_Warehouse_Easyecom_Staging.Mini_Sales_report`

  union all

  select 
  'all_return_orders' as Dest_table_name,
  max(date(order_date)) as Dest_max_date,
  count(distinct case when date(order_date) = (select max(order_date) from `shopify-pubsub-project.easycom.all_return_orders`) then order_id end ) as Dest_pk_count
  from  `shopify-pubsub-project.Data_Warehouse_Easyecom_Staging.All_Return_Orders`

  union all

  select 
  'All_Return_Order_items' as Dest_table_name,
  max(order_date) as Dest_max_date,
 count(distinct case when date(order_date) = (select max(order_date) from `shopify-pubsub-project.Data_Warehouse_Easyecom_Staging.All_Return_Order_items`) then order_id end ) as Dest_pk_count
  from `shopify-pubsub-project.Data_Warehouse_Easyecom_Staging.All_Return_Order_items`

  union all

  select 
  'status_wise_stock_report' as Dest_table_name,
  max(date(end_date)) as Dest_max_date,
  count(distinct case when date(end_date) = (select max(date(end_date)) from `shopify-pubsub-project.easycom.status_wise_stock_report`) then concat(SKU, Company_Token) end ) as Dest_pk_count
  from  `shopify-pubsub-project.Data_Warehouse_Easyecom_Staging.Status_Wise_Stock_report`

  union all

  select 
  'states' as Dest_table_name,
  max(date(ee_extracted_at)) as Dest_max_date,
  count(distinct case when date(ee_extracted_at) = (select max(date(ee_extracted_at)) from `shopify-pubsub-project.easycom.states`) then state_id end ) as Dest_pk_count
  from  `shopify-pubsub-project.Data_Warehouse_Easyecom_Staging.States`  

  union all

  select 
  'order_status_metrics' as Dest_table_name,
  max(date(date)) as Dest_max_date,
  count(distinct case when date(date) = (select max(date(date)) from `shopify-pubsub-project.easycom.order_status_metrics`) then concat(date,status) end ) as Dest_pk_count
  from  `shopify-pubsub-project.Data_Warehouse_Easyecom_Staging.order_status_metrics` 

  union all

  select 
  'pending_returns_report' as Dest_table_name,
  max(date(end_date)) as Dest_max_date,
  count(distinct case when date(end_date) = (select max(date(end_date)) from `shopify-pubsub-project.easycom.pending_returns_report`) then concat(Order_Number, Order_Item_ID) end ) as Dest_pk_count
  from  `shopify-pubsub-project.Data_Warehouse_Easyecom_Staging.Pending_Returns_report`  

  union all

  select 
  'purchase_orders' as Dest_table_name,
  max(date(ee_extracted_at)) as Dest_max_date,
  count(distinct case when date(ee_extracted_at) = (select max(date(ee_extracted_at)) from `shopify-pubsub-project.easycom.purchase_orders`) then concat(po_id,ee_extracted_at) end ) as Dest_pk_count
  from  `shopify-pubsub-project.Data_Warehouse_Easyecom_Staging.Purchase_Orders` 


  union all

  select 
  'warehouse_metrics' as Dest_table_name,
  max(date(analysis_date)) as Dest_max_date,
  count(distinct case when date(analysis_date) = (select max(date(analysis_date)) from `shopify-pubsub-project.easycom.warehouse_metrics`) then concat(location_key, warehouse_name) end ) as Dest_pk_count
  from  `shopify-pubsub-project.Data_Warehouse_Easyecom_Staging.warehouse_metrics`


  union all

  select 
  'kits' as Dest_table_name,
  max(date(add_date)) as Dest_max_date,
  count(distinct case when date(add_date) = (select max(date(add_date)) from `shopify-pubsub-project.easycom.kits`) then product_id end ) as Dest_pk_count
  from  `shopify-pubsub-project.Data_Warehouse_Easyecom_Staging.Kits` 


  union all

  select 
  'shipping_status_metrics' as Dest_table_name,
  max(date(analysis_date)) as Dest_max_date,
  count(distinct case when date(analysis_date) = (select max(date(analysis_date)) from `shopify-pubsub-project.easycom.shipping_status_metrics`) then concat(analysis_date, status) end ) as Dest_pk_count
  from  `shopify-pubsub-project.Data_Warehouse_Easyecom_Staging.shipping_status_metrics`


  union all

  select 
  'reports' as Dest_table_name,
  max(date(created_on)) as Dest_max_date,
  count(distinct case when date(created_on) = (select max(date(created_on)) from `shopify-pubsub-project.easycom.reports`) then report_id end ) as Dest_pk_count
  from  `shopify-pubsub-project.Data_Warehouse_Easyecom_Staging.Reports` 


  union all

  select 
  'pending_return_orders' as Dest_table_name,
  max(date(import_date)) as import_date,
  count(distinct case when date(import_date) = (select max(date(import_date)) from `shopify-pubsub-project.easycom.pending_return_orders`) then invoice_id end ) as Dest_pk_count
  from  `shopify-pubsub-project.Data_Warehouse_Easyecom_Staging.pending_return_orders` 


  union all

  select 
  'vendors' as Dest_table_name,
  max(date(ee_extracted_at)) as import_date,
  count(distinct case when date(ee_extracted_at) = (select max(date(ee_extracted_at)) from `shopify-pubsub-project.easycom.vendors`) then vendor_c_id end ) as Dest_pk_count
  from  `shopify-pubsub-project.Data_Warehouse_Easyecom_Staging.Vendors` 


  union all

  select 
  'inventory_view_by_bin_report' as Dest_table_name,
  max(date(created_on)) as import_date,
  count(distinct case when date(created_on) = (select max(date(created_on)) from `shopify-pubsub-project.easycom.inventory_view_by_bin_report`) then SKU end ) as Dest_pk_count
  from  `shopify-pubsub-project.Data_Warehouse_Easyecom_Staging.inventory_view_by_bin_report` 


  union all

  select 
  'inventory_details' as Dest_table_name,
  max(date(creation_date)) as import_date,
  count(distinct case when date(creation_date) = (select max(date(creation_date)) from `shopify-pubsub-project.easycom.inventory_details`) then concat(product_id,company_product_id) end ) as Dest_pk_count
  from  `shopify-pubsub-project.Data_Warehouse_Easyecom_Staging.Inventory_details`


  union all

  select 
  'daily_metrics' as Dest_table_name,
  max(date(analysis_date)) as import_date,
  count(distinct case when date(analysis_date) = (select max(date(analysis_date)) from `shopify-pubsub-project.easycom.daily_metrics`) then analysis_date end ) as Dest_pk_count
  from  `shopify-pubsub-project.Data_Warehouse_Easyecom_Staging.daily_metrics`


  union all

  select 
  'customers' as Dest_table_name,
  max(date(ee_extracted_at)) as import_date,
  count(distinct case when date(ee_extracted_at) = (select max(date(ee_extracted_at)) from `shopify-pubsub-project.easycom.customers`) then c_id end ) as Dest_pk_count
  from  `shopify-pubsub-project.Data_Warehouse_Easyecom_Staging.Customers`


  union all

  select 
  'grn_details' as Dest_table_name,
  max(date(grn_created_at)) as import_date,
  count(distinct case when date(grn_created_at) = (select max(date(grn_created_at)) from `shopify-pubsub-project.easycom.grn_details`) then grn_id end ) as Dest_pk_count
  from  `shopify-pubsub-project.Data_Warehouse_Easyecom_Staging.grn_details`


  union all

  select 
  'marketplace_listings' as Dest_table_name,
  max(date(ee_extracted_at)) as import_date,
  count(distinct case when date(ee_extracted_at) = (select max(date(ee_extracted_at)) from `shopify-pubsub-project.easycom.marketplace_listings`) then concat(sku,site_uid) end ) as Dest_pk_count
  from  `shopify-pubsub-project.Data_Warehouse_Easyecom_Staging.Marketplace_listings`


  union all

  select 
  'grn_details_report' as Dest_table_name,
  max(date(created_on)) as import_date,
  count(distinct case when date(created_on) = (select max(date(created_on)) from `shopify-pubsub-project.easycom.grn_details_report`) then concat(poNo, companyProductId, receivedQty) end ) as Dest_pk_count
  from  `shopify-pubsub-project.Data_Warehouse_Easyecom_Staging.grn_details_report`
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