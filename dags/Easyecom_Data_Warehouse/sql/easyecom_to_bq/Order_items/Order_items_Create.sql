
CREATE  OR REPLACE TABLE`shopify-pubsub-project.Data_Warehouse_Easyecom_Staging.Orderitem`
PARTITION BY DATE_TRUNC(order_date,day)
CLUSTER BY order_status
OPTIONS(
 description = "Orderitem table is partitioned on Order date at day level and clustered by order status",
 require_partition_filter = FALSE
 )
 AS 

SELECT
distinct
  SAFE_CAST(order_id as STRING) as order_id,
  SAFE_CAST(invoice_id as STRING) as invoice_id,
  order_date,
  order_status,
  SAFE_CAST(JSON_EXTRACT_SCALAR(suborder_flat,'$[suborder_id]') as STRING) AS suborder_id,
  JSON_EXTRACT_SCALAR(suborder_flat,'$[suborder_num]') AS suborder_num,
  JSON_EXTRACT_SCALAR(suborder_flat,'$[reference_code]') AS reference_code,
  JSON_EXTRACT_SCALAR(suborder_flat,'$[item_status]') AS item_status,
  JSON_EXTRACT_SCALAR(suborder_flat,'$[shipment_type]') AS shipment_type,
  SAFE_CAST(JSON_EXTRACT_SCALAR(suborder_flat,'$[suborder_quantity]') as FLOAT64) AS suborder_quantity,
  SAFE_CAST(JSON_EXTRACT_SCALAR(suborder_flat,'$[item_quantity]') as FLOAT64) AS item_quantity,
  SAFE_CAST(JSON_EXTRACT_SCALAR(suborder_flat,'$[returned_quantity]') as FLOAT64) AS returned_quantity,
  SAFE_CAST(JSON_EXTRACT_SCALAR(suborder_flat,'$[cancelled_quantity]') as FLOAT64) AS cancelled_quantity,
  SAFE_CAST(JSON_EXTRACT_SCALAR(suborder_flat,'$[shipped_quantity]') as FLOAT64) AS shipped_quantity,
  JSON_EXTRACT_SCALAR(suborder_flat,'$[batch_codes]') AS batch_codes,
  JSON_EXTRACT_SCALAR(suborder_flat,'$[serial_nums]') AS serial_nums,
  JSON_EXTRACT_SCALAR(suborder_flat,'$[batchcode_serial]') AS batchcode_serial,
  JSON_EXTRACT_SCALAR(suborder_flat,'$[batchcode_expiry]') AS batchcode_expiry,
  JSON_EXTRACT_SCALAR(suborder_flat,'$[tax_type]') AS tax_type,
  
  SAFE_CAST(JSON_EXTRACT_SCALAR(suborder_flat,'$[suborder_history].qc_pass_datetime') as datetime) AS qc_pass_datetime,
  SAFE_CAST(JSON_EXTRACT_SCALAR(suborder_flat,'$[suborder_history].confirm_datetime') as datetime) AS confirm_datetime,
  SAFE_CAST(JSON_EXTRACT_SCALAR(suborder_flat,'$[suborder_history].print_datetime') as datetime) AS print_datetime,
  SAFE_CAST(JSON_EXTRACT_SCALAR(suborder_flat,'$[suborder_history].manifest_datetime') as datetime) AS manifest_datetime,

  JSON_EXTRACT_SCALAR(suborder_flat,'$[meta]') AS meta,
  SAFE_CAST(JSON_EXTRACT_SCALAR(suborder_flat,'$[selling_price]') as FLOAT64) AS selling_price,
  SAFE_CAST(JSON_EXTRACT_SCALAR(suborder_flat,'$[total_shipping_charge]') as FLOAT64) AS total_shipping_charge,
  SAFE_CAST(JSON_EXTRACT_SCALAR(suborder_flat,'$[total_miscellaneous]') as FLOAT64) AS total_miscellaneous,
  SAFE_CAST(JSON_EXTRACT_SCALAR(suborder_flat,'$[tax_rate]') as FLOAT64) AS tax_rate,
  SAFE_CAST(JSON_EXTRACT_SCALAR(suborder_flat,'$[tax]') as FLOAT64) AS tax,
  JSON_EXTRACT_SCALAR(suborder_flat,'$[product_id]') AS product_id,
  JSON_EXTRACT_SCALAR(suborder_flat,'$[company_product_id]') AS company_product_id,
  JSON_EXTRACT_SCALAR(suborder_flat,'$[sku]') AS sku,
  JSON_EXTRACT_SCALAR(suborder_flat,'$[sku_type]') AS sku_type,
  SAFE_CAST(JSON_EXTRACT_SCALAR(suborder_flat,'$[sub_product_count]') as FLOAT64) AS sub_product_count,
  JSON_EXTRACT_SCALAR(suborder_flat,'$[marketplace_sku]') AS marketplace_sku,
  JSON_EXTRACT_SCALAR(suborder_flat,'$[productName]') AS productName,
  JSON_EXTRACT_SCALAR(suborder_flat,'$[Identifier]') AS Identifier,
  JSON_EXTRACT_SCALAR(suborder_flat,'$[description]') AS description,
  JSON_EXTRACT_SCALAR(suborder_flat,'$[category]') AS category,
  JSON_EXTRACT_SCALAR(suborder_flat,'$[brand]') AS brand,
  JSON_EXTRACT_SCALAR(suborder_flat,'$[model_no]') AS model_no,
  JSON_EXTRACT_SCALAR(suborder_flat,'$[product_tax_code]') AS product_tax_code,
  JSON_EXTRACT_SCALAR(suborder_flat,'$[AccountingSku]') AS AccountingSku,
  JSON_EXTRACT_SCALAR(suborder_flat,'$[accounting_unit]') AS accounting_unit,
  JSON_EXTRACT_SCALAR(suborder_flat,'$[ean]') AS ean,
  JSON_EXTRACT_SCALAR(suborder_flat,'$[size]') AS size,
  SAFE_CAST(JSON_EXTRACT_SCALAR(suborder_flat,'$[cost]') as FLOAT64) AS cost,
  SAFE_CAST(JSON_EXTRACT_SCALAR(suborder_flat,'$[mrp]') as FLOAT64) AS mrp,
  SAFE_CAST(JSON_EXTRACT_SCALAR(suborder_flat,'$[weight]') as FLOAT64) AS weight,
  SAFE_CAST(JSON_EXTRACT_SCALAR(suborder_flat,'$[length]') as FLOAT64) AS length,
  SAFE_CAST(JSON_EXTRACT_SCALAR(suborder_flat,'$[width]') as FLOAT64) AS width,
  SAFE_CAST(JSON_EXTRACT_SCALAR(suborder_flat,'$[height]') as FLOAT64) AS height,
  SAFE_CAST(JSON_EXTRACT_SCALAR(suborder_flat,'$[scheme_applied]') as FLOAT64) AS scheme_applied,
  ee_extracted_at,

  FROM
    (
      SELECT
        order_id,
        order_date,
        order_status,
        invoice_id,
        last_update_date,
        ee_extracted_at,
        row_number() over(partition by invoice_id order by last_update_date desc,ee_extracted_at desc) as ranking,
        JSON_QUERY_ARRAY(suborders,'$[0]') AS suborder_array,
      FROM
        `shopify-pubsub-project.easycom.orders`),UNNEST(suborder_array) AS suborder_flat
        where ranking=1