
MERGE INTO `shopify-pubsub-project.Data_Warehouse_Shopify_Staging.Draft_Order_items` AS target

USING (
  SELECT 
 distinct
 *
 from  (select 
    _airbyte_extracted_at,
    draft_order_status,
    draft_order_payment_terms,
    draft_order_note,
    draft_order_email,
    draft_order_completed_at,
    draft_order_id,
    draft_order_created_at,
    draft_order_updated_at,
    draft_id,
    draft_order_name,
    draft_order_invoice_url,
    customer_id,
    discount_value_type,
    item_discount_created_at,
    item_discount_id,
    product_id,
    item_SKU_code,
    tax_line_title,
    product_title,
    item_variant_id,
    item_variant_title,

    max(draft_order_total_tax) as draft_order_total_tax,
    max(draft_order_total_price) as draft_order_total_price,
    max(discount_amount) as discount_amount,
    max(discount_value) as discount_value,
    max(item_discount_amount) as item_discount_amount,
    max(item_line_price) as item_line_price,
    max(item_price) as item_price,
    sum(item_quantity) as item_quantity,
    sum(tax_line_price) as tax_line_price,
    max(tax_line_rate) as tax_line_rate,
    max(item_variant_price) as item_variant_price,

 from 
    (select 
        distinct
        _airbyte_extracted_at,
        draft_order_status,
        draft_order_payment_terms,
        draft_order_total_tax,
        draft_order_total_price,
        draft_order_note,
        draft_order_email,
        draft_order_completed_at,
        draft_order_id,
        draft_order_created_at,
        draft_order_updated_at,
        draft_id,
        draft_order_name,
        draft_order_invoice_url,
        customer_id,
        discount_amount,
        discount_value,
        discount_value_type,

        CAST(JSON_EXTRACT_SCALAR(Full_FLAT,'$.applied_discounts[0].amount') AS FLOAT64) as item_discount_amount,
        CAST(JSON_EXTRACT_SCALAR(Full_FLAT,'$.applied_discounts[0].created_at') AS TIMESTAMP) as item_discount_created_at,
        CAST(JSON_EXTRACT_SCALAR(Full_FLAT,'$.applied_discounts[0].id') AS STRING) as item_discount_id,
        CAST(JSON_EXTRACT_SCALAR(Full_FLAT,'$.line_price') AS FLOAT64) as item_line_price,
        CAST(JSON_EXTRACT_SCALAR(Full_FLAT,'$.price') AS FLOAT64) as item_price,
        JSON_EXTRACT_SCALAR(Full_FLAT,'$.product_id') as product_id,
        JSON_EXTRACT_SCALAR(Full_FLAT,'$.sku') as item_SKU_code,
        CAST(JSON_EXTRACT_SCALAR(Full_FLAT,'$.quantity') AS FLOAT64) as item_quantity,
        CAST(JSON_EXTRACT_SCALAR(Full_FLAT,'$.tax_lines[0].price') AS FLOAT64) as tax_line_price,
        CAST(JSON_EXTRACT_SCALAR(Full_FLAT,'$.tax_lines[0].rate') AS FLOAT64 )as tax_line_rate,
        JSON_EXTRACT_SCALAR(Full_FLAT,'$.tax_lines[0].title') as tax_line_title,
        JSON_EXTRACT_SCALAR(Full_FLAT,'$.title') as product_title,
        CAST(JSON_EXTRACT_SCALAR(Full_FLAT,'$.variant_id') AS STRING) as item_variant_id,
        CAST(JSON_EXTRACT_SCALAR(Full_FLAT,'$.variant_price') AS FLOAT64) as item_variant_price,
        CAST(JSON_EXTRACT_SCALAR(Full_FLAT,'$.variant_title') AS STRING) as item_variant_title,

        from 
            (
                select
                
                _airbyte_extracted_at,
                draft_order_status,
                draft_order_payment_terms,
                draft_order_total_tax,
                draft_order_total_price,
                draft_order_note,
                draft_order_email,
                draft_order_completed_at,
                draft_order_id,
                draft_order_created_at,
                draft_order_updated_at,
                draft_id,
                draft_order_name,
                draft_order_invoice_url,
                customer_id,
                discount_amount,
                discount_value,
                discount_value_type,
                FULL_FLAT,
                FROM (
                    SELECT 
                    
                    _airbyte_extracted_at,
                    status as draft_order_status,
                    payment_terms as draft_order_payment_terms,
                    CAST(total_tax AS FLOAT64) as draft_order_total_tax,
                    CAST(total_price AS FLOAT64) as draft_order_total_price,
                    note as draft_order_note,
                    email as draft_order_email,
                    completed_at as draft_order_completed_at,
                    CAST(order_id AS STRING) as draft_order_id,
                    created_at as draft_order_created_at,
                    updated_at as draft_order_updated_at,
                    CAST(id AS STRING) as draft_id,
                    CAST(name AS STRING) as draft_order_name,
                    invoice_url as draft_order_invoice_url,
                    CAST(JSON_EXTRACT_SCALAR(customer, '$.id') AS STRING) AS customer_id,
                    CAST(JSON_EXTRACT_SCALAR(applied_discount, '$.amount') AS FLOAT64) AS discount_amount,
                    CAST(JSON_EXTRACT_SCALAR(applied_discount, '$.value') AS FLOAT64) AS discount_value,
                    CAST(JSON_EXTRACT_SCALAR(applied_discount, '$.value_type') AS STRING) AS discount_value_type,
                    JSON_EXTRACT_ARRAY(line_items) as line_item_flat,

                    FROM   `shopify-pubsub-project.pilgrim_bi_airbyte.draft_orders`

                ), UNNEST (line_item_flat) AS FULL_FLAT
            )
         
    )
group by All)

  WHERE date(_airbyte_extracted_at) >= DATE_SUB(CURRENT_DATE("Asia/Kolkata"), INTERVAL 10 DAY)
 
 ) AS source
ON target.draft_order_id = source.draft_order_id 
and target.item_variant_id = source.item_variant_id

WHEN MATCHED AND source._airbyte_extracted_at > target._airbyte_extracted_at THEN UPDATE SET

target._airbyte_extracted_at = source._airbyte_extracted_at,
target.draft_order_status = source.draft_order_status,
target.draft_order_payment_terms = source.draft_order_payment_terms,
target.draft_order_note = source.draft_order_note,
target.draft_order_email = source.draft_order_email,
target.draft_order_completed_at = source.draft_order_completed_at,
target.draft_order_id = source.draft_order_id,
target.draft_order_created_at = source.draft_order_created_at,
target.draft_order_updated_at = source.draft_order_updated_at,
target.draft_id = source.draft_id,
target.draft_order_name = source.draft_order_name,
target.draft_order_invoice_url = source.draft_order_invoice_url,
target.customer_id = source.customer_id,
target.discount_value_type = source.discount_value_type,
target.item_discount_created_at = source.item_discount_created_at,
target.item_discount_id = source.item_discount_id,
target.product_id = source.product_id,
target.item_SKU_code = source.item_SKU_code,
target.tax_line_title = source.tax_line_title,
target.product_title = source.product_title,
target.item_variant_id = source.item_variant_id,
target.item_variant_title = source.item_variant_title,
target.draft_order_total_tax = source.draft_order_total_tax,
target.draft_order_total_price = source.draft_order_total_price,
target.discount_amount = source.discount_amount,
target.discount_value = source.discount_value,
target.item_discount_amount = source.item_discount_amount,
target.item_line_price = source.item_line_price,
target.item_price = source.item_price,
target.item_quantity = source.item_quantity,
target.tax_line_price = source.tax_line_price,
target.tax_line_rate = source.tax_line_rate,
target.item_variant_price = source.item_variant_price

WHEN NOT MATCHED THEN INSERT (
_airbyte_extracted_at,
draft_order_status,
draft_order_payment_terms,
draft_order_note,
draft_order_email,
draft_order_completed_at,
draft_order_id,
draft_order_created_at,
draft_order_updated_at,
draft_id,
draft_order_name,
draft_order_invoice_url,
customer_id,
discount_value_type,
item_discount_created_at,
item_discount_id,
product_id,
item_SKU_code,
tax_line_title,
product_title,
item_variant_id,
item_variant_title,
draft_order_total_tax,
draft_order_total_price,
discount_amount,
discount_value,
item_discount_amount,
item_line_price,
item_price,
item_quantity,
tax_line_price,
tax_line_rate,
item_variant_price

  )

  VALUES (
source._airbyte_extracted_at,
source.draft_order_status,
source.draft_order_payment_terms,
source.draft_order_note,
source.draft_order_email,
source.draft_order_completed_at,
source.draft_order_id,
source.draft_order_created_at,
source.draft_order_updated_at,
source.draft_id,
source.draft_order_name,
source.draft_order_invoice_url,
source.customer_id,
source.discount_value_type,
source.item_discount_created_at,
source.item_discount_id,
source.product_id,
source.item_SKU_code,
source.tax_line_title,
source.product_title,
source.item_variant_id,
source.item_variant_title,
source.draft_order_total_tax,
source.draft_order_total_price,
source.discount_amount,
source.discount_value,
source.item_discount_amount,
source.item_line_price,
source.item_price,
source.item_quantity,
source.tax_line_price,
source.tax_line_rate,
source.item_variant_price
  )
