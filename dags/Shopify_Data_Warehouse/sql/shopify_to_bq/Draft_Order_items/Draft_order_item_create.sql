CREATE or replace TABLE `shopify-pubsub-project.Data_Warehouse_Shopify_Staging.Draft_Order_items`
PARTITION BY DATE_TRUNC(draft_order_created_at,day)
CLUSTER BY draft_order_id
OPTIONS(
 description = "Draft Order items table is partitioned on draft order created at",
 require_partition_filter = False
 )
 AS 
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
