CREATE OR REPLACE TABLE `shopify-pubsub-project.Data_Warehouse_judgeme_staging.reviews` as
select
    id,
    title,
    rating,
    product_external_id,
    product_title,
    product_handle,
    pictures,
    JSON_VALUE(reviewer, '$.email') AS reviewer_email,
    JSON_VALUE(reviewer, '$.id') AS reviewer_id,
    JSON_VALUE(reviewer, '$.name') AS reviewer_name,
    JSON_VALUE(reviewer, '$.phone') AS reviewer_phone,
    JSON_VALUE(reviewer, '$.external_id') AS reviewer_external_id,
    
    -- Convert reviewer_tags JSON array to a comma-separated string
    ARRAY_TO_STRING(JSON_VALUE_ARRAY(reviewer, '$.tags'), ', ') AS reviewer_tags_str,

    -- Extract Order Type
    ARRAY_TO_STRING(
        ARRAY(
            SELECT tag 
            FROM UNNEST(JSON_VALUE_ARRAY(reviewer, '$.tags')) AS tag
            WHERE tag LIKE '%COD Customer%' OR tag LIKE '%Prepaid Customer%'
        ), ', '
    ) AS order_type,

    -- Extract Delivery Status
    ARRAY_TO_STRING(
        ARRAY(
            SELECT tag 
            FROM UNNEST(JSON_VALUE_ARRAY(reviewer, '$.tags')) AS tag
            WHERE tag LIKE '%Successful Delivery%' OR tag LIKE '%Failed Delivery%'
        ), ', '
    ) AS delivery_status,

    source,
    curated,
    hidden,
    verified,
    created_at,
    updated_at,
    ip_address,
    has_published_pictures,
    has_published_videos,
    ee_extracted_at

FROM `shopify-pubsub-project.pilgrim_bi_judgeme.reviews`;


