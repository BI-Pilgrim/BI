MERGE INTO `shopify-pubsub-project.Data_Warehouse_judgeme_staging.reviews` AS target
USING (
  SELECT
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
    DATE(created_at) AS created_at,
    updated_at,
    ip_address,
    has_published_pictures,
    has_published_videos,
    ee_extracted_at
  FROM `shopify-pubsub-project.pilgrim_bi_judgeme.reviews`
  WHERE date(created_at) > (SELECT MAX(date(created_at)) FROM `shopify-pubsub-project.Data_Warehouse_judgeme_staging.reviews`)
) AS source
ON FALSE  
WHEN NOT MATCHED THEN 
  INSERT (
    id,
    title,
    rating,
    product_external_id,
    product_title,
    product_handle,
    pictures,
    reviewer_email,
    reviewer_id,
    reviewer_name,
    reviewer_phone,
    reviewer_external_id,
    reviewer_tags_str,
    order_type,
    delivery_status,
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
  )
  VALUES (
    source.id,
    source.title,
    source.rating,
    source.product_external_id,
    source.product_title,
    source.product_handle,
   source.pictures,
    source.reviewer_email,
    source.reviewer_id,
    source.reviewer_name,
    source.reviewer_phone,
    source.reviewer_external_id,
    source.reviewer_tags_str,
    source.order_type,
    source.delivery_status,
    source.source,
    source.curated,
    source.hidden,
    source.verified,
    source.created_at,
    source.updated_at,
    source.ip_address,
    source.has_published_pictures,
    source.has_published_videos,
    source.ee_extracted_at
  );
