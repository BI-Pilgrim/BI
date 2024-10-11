with cte as
(SELECT
    'Abandoned_checkout' AS table_name,
    max(_airbyte_extracted_at) AS max_airbyte_extracted_at,
    max(aband_created_at) as max_created_at

  FROM
    `shopify-pubsub-project.Shopify_staging.Abandoned_checkout`

UNION ALL

SELECT
    'Abandoned_checkout_lineitem' AS table_name,
    max(_airbyte_extracted_at) AS max_airbyte_extracted_at,
    max(aband_created_at) as max_aband_created_at

  FROM
    `shopify-pubsub-project.Shopify_staging.Abandoned_checkout_lineitem`

UNION ALL

SELECT
    'Customers' AS table_name,
    max(_airbyte_extracted_at) AS max_airbyte_extracted_at,
    max(Customer_created_at) AS Customer_created_at

  FROM
    `shopify-pubsub-project.Shopify_staging.Customers`

UNION ALL

SELECT
    'Discount_Code' AS table_name,
    max(_airbyte_extracted_at) AS max_airbyte_extracted_at,
    max(discount_created_at) AS discount_created_at
  FROM
    `shopify-pubsub-project.Shopify_staging.Discount_Code`

UNION ALL

SELECT
    'Draft_Order_items' AS table_name,
    max(_airbyte_extracted_at) AS max_airbyte_extracted_at,
    max(draft_order_created_at) AS draft_order_created_at
  FROM
    `shopify-pubsub-project.Shopify_staging.Draft_Order_items`

UNION ALL

SELECT
    'Draft_Orders' AS table_name,
    max(_airbyte_extracted_at) AS max_airbyte_extracted_at,
    max(draft_order_created_at) AS draft_order_created_at

  FROM
    `shopify-pubsub-project.Shopify_staging.Draft_Orders`

UNION ALL

SELECT
    'Fulfillments' AS table_name,
    max(_airbyte_extracted_at) AS max_airbyte_extracted_at,
    max(fulfillment_created_at) AS fulfillment_created_at
  FROM
    `shopify-pubsub-project.Shopify_staging.Fulfillments`

UNION ALL

SELECT
    'Order_items' AS table_name,
    max(_airbyte_extracted_at) AS max_airbyte_extracted_at,
    max(Order_created_at) AS Order_created_at
  FROM
    `shopify-pubsub-project.Shopify_staging.Order_items`

UNION ALL

SELECT
    'Orders' AS table_name,
    max(_airbyte_extracted_at) AS max_airbyte_extracted_at,
    max(Order_created_at) AS Order_created_at,
  FROM
    `shopify-pubsub-project.Shopify_staging.Orders`

UNION ALL

SELECT
    'Refund_Order_items' AS table_name,
    max(_airbyte_extracted_at) AS max_airbyte_extracted_at,
    max(Refund_created_at) As Refund_created_at
  FROM
    `shopify-pubsub-project.Shopify_staging.Refund_Order_items`)

    select 
    table_name,
    max_airbyte_extracted_at,
    max_created_at,
    DATE_DIFF(CURRENT_DATE(), date(max_airbyte_extracted_at), DAY) AS Shopify_sync_delayed,
    DATE_DIFF(CURRENT_DATE(), date(max_created_at), DAY) AS Gap_latest_date,
    from cte

