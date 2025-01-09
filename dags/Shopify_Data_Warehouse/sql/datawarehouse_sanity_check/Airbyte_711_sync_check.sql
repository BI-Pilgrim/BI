with cte as (
SELECT
    'orders' AS table_name,
    MAX(orders._airbyte_emitted_at) AS max_airbyte_emitted_at,
    MAX(orders.created_at) AS max_created_at
  FROM
    `shopify-pubsub-project.airbyte711.orders` AS orders

UNION ALL

SELECT
    'customers' AS table_name,
    MAX(customers._airbyte_emitted_at) AS max_airbyte_emitted_at,
    MAX(customers.created_at) AS max_created_at
  FROM
    `shopify-pubsub-project.airbyte711.customers` AS customers

UNION ALL

SELECT
    'inventory_levels' AS table_name,
    MAX(inventory_levels._airbyte_emitted_at) AS max_airbyte_emitted_at,
    MAX(inventory_levels.updated_at) AS max_created_at
  FROM
    `shopify-pubsub-project.airbyte711.inventory_levels` AS inventory_levels

UNION ALL

SELECT
    'draft_orders' AS table_name,
    MAX(draft_orders._airbyte_emitted_at) AS max_airbyte_emitted_at,
    MAX(draft_orders.created_at) AS max_created_at
  FROM
    `shopify-pubsub-project.airbyte711.draft_orders` AS draft_orders

UNION ALL

SELECT
    'abandoned_checkouts' AS table_name,
    MAX(abandoned_checkouts._airbyte_emitted_at) AS max_airbyte_emitted_at,
    MAX(abandoned_checkouts.created_at) AS max_created_at
  FROM
    `shopify-pubsub-project.airbyte711.abandoned_checkouts` AS abandoned_checkouts

UNION ALL

SELECT
    'fulfillments' AS table_name,
    MAX(fulfillments._airbyte_emitted_at) AS max_airbyte_emitted_at,
    MAX(fulfillments.created_at) AS max_created_at
  FROM
    `shopify-pubsub-project.airbyte711.fulfillments` AS fulfillments

UNION ALL

SELECT
    'transactions' AS table_name,
    MAX(transactions._airbyte_emitted_at) AS max_airbyte_emitted_at,
    MAX(transactions.created_at) AS max_created_at
  FROM
    `shopify-pubsub-project.airbyte711.transactions` AS transactions

UNION ALL

SELECT
    'metafield_orders' AS table_name,
    MAX(metafield_orders._airbyte_emitted_at) AS max_airbyte_emitted_at,
    MAX(metafield_orders.created_at) AS max_created_at
  FROM
    `shopify-pubsub-project.airbyte711.metafield_orders` AS metafield_orders

UNION ALL

SELECT
    'countries' AS table_name,
    MAX(countries._airbyte_emitted_at) AS max_airbyte_emitted_at,
    NULL AS max_created_at
  FROM
    `shopify-pubsub-project.airbyte711.countries` AS countries
UNION ALL

SELECT
    'products' AS table_name,
    MAX(products._airbyte_emitted_at) AS max_airbyte_emitted_at,
    MAX(products.created_at) AS max_created_at
  FROM
    `shopify-pubsub-project.airbyte711.customers` AS products    
   )

select 
table_name,
max_airbyte_emitted_at,
max_created_at,
DATE_DIFF(CURRENT_DATE(), date(max_airbyte_emitted_at), DAY) AS Airbyte_sync_delayed,
DATE_DIFF(CURRENT_DATE(), date(max_created_at), DAY) AS Gap_latest_date,
from cte
