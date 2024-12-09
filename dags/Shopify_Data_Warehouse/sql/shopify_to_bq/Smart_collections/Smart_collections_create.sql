
CREATE OR REPLACE TABLE `shopify-pubsub-project.Data_Warehouse_Shopify_Staging.Smart_collections`
PARTITION BY DATE_TRUNC(collection_updated_at, DAY)
CLUSTER BY collection_id
OPTIONS(
  description = "Smart Collections table is partitioned on Collection updated at",
  require_partition_filter = FALSE
)
AS
WITH expanded_rules AS (
  SELECT 
    id AS collection_id,
    _airbyte_extracted_at,
    title AS collection_title,
    handle AS collection_handle,
    shop_url,
    body_html,
    sort_order,
    updated_at AS collection_updated_at,
    disjunctive,
    published_at,
    published_scope,
    template_suffix,
    admin_graphql_api_id,
    rules,
    JSON_EXTRACT_SCALAR(rule, '$') AS raw_rule
  FROM
    `shopify-pubsub-project.pilgrim_bi_airbyte.smart_collections`,
    UNNEST(JSON_EXTRACT_ARRAY(rules)) AS rule
),
flattened_rules AS (
  SELECT 
    er.collection_id,
    er._airbyte_extracted_at,
    er.collection_title,
    er.collection_handle,
    er.shop_url,
    er.body_html,
    er.sort_order,
    er.collection_updated_at,
    er.disjunctive,
    er.published_at,
    er.published_scope,
    er.template_suffix,
    er.admin_graphql_api_id,
    REGEXP_REPLACE(raw_rule, r"'", '"') AS fixed_rule
  FROM 
    expanded_rules AS er
)
SELECT DISTINCT
  _airbyte_extracted_at,
  collection_id,
  collection_title,
  collection_handle,
  shop_url,
  body_html,
  sort_order,
  collection_updated_at,
  disjunctive,
  published_at,
  published_scope,
  template_suffix,
  admin_graphql_api_id,
  JSON_EXTRACT_SCALAR(fixed_rule, '$.column') AS type,
  JSON_EXTRACT_SCALAR(fixed_rule, '$.relation') AS relation,
  JSON_EXTRACT_SCALAR(fixed_rule, '$.condition') AS condition
FROM 
  flattened_rules;
