
CREATE OR REPLACE TABLE `shopify-pubsub-project.Data_Warehouse_Amazon_Seller_Staging.XML_BROWSE_TREE_DATA`
PARTITION BY dataEndTime
CLUSTER BY browseNodeId
OPTIONS(
  description = "XML_BROWSE_TREE_DATA table is partitioned on dataEndTime ",
  require_partition_filter = FALSE
)
 AS 
SELECT  
distinct
_airbyte_extracted_at,
JSON_EXTRACT_SCALAR(childNodes,"$.count") as Child_nodes,
dataEndTime,
hasChildren,
browseNodeId,
browseNodeName,
browsePathById,
browsePathByName,
JSON_EXTRACT_SCALAR(attribute_item, '$.name') AS browse_node_attributes_name,
JSON_EXTRACT_SCALAR(attribute_item, '$.text') AS browse_node_attributes_text,
CAST(JSON_EXTRACT_SCALAR(browseNodeAttributes, '$.count') AS INT64) AS browse_node_count,
productTypeDefinitions,
JSON_EXTRACT_SCALAR(refinementsInformation,"$.count") as Refinements,
browseNodeStoreContextName
FROM `shopify-pubsub-project.pilgrim_bi_airbyte_amazon_seller.GET_XML_BROWSE_TREE_DATA`,
UNNEST(JSON_EXTRACT_ARRAY(browseNodeAttributes, '$.attribute')) AS attribute_item
