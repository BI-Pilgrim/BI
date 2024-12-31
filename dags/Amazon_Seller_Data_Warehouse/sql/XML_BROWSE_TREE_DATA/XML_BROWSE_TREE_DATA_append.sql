MERGE INTO `shopify-pubsub-project.Data_Warehouse_Amazon_Seller_Staging.XML_BROWSE_TREE_DATA` AS target  
USING (
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

WHERE DATE(_airbyte_extracted_at) >= DATE_SUB(CURRENT_DATE("Asia/Kolkata"), INTERVAL 10 DAY)
) AS source

ON target.browseNodeId = source.browseNodeId

WHEN MATCHED AND source._airbyte_extracted_at > target._airbyte_extracted_at 
THEN UPDATE SET

target._airbyte_extracted_at = source._airbyte_extracted_at,
target.Child_nodes = source.Child_nodes,
target.dataEndTime = source.dataEndTime,
target.hasChildren = source.hasChildren,
target.browseNodeId = source.browseNodeId,
target.browseNodeName = source.browseNodeName,
target.browsePathById = source.browsePathById,
target.browsePathByName = source.browsePathByName,
target.browse_node_attributes_name = source.browse_node_attributes_name,
target.browse_node_attributes_text = source.browse_node_attributes_text,
target.browse_node_count = source.browse_node_count,
target.productTypeDefinitions = source.productTypeDefinitions,
target.Refinements = source.Refinements,
target.browseNodeStoreContextName = source.browseNodeStoreContextName

WHEN NOT MATCHED THEN INSERT (
_airbyte_extracted_at,
Child_nodes,
dataEndTime,
hasChildren,
browseNodeId,
browseNodeName,
browsePathById,
browsePathByName,
browse_node_attributes_name,
browse_node_attributes_text,
browse_node_count,
productTypeDefinitions,
Refinements,
browseNodeStoreContextName
    
  )
  VALUES (
  source._airbyte_extracted_at,
  source.Child_nodes,
  source.dataEndTime,
  source.hasChildren,
  source.browseNodeId,
  source.browseNodeName,
  source.browsePathById,
  source.browsePathByName,
  source.browse_node_attributes_name,
  source.browse_node_attributes_text,
  source.browse_node_count,
  source.productTypeDefinitions,
  source.Refinements,
  source.browseNodeStoreContextName
  );
