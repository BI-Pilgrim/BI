MERGE INTO `shopify-pubsub-project.easycom.kits` AS target
USING (
    SELECT
        CAST(product_id AS STRING) AS product_id,
        CAST(sku AS STRING) AS sku,
        CAST(accounting_sku AS STRING) AS accounting_sku,
        CAST(accounting_unit AS STRING) AS accounting_unit,
        mrp,
        add_date,
        last_update_date,
        cost,
        CAST(hsn_code AS STRING) AS hsn_code,
        CAST(colour AS STRING) AS colour,
        weight,
        height,
        length,
        width,
        size,
        CAST(material_type AS INT64) AS material_type, -- Fixed type cast
        CAST(model_number AS STRING) AS model_number,
        CAST(model_name AS STRING) AS model_name,
        CAST(category AS STRING) AS category,
        CAST(brand AS STRING) AS brand,
        CAST(c_id AS INT64) AS c_id, -- Fixed type cast
        ee_extracted_at
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY add_date) AS rn
        FROM `shopify-pubsub-project.easycom.kits`
    )
    WHERE rn = 1 AND add_date >= DATE_SUB(CURRENT_DATE("Asia/Kolkata"), INTERVAL 10 DAY)
) AS source
ON CAST(target.product_id AS STRING) = source.product_id
WHEN MATCHED AND target.add_date < source.add_date THEN
    UPDATE SET
        target.product_id = CAST(source.product_id AS INT64),
        target.sku = source.sku,
        target.accounting_sku = source.accounting_sku,
        target.accounting_unit = source.accounting_unit,
        target.mrp = source.mrp,
        target.add_date = source.add_date,
        target.last_update_date = source.last_update_date,
        target.cost = source.cost,
        target.hsn_code = source.hsn_code,
        target.colour = source.colour,
        target.weight = source.weight,
        target.height = source.height,
        target.length = source.length,
        target.width = source.width,
        target.size = source.size,
        target.material_type = source.material_type, -- Fixed type cast
        target.model_number = source.model_number,
        target.model_name = source.model_name,
        target.category = source.category,
        target.brand = source.brand,
        target.c_id = source.c_id, -- Fixed type cast
        target.ee_extracted_at = source.ee_extracted_at
WHEN NOT MATCHED THEN
    INSERT (
        product_id,
        sku,
        accounting_sku,
        accounting_unit,
        mrp,
        add_date,
        last_update_date,
        cost,
        hsn_code,
        colour,
        weight,
        height,
        length,
        width,
        size,
        material_type,
        model_number,
        model_name,
        category,
        brand,
        c_id,
        ee_extracted_at
    )
    VALUES (
        CAST(source.product_id AS INT64),
        source.sku,
        source.accounting_sku,
        source.accounting_unit,
        source.mrp,
        source.add_date,
        source.last_update_date,
        source.cost,
        source.hsn_code,
        source.colour,
        source.weight,
        source.height,
        source.length,
        source.width,
        source.size,
        source.material_type, -- Fixed type cast
        source.model_number,
        source.model_name,
        source.category,
        source.brand,
        source.c_id, -- Fixed type cast
        source.ee_extracted_at
    );
