
SELECT 
    latest.Category, latest.Brand_value, latest.Product_Title, latest.Parent_ASIN, latest.Child_ASIN,latest.Product_URL, latest.Selling_Price,latest.MRP_Price,
    latest.Unit_Sold, latest.Revenue,
FROM 
    `Amazon_Market_Sizing.AMZ_Current_month_MS` latest
LEFT JOIN 
    `Amazon_Market_Sizing.AMZ_SKU_level_Historical_MS`  past
ON 
    latest.Child_ASIN = past.Child_ASIN
    AND latest.Parent_ASIN = past.Parent_ASIN
WHERE 
    past.Date_MS < DATE_FORMAT(DATE_SUB(CURDATE(), INTERVAL 1 MONTH), '%Y-%m-01') and 
    past.Child_ASIN IS NULL 
    AND past.Parent_ASIN IS NULL 
    and latest.Category Not Like "%EDP%"
    and (latest.Brand_value LIKE '%mCaffeine%' 
   OR latest.Brand_value LIKE '%Plum%' 
   OR latest.Brand_value LIKE '%Minimalist%' 
   OR latest.Brand_value LIKE '%Mama Earth%' 
   OR latest.Brand_value LIKE '%Pilgrim%' 
   OR latest.Brand_value LIKE '%Dot & Key%' 
   OR latest.Brand_value LIKE '%Loreal%' 
   OR latest.Brand_value LIKE '%Dove%' 
   OR latest.Brand_value LIKE '%Tresemme%' 
   OR latest.Brand_value LIKE '%Pantene%' 
   OR latest.Brand_value LIKE '%Head & Shoulders%' 
   OR latest.Brand_value LIKE '%Indulekha%' 
   OR latest.Brand_value LIKE '%Love Beauty%' 
   OR latest.Brand_value LIKE '%Traya%' 
   OR latest.Brand_value LIKE '%Wishcare%' 
   OR latest.Brand_value LIKE '%Wow%' 
   OR latest.Brand_value LIKE '%Soulflower%' 
   OR latest.Brand_value LIKE '%Bare Anatomy%' 
   OR latest.Brand_value LIKE '%Derma Co%' 
   OR latest.Brand_value LIKE '%Plix%' 
   OR latest.Brand_value LIKE '%Ponds%' 
   OR latest.Brand_value LIKE '%Nivea%' 
   OR latest.Brand_value LIKE '%Neutrogena%' 
   OR latest.Brand_value LIKE '%Garnier%' 
   OR latest.Brand_value LIKE '%Lakme%' 
   OR latest.Brand_value LIKE '%Glow & Lovely%' 
   OR latest.Brand_value LIKE '%Aqualogica%' 
   OR latest.Brand_value LIKE '%Dr. Sheth%' 
   OR latest.Brand_value LIKE '%Deconstruct%' 
   OR latest.Brand_value LIKE '%La Shield%')
