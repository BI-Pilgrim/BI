import pandas as pd
market_place=pd.read_excel("C:/Users/shiva/Downloads/Marketplace SKU Master 10-01-2025 (1).xlsx")
combo=pd.read_excel("C:/Users/shiva/Downloads/Combo Master Data - Cost_N.xlsx",sheet_name='SKUWise Cost')
result = pd.merge(
    b2b_combined[['Total_sku_id', 'Total_qty']],  # Selecting required columns from easycom
    market_place[['Product ID']],  # Selecting required columns from combo
    left_on='Total_sku_id', right_on='Product ID', how='left'
)
# Drop duplicate SKU column after join
#merged_df.drop(columns=['SKU'], inplace=True)

# Replace NaN in Cost with 0
#merged_combo['Cost'] = merged_combo['Cost'].fillna(0)
result.tail()
merged_df = amaz_b2b.merge(market_place, left_on='SKU_ID', right_on='Product ID', how='left')

# Calculate required columns
merged_df['GMV'] = merged_df['MRP'] * merged_df['QTY']
merged_df['Discount'] = merged_df['GMV'] - merged_df['GROSS_SALE']
merged_df['Commission'] = merged_df['MRP'] * .2714
merged_df['Net_Sales'] = merged_df['GMV'] - merged_df['Discount'] - merged_df['GST'] - merged_df['Commission']

# Select and rename final columns
final_df = merged_df[['SKU_ID','Brand SKU','Category', 'GMV', 'Discount', 'GROSS_SALE', 'GST', 'Commission', 'Net_Sales','QTY']]
standardized_columns = {
    col.strip().lower(): col.strip() for col in final_df.columns  # Normalize
}

# Correct common spelling mistakes (optional)
corrections = {
    'gmv': 'GMV',
    'discount': 'Discount',
    'gross_sale': 'GROSS_SALE',
    'gst': 'GST',
    'commission': 'Commission',
    'net_sales': 'Net_Sales',
    'category': 'Category'
}

# Apply corrections and rename to "Total_<colname>" format
final_df.columns = [f"Total_{corrections.get(col, col)}" for col in standardized_columns.keys()]

# Display or save the result
(final_df.head(99))

# Save to Excel
final_df.to_excel('amazon.xlsx', index=False)
pd.set_option('display.max_rows', None)

(final_df.head())
#MRP - Discount - GST - Commision
#######################################################################################################################33'
blinkit_b2b=pd.read_excel("C:/Users/shiva/Downloads/Blinkit Sales_Novmber 2024 (1).xlsx")
merged_df1 = blinkit_b2b.merge(market_place, 
                              left_on='SKU_ID', 
                              right_on='Product ID', 
                              how='left')

# Calculate Net Sales (Gross_Sale - GST - Commission)
merged_df1['Net_Sales'] = merged_df1['GMV']- merged_df1['Discount']- merged_df1['GST'] - merged_df1['Commission']

# Select required columns
final_df1 = merged_df1[['SKU_ID','Brand SKU','Category', 'GMV', 'Discount', 'Gross_Sale', 'GST', 'Commission', 'Net_Sales','QTY']]
standardized_columns = {
    col.strip().lower(): col.strip() for col in final_df1.columns  # Normalize
}

# Correct common spelling mistakes (optional)
corrections = {
    'gmv': 'GMV',
    'discount': 'Discount',
    'gross_sale': 'GROSS_SALE',
    'gst': 'GST',
    'commission': 'Commission',
    'net_sales': 'Net_Sales',
    'category': 'Category'
}

# Apply corrections and rename to "Total_<colname>" format
final_df1.columns = [f"Total_{corrections.get(col, col)}" for col in standardized_columns.keys()]


# Save the result to an Excel file
final_df1.to_excel('blinkit_b2b_final.xlsx', index=False)

(final_df1.head())
#MRP - Discount - GST - Commision
###########################################################################################################################33333
first_b2b=pd.read_excel("C:/Users/shiva/Downloads/First Cry_Pilgrim Sales and Claim Working - Nov'24 (Rs 7,50,211.42).xlsx")
merged_df2 = first_b2b.merge(market_place, 
                              left_on='SKU_ID', 
                              right_on='Product ID', 
                              how='left')

# Calculate Net Sales (Gross_Sale - GST - Commission)
merged_df2['Net_Sales'] = merged_df1['GMV']- merged_df1['Discount']- merged_df1['GST'] - merged_df1['Commission']

# Select required columns
final_df2 = merged_df2[['SKU_ID','Brand SKU','Category', 'GMV', 'Discount', 'GROSS_SALE', 'GST', 'Commission', 'Net_Sales','Qty']]
standardized_columns = {
    col.strip().lower(): col.strip() for col in final_df2.columns  # Normalize
}

# Correct common spelling mistakes (optional)
corrections = {
    'gmv': 'GMV',
    'discount': 'Discount',
    'gross_sale': 'GROSS_SALE',
    'gst': 'GST',
    'commission': 'Commission',
    'net_sales': 'Net_Sales',
    'category': 'Category',
    'Qty':'QTY'
}

# Apply corrections and rename to "Total_<colname>" format
final_df2.columns = [f"Total_{corrections.get(col, col)}" for col in standardized_columns.keys()]



# Save the result to an Excel file
final_df2.to_excel('first_cry.xlsx', index=False)

(final_df2.head())
#MRP - Discount - GST - Commision
#################################################################################################################3333333
myntra_b2b=pd.read_excel("C:/Users/shiva/Downloads/Myntra B2B November 2024 (1).xlsx")

merged_df3 = myntra_b2b.merge(market_place, 
                              left_on='SKU_ID', 
                              right_on='Product ID', 
                              how='left')

# Calculate Net Sales (Gross_Sale - GST - Commission)
merged_df3['Net_Sales'] = merged_df3['GMV']- merged_df3['Discount']- merged_df3['GST'] - merged_df3['Commission']

# Select required columns
final_df3 = merged_df3[['SKU_ID','Brand SKU','Category', 'GMV', 'Discount', 'Gross_Sale', 'GST', 'Commission', 'Net_Sales','qty']]
standardized_columns = {
    col.strip().lower(): col.strip() for col in final_df3.columns  # Normalize
}

# Correct common spelling mistakes (optional)
corrections = {
    'gmv': 'GMV',
    'discount': 'Discount',
    'gross_sale': 'GROSS_SALE',
    'gst': 'GST',
    'commission': 'Commission',
    'net_sales': 'Net_Sales',
    'category': 'Category'
}

# Apply corrections and rename to "Total_<colname>" format
final_df3.columns = [f"Total_{corrections.get(col, col)}" for col in standardized_columns.keys()]


# Save the result to an Excel file
final_df3.to_excel('myntra.xlsx', index=False)

(final_df3.head())
#MRP - Discount - GST - Commision
######################################################################################################33333
insta_b2b=pd.read_excel("C:/Users/shiva/Downloads/Instamart_Pilgrim Nov.xlsx")
merged_df4 = insta_b2b.merge(market_place, 
                              left_on='SKU_ID', 
                              right_on='Product ID', 
                              how='left')

# Calculate Net Sales (Gross_Sale - GST - Commission)
merged_df4['Net_Sales'] = merged_df4['gmv']- merged_df4['Discount']- merged_df4['GST'] - merged_df4['Commission']

# Select required columns
final_df4 = merged_df4[['SKU_ID','Brand SKU','Category', 'gmv', 'Discount', 'GROSS_SALE', 'GST', 'Commission', 'Net_Sales','QTY']]
standardized_columns = {
    col.strip().lower(): col.strip() for col in final_df4.columns  # Normalize
}

# Correct common spelling mistakes (optional)
corrections = {
    'gmv': 'GMV',
    'discount': 'Discount',
    'gross_sale': 'GROSS_SALE',
    'gst': 'GST',
    'commission': 'Commission',
    'net_sales': 'Net_Sales',
    'category': 'Category',
    'Final_Qty':'QTY'
}

# Apply corrections and rename to "Total_<colname>" format
final_df4.columns = [f"Total_{corrections.get(col, col)}" for col in standardized_columns.keys()]


# Save the result to an Excel file
final_df4.to_excel('intsamart.xlsx', index=False)

(final_df4.head())
#MRP - Discount - GST - Commision
################################################################################################################
purplle_b2b=pd.read_excel("C:/Users/shiva/Downloads/Purplle Nov-24.xlsx")
merged_df5 = purplle_b2b.merge(market_place, 
                              left_on='SKU_ID', 
                              right_on='Product ID', 
                              how='left')

# Calculate Net Sales (Gross_Sale - GST - Commission)
merged_df5['Net_Sales'] = merged_df5['GMV']- merged_df5['Discount']- merged_df5['GST'] - merged_df5['Commission']

# Select required columns
final_df5 = merged_df5[['SKU_ID','Brand SKU','Category', 'GMV', 'Discount', 'Gross_Sale', 'GST', 'Commission', 'Net_Sales','Qty']]
standardized_columns = {
    col.strip().lower(): col.strip() for col in final_df5.columns  # Normalize
}

# Correct common spelling mistakes (optional)
corrections = {
    'gmv': 'GMV',
    'discount': 'Discount',
    'gross_sale': 'GROSS_SALE',
    'gst': 'GST',
    'commission': 'Commission',
    'net_sales': 'Net_Sales',
    'category': 'Category'
}

# Apply corrections and rename to "Total_<colname>" format
final_df5.columns = [f"Total_{corrections.get(col, col)}" for col in standardized_columns.keys()]


# Save the result to an Excel file
final_df5.to_excel('purplle.xlsx', index=False)

(final_df5.head())
#MRP - Discount - GST - Commision
####################################################################################################3333333
zepto_b2b=pd.read_excel("C:/Users/shiva/Downloads/Zepto Nov2024 (2).xlsx")
merged_df6 = zepto_b2b.merge(market_place, 
                              left_on='SKU_ID', 
                              right_on='Product ID', 
                              how='left')

# Calculate Net Sales (Gross_Sale - GST - Commission)
merged_df6['Net_Sales'] = merged_df6['GMV']- merged_df6['Discount']- merged_df6['GST'] - merged_df6['Commission']

# Select required columns
final_df6 = merged_df6[['SKU_ID','Brand SKU','Category', 'GMV', 'Discount', 'Gross_Sale', 'GST', 'Commission', 'Net_Sales','QTY']]
standardized_columns = {
    col.strip().lower(): col.strip() for col in final_df6.columns  # Normalize
}

# Correct common spelling mistakes (optional)
corrections = {
    'gmv': 'GMV',
    'discount': 'Discount',
    'gross_sale': 'GROSS_SALE',
    'gst': 'GST',
    'commission': 'Commission',
    'net_sales': 'Net_Sales',
    'category': 'Category'
}

# Apply corrections and rename to "Total_<colname>" format
final_df6.columns = [f"Total_{corrections.get(col, col)}" for col in standardized_columns.keys()]


# Save the result to an Excel file
final_df6.to_excel('zepto.xlsx', index=False)

(final_df6.head())
#MRP - Discount - GST - Commisionny
#########################################################################################################
nyka_b2b=pd.read_excel("C:/Users/shiva/Downloads/nyka.xlsx")
merged_df7 = nyka_b2b.merge(market_place, 
                              left_on='SKU_ID', 
                              right_on='Product ID', 
                              how='left')

# Calculate Net Sales (Gross_Sale - GST - Commission)
merged_df7['Net_Sales'] = merged_df7['GMV']- merged_df7['DISCOUNT']- merged_df7['GST'] - merged_df7['COMMISSION']

# Select required columns
final_df7 = merged_df7[['SKU_ID','Brand SKU','Category', 'GMV', 'DISCOUNT', 'Gross_Sale', 'GST', 'COMMISSION', 'Net_Sales','QTY']]
standardized_columns = {
    col.strip().lower(): col.strip() for col in final_df7.columns  # Normalize
}


# Create COGS column
last['COGS'] = last['Total_qty'] * last['Cost']

# Categorize into defined groups, else classify as "Other"
last['Category_Grouped'] = last['Total_Category'].apply(lambda x: x if x in required_categories else 'Other')

# Aggregate COGS based on the new grouped category
aggregated_cogs = last.groupby('Category_Grouped',as_index=False)['COGS'].sum()

# Display result
print(aggregated_cogs)
required_categories = ['Hair Care','Skin Care', 'Makeup']

# Replace categories not in the required list with 'Other'
last['Total_Category'] = last['Total_Category'].apply(lambda x: x if x in required_categories else 'Other')

# Create the COGS column
last['COGS'] = last['Total_qty'] * last['Cost']

# Aggregate the data based on the updated category column
aggregated_df = last.groupby('Total_Category', as_index=False).agg({
    'Total_GMV': 'sum',
    'Total_GROSS_SALE': 'sum',
    'Total_Discount': 'sum',
    'Total_GST': 'sum',
    'Total_Commission': 'sum',
    'Total_Net_Sales': 'sum',
    'COGS': 'sum'
})

# Display the aggregated dataframe
(aggregated_df.head())

# Correct common spelling mistakes (optional)
corrections = {
    'gmv': 'GMV',
    'discount': 'Discount',
    'gross_sale': 'GROSS_SALE',
    'gst': 'GST',
    'commission': 'Commission',
    'net_sales': 'Net_Sales',
    'category': 'Category',
}

# Apply corrections and rename to "Total_<colname>" format
final_df7.columns = [f"Total_{corrections.get(col, col)}" for col in standardized_columns.keys()]


# Save the result to an Excel file
final_df7.to_excel('nyka.xlsx', index=False)

(final_df7.head(5))
#MRP - Discount - GST - Commisionny
############################################################################################################################
# Union all datasets by concatenation (row-wise merge)
b2b_combined = pd.concat([final_df, final_df2,final_df3,final_df4,final_df5,final_df6,final_df7], ignore_index=True)

# Optional: Remove duplicates if necessary
b2b_combined.drop_duplicates(inplace=True)

last=b2b_combined.merge(combo, 
                              left_on='Total_brand sku', 
                              right_on='SKU', 
                              how='left')
last['COGS'] = last['Total_qty'] * last['Cost']  # Create COGS column'

# Define required categories
required_categories = ['Hair Care', 'Skin Care', 'Makeup']

# Create COGS column
last['COGS'] = last['Total_qty'] * last['Cost']

# Categorize into defined groups, else classify as "Other"
last['Category_Grouped'] = last['Total_Category'].apply(lambda x: x if x in required_categories else 'Other')

# Aggregate COGS based on the new grouped category
aggregated_cogs = last.groupby('Category_Grouped',as_index=False)['COGS'].sum()

# Display result
print(aggregated_cogs)
required_categories = ['Hair Care','Skin Care', 'Makeup']

# Replace categories not in the required list with 'Other'
last['Total_Category'] = last['Total_Category'].apply(lambda x: x if x in required_categories else 'Other')

# Create the COGS column
last['COGS'] = last['Total_qty'] * last['Cost']

# Aggregate the data based on the updated category column
aggregated_df = last.groupby('Total_Category', as_index=False).agg({
    'Total_GMV': 'sum',
    'Total_GROSS_SALE': 'sum',
    'Total_Discount': 'sum',
    'Total_GST': 'sum',
    'Total_Commission': 'sum',
    'Total_Net_Sales': 'sum',
    'COGS': 'sum'
})

# Display the aggregated dataframe
(aggregated_df.head())
