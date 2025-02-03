import pandas as pd
amazon_data = pd.read_excel(
    "C:/Users/shiva/Downloads/MRP Online-Category - Nov 24.xlsx",
    sheet_name='Amazon'
)
smytten_data=pd.read_excel(
    "C:/Users/shiva/Downloads/MRP Online-Category - Nov 24.xlsx",
    sheet_name='Smytten' # Skip the first unnamed column
)
flipkart_data= pd.read_excel(
   "C:/Users/shiva/Downloads/MRP Online-Category - Nov 24.xlsx",
    sheet_name='Flipkart'
)
myntra_sjit=pd.read_excel(
    "C:/Users/shiva/Downloads/MRP Online-Category - Nov 24.xlsx",
    sheet_name='Myntra SJIT'
)
myntra_ppmp=pd.read_excel(
    "C:/Users/shiva/Downloads/MRP Online-Category - Nov 24.xlsx",
    sheet_name='Myntra PPMP'
    
)
website_data=pd.read_excel(
    "C:/Users/shiva/Downloads/MRP Online-Category - Nov 24.xlsx",
    sheet_name='Website'
)
combined_data = pd.concat([amazon_data, flipkart_data, smytten_data, myntra_sjit, myntra_ppmp,website_data],ignore_index=True)
# Save the result to a CSV file
combined_data.to_csv('combined_data.csv', index=False)

print("CSV file 'combined_data1.csv' has been saved successfully.")
df_cleaned = combined_data.dropna(how='all')
# ONLINE RETURN
amzon_return = pd.read_excel("C:/Users/shiva/Downloads/Online Channel Returns (1).xlsx",sheet_name='Amazon')
flipkar_return=pd.read_excel("C:/Users/shiva/Downloads/Online Channel Returns (1).xlsx",sheet_name='Flipkart')
myntra_ppmp=pd.read_excel("C:/Users/shiva/Downloads/Online Channel Returns (1).xlsx",sheet_name='Myntra PPMP')
myntra_sjit=pd.read_excel("C:/Users/shiva/Downloads/Online Channel Returns (1).xlsx",sheet_name='Myntra SJIT')
website_return = pd.read_excel("C:/Users/shiva/Downloads/Online Channel Returns (1).xlsx",sheet_name='Website')
myntra_return = pd.concat([myntra_ppmp, myntra_sjit], ignore_index=True)
combined_return = pd.concat([amzon_return, flipkar_return, myntra_ppmp,myntra_sjit,website_return], ignore_index=True)
combined_return.to_excel('return_b2c.xlsx',index=False)
result = pd.merge(
    df_cleaned,
    combined_return,  # Include 'Qty' in the right DataFrame
    how='left',
    left_on=['SKU', 'Qty'],  # Match on both 'SKU' and 'Qty'
    right_on=['SKU', 'Qty']  # Match on both 'SKU' and 'Qty'
)
# Perform Left Join on 'sku' (easycom) and 'SKU' (combo)
result3 = pd.merge(
    result[['SKU', 'Qty']],  # Selecting required columns from easycom
    comboo[['SKU', 'Cost']],  # Selecting required columns from combo
    left_on='SKU', right_on='SKU', how='left'
)
#result.drop(columns=['SKU'], inplace=True)
#result = result.drop_duplicates()

result = result.drop_duplicates()

# Create new column 'COGS' as item_quantity * Cost
result4['COGS'] = result4['Qty'] * result4['Cost']

# Create a new DataFrame with only 'category' and 'COGS'
cogs_result = result4[['category', 'COGS']]

# Display the final result
(cogs_result.head())
# Create new column 'COGS' as item_quantity * Cost
result5['COGS'] = result5['Qty'] * result5['Cost']

# Create a new DataFrame with only 'category' and 'COGS'
cogs_result = result5[['category', 'COGS']]

# Display the final result
(cogs_result.head())

required_categories = ['Haircare', 'Skincare', 'Makeup']

# Replace categories not in the required list with 'Other'
cogs_result['category'] = cogs_result['category'].apply(lambda x: x if x in required_categories else 'Other')

aggregated_cogs = cogs_result.groupby('category', as_index=False)['COGS'].sum()

# Display the result
(aggregated_cogs.head())

result_nodup=result.drop_duplicates()
easycom=pd.read_csv("C:/Users/shiva/Downloads/bquxjob_61582b60_194aca9ce31.csv")
result = pd.merge(easycom, combined_return,how='left',  left_on=['sku'],  # Match on both 'SKU' and 'Qty'
    right_on=['SKU'])
# Select the required columns
result = result[['SKU','category','Value']]

ans=result.drop_duplicates()
required_categories = ['Haircare', 'Skincare', 'Makeup']

# Replace categories not in the required list with 'Other'
ans5['category'] = ans5['category'].apply(lambda x: x if x in required_categories else 'Other')

# Aggregate the data based on the new category column
aggregated_df = ans5.groupby('category', as_index=False).agg({
    
    'Value':'sum'
    
    # 'Return': 'sum',
    
})
aggregated_df

ans2 = pd.merge(
    combined_data,
    ans,  # Include 'Qty' in the right DataFrame
    how='left',
    on=['SKU'],  # Match on both 'SKU' and 'Qty'
    # Match on both 'SKU' and 'Qty'
)
ans3 = ans2[['category','Gross Sales','Discount','GMV','Value']]
required_categories = ['Haircare', 'Skincare', 'Makeup']

# Replace categories not in the required list with 'Other'
ans['category'] = ans['category'].apply(lambda x: x if x in required_categories else 'Other')

# Aggregate the data based on the new category column
aggregated_df = ans.groupby('category', as_index=False).agg({
    'Gross Sales': 'sum',
    'GST': 'sum',
    'Discount':'sum',
    'GMV':'sum',
   # 'Value':'sum'
    
    # 'Return': 'sum',
    
})
r=pd.read_excel("C:/Users/shiva/Downloads/return_b2c.xlsx")
a=pd.read_excel("C:/Users/shiva/Downloads/ans.xlsx")
x = pd.merge(
    a,  # First dataframe
    r[['SKU', 'Qty', 'Value']],  # Selecting necessary columns from 'r'
    how='left',  # Left join
    on=['SKU']  # Join on both 'SKU' and 'Qty'
)
# Create new column 'COGS' as item_quantity * Cost
result['COGS'] = result['item_quantity'] * result['Cost']

# Create a new DataFrame with only 'category' and 'COGS'
cogs_result = result[['category', 'COGS']]

# Display the final result
(cogs_result.head())
required_categories = ['Haircare', 'Skincare', 'Makeup']

# Replace categories not in the required list with 'Other'
cogs_result['category'] = cogs_result['category'].apply(lambda x: x if x in required_categories else 'Other')

aggregated_cogs = cogs_result.groupby('category', as_index=False)['COGS'].sum()

# Display the result
(aggregated_cogs.head())
