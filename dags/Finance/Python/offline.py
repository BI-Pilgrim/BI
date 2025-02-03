
import pandas as pd
export_sales=pd.read_excel("C:/Users/shiva/Downloads/Export - Nov 24.xlsx",sheet_name='Sales1')
export_return=pd.read_excel("C:/Users/shiva/Downloads/Export - Nov 24.xlsx",sheet_name='RETURN')
merged_df = export_sales.merge(
    export_return[['Component SKU', 'EE_Invoice_No', 'Cost Value']], 
    on=['Component SKU','EE_Invoice_No'], 
    how='left',  # Left join
)

# # Replace NaN values in 'Cost Value_return' with 0 before summing
merged_df['Cost Value_x'] = merged_df['Cost Value_x'].fillna(0)
merged_df['Cost Value_y'] = merged_df['Cost Value_y'].fillna(0)

# # Combine Cost Values (Summing from both tables)
merged_df['Total_Cost_Value'] = merged_df['Cost Value_x'] - merged_df['Cost Value_y']

Export = export_sales[['Component SKU','Gross Total','Tax','EE_Invoice_No','Cost','Cost Value']].merge(
    export_return[['Component SKU','Taxable Value','Gross Total','Cost','Cost Value','EE_Invoice_No']],
    on=['Component SKU','EE_Invoice_No'],  # Joining on both 'Component SKU' and 'EE Invoice No'
    how='left'
)
Export.fillna(0, inplace=True)
Export['Total_Gross'] = Export['Gross Total_x'] + Export['Gross Total_y']
Export['Total_Taxable_value']=Export['Taxable Value']
Export['Total_cost']=Export['Cost_x']+Export['Cost_y']
Export['Total_cost_value']=Export['Cost Value_x']Export['Cost Value_y']
export_rename=Export[['Total_cost','Total_cost_value','Component SKU','Gross Total_x','Gross Total_y','Tax','Total_Gross','Total_Taxable_value']].rename(columns={
    'Component SKU': 'Component_SKU',
    'Total_Gross': 'Gross_Sales',
        'Total_Taxable_value':'Returns',
    'Tax':'GST'
    
   
})

EXPORT_FINAL = export_rename[['Component_SKU', 'Gross_Sales', 'Returns', 'GST', 'Total_cost', 'Total_cost_value']]
####################################################################################################################################################################
ebo_sales=pd.read_excel("C:/Users/shiva/Downloads/EBO Sales Report Nov-24 (1).xlsx")
ebo_selected = ebo_sales[['Ref Sku Code', 'Total','Cost','Cost Value']]
ebo_selected = ebo_sales[['Ref Sku Code', 'Total', 'Tax Amount', 'Cost', 'Cost Value']].rename(columns={
    'Ref Sku Code': 'Component_SKU',
    'Total': 'Gross_Sales',
    'Taxable Amount':'Returns',
    'Tax Amount':'GST',  # Use the exact column name
    'Cost':'Total_cost',
    'Cost Value':'Total_cost_value'
})
#############################################################################################################################################
modern_return=pd.read_excel("C:/Users/shiva/Downloads/Modern Trade - Nov 24.xlsx",sheet_name='RETURN')
modern_sales=pd.read_excel("C:/Users/shiva/Downloads/Modern Trade - Nov 24.xlsx",sheet_name='B2B1')
Modern=modern_sales[['Component SKU','Gross Sales','Tax','EE Invoice No','Cost','Cost Value']].merge(
    modern_return[['Component SKU','Taxable Value','Gross Sales','Cost','Cost Value','EE Invoice No']],
    on=['Component SKU','EE Invoice No'],  # Joining on both 'Component SKU' and 'EE Invoice No'
    how='left'
)
Modern.fillna(0, inplace=True)
Modern['Total_Gross'] = Modern['Gross Sales_x'] + Modern['Gross Sales_y']
Modern['Total_Taxable_value']=Modern['Taxable Value']
Modern['Total_cost']=Modern['Cost_x']+Modern['Cost_y']
Modern['Total_cost_value']=Modern['Cost Value_x']+Modern['Cost Value_y']
modern_r = Modern[['Total_cost','Total_cost_value','Component SKU','Gross Sales_x','Gross Sales_y','Tax','Total_Gross','Total_Taxable_value']].rename(columns={
    'Component SKU': 'Component_SKU',
    'Total_Gross': 'Gross_Sales',
        'Total_Taxable_value':'Returns',
    'Tax':'GST'  
})
#############################################################################################################################################################
general_sales=pd.read_excel("C:/Users/shiva/Downloads/General Trade - Nov -24.xlsx",sheet_name='Sheet1')
general_return=pd.read_excel("C:/Users/shiva/Downloads/General Trade - Nov -24.xlsx",sheet_name='RETURN')
General=general_sales[['Component SKU','Gross Sales','Tax','EE Invoice No','Cost','COGS','Item Quantity']].merge(
    general_return[['Component SKU','Taxable Value','Gross Sales','Cost','COGS','EE Invoice No','Item Quantity']],
    on=['Component SKU','EE Invoice No'],  # Joining on both 'Component SKU' and 'EE Invoice No'
    how='left'
)
General.fillna(0, inplace=True)
general_r = General[['Total_cost','Total_cost_value','Component SKU','Gross Sales_x','Gross Sales_y','Tax','Total_Gross','Total_Taxable_value']].rename(columns={
    'Component SKU': 'Component_SKU',
    'Total_Gross': 'Gross_Sales',
        'Total_Taxable_value':'Returns',
    'Tax':'GST'
    
   
})
Genral_final = general_r[['Component_SKU', 'Gross_Sales', 'Returns', 'GST', 'Total_cost', 'Total_cost_value']]
#############################################################################################################################################################3
D2C = pd.concat([Genral_final, Modern_FINAL,ebo_selected,EXPORT_FINAL], ignore_index=True)
D2C[['Gross_Sales', 'Returns', 'GST', 'Total_cost', 'Total_cost_value']] = D2C[[
    'Gross_Sales', 'Returns', 'GST', 'Total_cost', 'Total_cost_value'
]].apply(pd.to_numeric, errors='coerce')

# Now perform the sum
sum_gross = D2C['Gross_Sales'].sum()
sum_tax_value = D2C['Returns'].sum()
sum_gst = D2C['GST'].sum()
sum_cost = D2C['Total_cost'].sum()
sum_cost_value = D2C['Total_cost_value'].sum()
easycom=pd.read_csv("C:/Users/shiva/Downloads/bquxjob_111b55c9_194ac966d69.csv")
combo_cost=pd.read_excel("C:/Users/shiva/Downloads/Combo Master Data - Cost_N.xlsx",sheet_name='SKUWise Cost')
merged_combo = easycom.merge(
    combo_cost, 
    left_on='sku', 
    right_on='SKU', 
    how='left'
)
merged_combo=merged_combo[['sku','category','Cost']]
result = pd.merge(D2C,easycom, how='left', left_on='Component_SKU', right_on='sku')

# Select the required columns
result = result[['Component_SKU','category', 'Gross_Sales', 'GST', 'Returns','Total_cost','Total_cost_value']]
###################################################################################################################################################333
required_categories = ['Haircare', 'Skincare', 'Makeup']

# Replace categories not in the required list with 'Other'
result['category'] = result['category'].apply(lambda x: x if x in required_categories else 'Other')

# Aggregate the data based on the new category column
aggregated_df = result.groupby('category', as_index=False).agg({
    'Gross_Sales': 'sum',
    'GST': 'sum',
    'Returns': 'sum',
    'Total_cost_value':'sum'
    
})
aggregated_df.to_excel('aggregated_output.xlsx', index=False)

