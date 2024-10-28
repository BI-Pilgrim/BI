from bs4 import BeautifulSoup
import requests
import csv
import time
import os
from selenium import webdriver
import pandas as pd
from openpyxl import Workbook
from openpyxl.utils.dataframe import dataframe_to_rows
import re
from pandas_gbq import to_gbq
from datetime import date,timedelta
import datetime
from dateutil.relativedelta import relativedelta


# In[12]:


def Search_page(nopage,key):
    time_interval = 1

    # Define how many page to scrape
    num_of_pages = nopage
    print('Hold your seat belt tight and enjoy the show !!!....')

    categories = ['Face wash','Face Serum','Face Moisturizers','Shampoo','Hair Conditioner','Hair Growth Serum',
                  'Sunscreen','Hair growth Oil','Body Lotion','Hair Mask','Facial Cleanser','Body Mist','EDP Women','EDP Men']

    keyword = key

    category_link = f'https://www.amazon.in/s?k={keyword.replace(" ","+")}&s=exact-aware-popularity-rank&page='

    Image_url_m = []
    Product_Title_m = []
    Quantity_sold_m = []
    No_of_ratings_m = [] 
    Selling_price_m = []
    Unit_price_m = []
    MRP_price_m = []
    Prod_url_m = []
    Asin_m = []
    Top_brands = []

    driver = webdriver.Firefox()
    for i in range(1,num_of_pages+1):
        url = category_link+str(i)
        Prod_url,Asin = Scrape_Page(driver,url)
        Prod_url_m.extend(Prod_url)
        Asin_m.extend(Asin)
        time.sleep(time_interval)

    driver.close()

    df = pd.DataFrame({'Parent_ASIN':Asin_m,'Child_ASIN':Asin_m,'Product_URL':Prod_url_m})
    return df


# In[13]:


def Scrape_Page(driver,inp_url):

    driver.get(inp_url)
    cards = driver.find_elements('xpath','//div[@data-cy="asin-faceout-container"]')
    Product_Title = []
    Prod_url = []
    Asin = []

    for card in cards:

        # Extract Product Link and ASIN Number        
        prod_link = Extract_card(card,'.//div[@data-cy="title-recipe"]//a[@class="a-link-normal s-underline-text s-underline-link-text s-link-style a-text-normal"]')
        if prod_link:
            link = prod_link.get_attribute('href')
            Prod_url.append(link.split('=')[0])

            try:
                url_parts = link.split("/")
                dp_index = url_parts.index("dp")
                asin = url_parts[dp_index + 1]
                Asin.append(asin)

            except:
                Asin.append(0)

        else:
            Prod_url.append(0)

    return Prod_url,Asin


# In[14]:


def Extract_card(card,path):
    try:
        content = card.find_element('xpath',path)
        return content
    except:
        return 0

    
def Extract_table(drive,path):
    try:
        content = drive.find_element('xpath',path)
        return content.text.strip()
    except:
        return 0


# In[15]:


# Create a new column 'maxunitsold' based on conditions
def calculate_maxunitsold(unitsold):
    if unitsold < 100:
        return unitsold + 50
    elif unitsold < 1000:
        return unitsold + 100
    elif unitsold < 10000:
        return unitsold + 1000
    else:
        return unitsold + 10000
    

def Find_competitor(df):
    
    df_grouped = df.groupby('Brand_Name')['Revenue'].sum().reset_index()
    df_sorted = df_grouped.sort_values(by='Revenue', ascending=False)
    top_10_brands = df_sorted.head(10)['Brand_Name'].tolist()
    
    return top_10_brands


# In[16]:


def Scrape_child_asin(df):
    child_url = []
    p_asin = []
    c_asin = []
    std = "https://www.amazon.in/dp/"
    time_interval = 1
    driver = webdriver.Firefox()

    for index, row in df.iterrows():
        
        amazon_url = row['Product_URL']
        driver.get(amazon_url)
        try:
            child_elements = driver.find_elements('xpath', './/div[@id="variation_size_name"]//ul//li')
            for li in child_elements:
                child_asin = li.get_attribute("data-dp-url")
                urlsplit = child_asin.split("/")
                if(len(urlsplit)>2):
                    child_url.append(std+urlsplit[2])
                    c_asin.append(urlsplit[2])
                    p_asin.append(row['Parent_ASIN'])
                    
        except:
            print("No Child ASINs")
            
    driver.close()
    
    return pd.DataFrame({'Parent_ASIN':p_asin,'Child_ASIN':c_asin,'Product_URL':child_url})


# In[17]:


def child_details(df):
   

    child_prod_title = []
    child_qty_sold = []
    child_ratings = []
    Selling_price = []
    Unit_price = []
    MRP_price = []
    Prod_url = []
    P_Asin = []
    C_Asin = []
    brands = []
    urls = []
    size = []
    brand_value = []
    scent = []
    skin_type = []
    benefits = []
    item_form = []
    item_weight = []
    active_ingredient = []
    unit_count = []
    skin_tone = []
    item_volume = []
    special_feature = []
    pack_size = []
    spf_factor = []
    hair_type = []
    material_type_free = []
    recomended_for = []
    best_seller_beauty = []
    best_seller_category = []
    reviews_summary = []
    
    driver = webdriver.Firefox()
    
    prefixes = ["Visit the ", "Brand: "]
    for index, row in df.iterrows():
        url =  row['Product_URL']
        driver.get(url)
        
        Prod_url.append(url)
        P_Asin.append(row['Parent_ASIN'])
        C_Asin.append(row['Child_ASIN'])
        
        brand_name = Extract_table(driver,'//*[@id="bylineInfo"]')
        if brand_name:
            if brand_name.startswith("Visit the "):
                brand_name = brand_name[len("Visit the "):]
            if brand_name.startswith("Brand: "):
                brand_name = brand_name[len("Brand: "):]
            if brand_name.endswith("Store"):
                brand_name = brand_name[:-5]
        brands.append(brand_name)
        urls.append(url)
        
        try:
            div_element = driver.find_element('xpath', '//div[@id="poExpander"]')
            driver.execute_script("arguments[0].style.height = 'auto';", div_element)
        
        except:
                print("No Expander in ",url)
    
        ml = Extract_table(driver,'//*[@id="variation_size_name"]//span[@class="selection"]')
        size.append(ml)
             
        bv = Extract_table(driver,'//tr[@class= "a-spacing-small po-brand"]//td[2]')
        brand_value.append(bv)
        
        sc = Extract_table(driver,'//tr[@class= "a-spacing-small po-scent"]//td[last()]//span')
        scent.append(sc)
        
        sty = Extract_table(driver,'//tr[@class= "a-spacing-small po-skin_type"]//td[2]')
        skin_type.append(sty)
        
        bf = Extract_table(driver,'//tr[@class= "a-spacing-small po-product_benefit"]//td[2]')
        benefits.append(bf)
        
        itf = Extract_table(driver,'//tr[@class= "a-spacing-small po-item_form"]//td[2]')
        item_form.append(itf)
        
        iw = Extract_table(driver,'//tr[@class= "a-spacing-small po-item_weight"]//td[2]')
        item_weight.append(iw)
        
        ai = Extract_table(driver,'//tr[@class= "a-spacing-small po-active_ingredients"]//td[2]')
        active_ingredient.append(ai)
        
        uc = Extract_table(driver,'//tr[@class= "a-spacing-small po-unit_count"]//td[2]')
        unit_count.append(uc)
        
        st = Extract_table(driver,'//tr[@class= "a-spacing-small po-skin_tone"]//td[2]')
        skin_tone.append(st)
        
        iv = Extract_table(driver,'//tr[@class= "a-spacing-small po-item_volume"]//td[2]')
        item_volume.append(iv)
        
        ps = Extract_table(driver,'//tr[@class= "a-spacing-small po-number_of_items"]//td[2]')
        pack_size.append(ps)
        
        spf = Extract_table(driver,'//tr[@class= "a-spacing-small po-sun_protection_factor"]//td[2]')
        spf_factor.append(spf)
        
        ht = Extract_table(driver,'//tr[@class= "a-spacing-small po-hair_type"]//td[2]')
        hair_type.append(ht)
        
        mt = Extract_table(driver,'//tr[@class= "a-spacing-small po-material_type_free"]//td[2]')
        material_type_free.append(mt)
        
        sf = Extract_table(driver,'//tr[@class= "a-spacing-small po-special_features"]//td[2]')
        special_feature.append(sf)
        
        rf = Extract_table(driver,'//tr[@class= "a-spacing-small po-recommended_uses_for_product"]//td[2]')
        recomended_for.append(rf)    
        
        bsib = Extract_table(driver,'//table[@id= "productDetails_detailBullets_sections1"]//tr[last()]//td//span[1]//span[1]')
        if bsib:
            best_seller_beauty.append(bsib.split(" ")[0][1:])
        else:
            best_seller_beauty.append(bsib)
        
        bsic = Extract_table(driver,'//table[@id= "productDetails_detailBullets_sections1"]//tr[last()]//td//span[1]//span[2]')
        if bsic:
            best_seller_category.append(bsic.split(" ")[0][1:])
        else:
            best_seller_category.append(bsic)
            
        rdesc = Extract_table(driver,'//div[@id= "product-summary"]//p[1]')
        reviews_summary.append(rdesc)
        
        # Extract Product title
        pt = Extract_table(driver,'.//h1[@id="title"]//span[@id="productTitle"]')
        child_prod_title.append(pt)


        # Extract rating        
        rt = Extract_table(driver,'.//a[@id="acrCustomerReviewLink"]//span')
        if rt:
            child_ratings.append(rt.split(" ")[0].replace(",",""))
        else:
            child_ratings.append(0)


        # Extract Quantity        
        qty = Extract_table(driver,'.//span[@id="social-proofing-faceout-title-tk_bought"]//span[1]')
        if qty:
            quantity = qty.split(" ")[0].replace('+','')
            if quantity[-1] == 'K':
                actual_value = int(quantity[:-1])*1000
            else:
                actual_value = int(quantity[:-1])*10 + int(quantity[-1])
            child_qty_sold.append(actual_value)
        else:
            child_qty_sold.append(0)


        combo = Extract_table(driver,'//table[@class="a-lineitem a-align-top"]')
        
        if combo:
            # Extract Selling Price        
            sp = Extract_table(driver,'//table[@class="a-lineitem a-align-top"]//tr[2]//td[2]//span[@class="a-price a-text-price a-size-medium apexPriceToPay"]')
            if sp:
                Selling_price.append(sp.replace("₹","").replace(",",""))
            else:
                Selling_price.append(0)



            # Extract Unit Price per 100ml        
            up = Extract_table(driver,'//table[@class="a-lineitem a-align-top"]//tr[2]//td[2]//span[@class="aok-relative"]')
            if up:
                Unit_price.append(up.split("₹")[1][:-1].replace(",",""))
            else:
                Unit_price.append(0)



            # Extract MRP Price        
            mrp = Extract_table(driver,'//table[@class="a-lineitem a-align-top"]//tr[1]//td[2]//span[@class="a-price a-text-price a-size-base"]')
            if mrp:
                MRP_price.append(mrp[1:].replace(",",""))
            else:
                MRP_price.append(0)

        else:


            # Extract Selling Price        
            sp = Extract_table(driver,'.//div[@id="corePriceDisplay_desktop_feature_div"]//div[@class="a-section a-spacing-none aok-align-center aok-relative"]//span[@class="a-price aok-align-center reinventPricePriceToPayMargin priceToPay"]//span[@class = "a-price-whole"]')
            if sp:
                Selling_price.append(sp.replace("₹","").replace(",",""))
            else:
                Selling_price.append(0)



            # Extract Unit Price per 100ml        
            up = Extract_table(driver,'.//div[@id="corePriceDisplay_desktop_feature_div"]//div[@class="a-section a-spacing-none aok-align-center aok-relative"]//span[@class="aok-relative"]//span[2]')
            if up:
                Unit_price.append(up.split("₹")[1][:-1].replace(",",""))
            else:
                Unit_price.append(0)



            # Extract MRP Price        
            mrp = Extract_table(driver,'.//div[@id="corePriceDisplay_desktop_feature_div"]//div[@class="a-section a-spacing-small aok-align-center"]//span[@class="aok-relative"]//span[@class="a-price a-text-price"]')
            if mrp:
                MRP_price.append(mrp[1:].replace(",",""))
            else:
                MRP_price.append(0)

        time.sleep(2)

    driver.close()


    df = pd.DataFrame({'Product_Title':child_prod_title, 'Parent_ASIN':P_Asin,'ASIN':C_Asin, 'Product_URL':Prod_url,
                    'No_Of_Ratings':child_ratings,'MRP_Price':MRP_price,'Per_100ml_price':Unit_price,
                   'Selling_Price':Selling_price,'Unit_Sold':child_qty_sold,'AZ_URL':urls,'Size_of_SKU':size,
                    'Brand_Name':brands,'Brand_value':brand_value,'Best_Seller_in_Beauty':best_seller_beauty,
                    'Best_Seller_in_Category':best_seller_category,'Scent_type':scent,'Skin_type':skin_type,
                    'Benefits':benefits,'Item_form':item_form,'Item_weight':item_weight,'Active_ingredient':active_ingredient,
                    'Net_volume':unit_count,'Skin_tone':skin_tone,'Item_volume':item_volume,'Special_feature':special_feature,
                    'Pack_size':pack_size,'SPF_factor':spf_factor,'Hair_type':hair_type,'Material_type_free':material_type_free,
                    'Special_feature':special_feature,'Recomended_for':recomended_for,'Reviews_Summary':reviews_summary})
    

    return df


# In[18]:


def Brand_Scrape(df):
    time_interval = 1

    driver = webdriver.Firefox()
    brands = []
    urls = []
    size = []
    brand_value = []
    scent = []
    skin_type = []
    benefits = []
    item_form = []
    item_weight = []
    active_ingredient = []
    unit_count = []
    skin_tone = []
    item_volume = []
    special_feature = []
    pack_size = []
    spf_factor = []
    hair_type = []
    material_type_free = []
    recomended_for = []
    best_seller_beauty = []
    best_seller_category = []
    reviews_summary = []
    
    prefixes = ["Visit the ", "Brand: "]
    for index, row in df.iterrows():
        amazon_url = row['Product_URL']
        driver.get(amazon_url)

        brand_name = Extract_table(driver,'//*[@id="bylineInfo"]')
        if brand_name:
            if brand_name.startswith("Visit the "):
                brand_name = brand_name[len("Visit the "):]
            if brand_name.startswith("Brand: "):
                brand_name = brand_name[len("Brand: "):]
            if brand_name.endswith("Store"):
                brand_name = brand_name[:-5]
        brands.append(brand_name)
        urls.append(amazon_url)
        
        try:
            div_element = driver.find_element('xpath', '//div[@id="poExpander"]')
            driver.execute_script("arguments[0].style.height = 'auto';", div_element)
        
        except:
                print("No Expander in ",amazon_url)
    
        ml = Extract_table(driver,'//*[@id="variation_size_name"]//span[@class="selection"]')
        size.append(ml)
             
        bv = Extract_table(driver,'//tr[@class= "a-spacing-small po-brand"]//td[2]')
        brand_value.append(bv)
        
        sc = Extract_table(driver,'//tr[@class= "a-spacing-small po-scent"]//td[last()]//span')
        scent.append(sc)
        
        sty = Extract_table(driver,'//tr[@class= "a-spacing-small po-skin_type"]//td[2]')
        skin_type.append(sty)
        
        bf = Extract_table(driver,'//tr[@class= "a-spacing-small po-product_benefit"]//td[2]')
        benefits.append(bf)
        
        itf = Extract_table(driver,'//tr[@class= "a-spacing-small po-item_form"]//td[2]')
        item_form.append(itf)
        
        iw = Extract_table(driver,'//tr[@class= "a-spacing-small po-item_weight"]//td[2]')
        item_weight.append(iw)
        
        ai = Extract_table(driver,'//tr[@class= "a-spacing-small po-active_ingredients"]//td[2]')
        active_ingredient.append(ai)
        
        uc = Extract_table(driver,'//tr[@class= "a-spacing-small po-unit_count"]//td[2]')
        unit_count.append(uc)
        
        st = Extract_table(driver,'//tr[@class= "a-spacing-small po-skin_tone"]//td[2]')
        skin_tone.append(st)
        
        iv = Extract_table(driver,'//tr[@class= "a-spacing-small po-item_volume"]//td[2]')
        item_volume.append(iv)
        
        ps = Extract_table(driver,'//tr[@class= "a-spacing-small po-number_of_items"]//td[2]')
        pack_size.append(ps)
        
        spf = Extract_table(driver,'//tr[@class= "a-spacing-small po-sun_protection_factor"]//td[2]')
        spf_factor.append(spf)
        
        ht = Extract_table(driver,'//tr[@class= "a-spacing-small po-hair_type"]//td[2]')
        hair_type.append(ht)
        
        mt = Extract_table(driver,'//tr[@class= "a-spacing-small po-material_type_free"]//td[2]')
        material_type_free.append(mt)
        
        sf = Extract_table(driver,'//tr[@class= "a-spacing-small po-special_features"]//td[2]')
        special_feature.append(sf)
        
        rf = Extract_table(driver,'//tr[@class= "a-spacing-small po-recommended_uses_for_product"]//td[2]')
        recomended_for.append(rf)    
        
        bsib = Extract_table(driver,'//table[@id= "productDetails_detailBullets_sections1"]//tr[last()]//td//span[1]//span[1]')
        if bsib:
            best_seller_beauty.append(bsib.split(" ")[0][1:].replace(",",""))
        else:
            best_seller_beauty.append(bsib)
        
        bsic = Extract_table(driver,'//table[@id= "productDetails_detailBullets_sections1"]//tr[last()]//td//span[1]//span[2]')
        if bsic:
            best_seller_category.append(bsic.split(" ")[0][1:].replace(",",""))
        else:
            best_seller_category.append(bsic)
            
        rdesc = Extract_table(driver,'//div[@id= "product-summary"]//p[1]')
        reviews_summary.append(rdesc)
            
        time.sleep(time_interval)
    driver.close()

    op_df = pd.DataFrame({
        'Size_of_SKU':size,
        'Brand_Name':brands,
        'Brand_value':brand_value,
        'Best_Seller_in_Beauty':best_seller_beauty,
        'Best_Seller_in_Category':best_seller_category,
        'Scent_type':scent,
        'Skin_type':skin_type,
        'Benefits':benefits,
        'Item_form':item_form,
        'Item_weight':item_weight,
        'Active_ingredient':active_ingredient,
        'Net_volume':unit_count,
        'Skin_tone':skin_tone,
        'Item_volume':item_volume,
        'Special_feature':special_feature,
        'Pack_size':pack_size,
        'SPF_factor':spf_factor,
        'Hair_type':hair_type,
        'Material_type_free':material_type_free,
        'Special_feature':special_feature,
        'Recomended_for':recomended_for,
        'Reviews_Summary':reviews_summary
    })
    
    return op_df


# In[19]:


def Metrics_derivation(df):
    df_unique = df.drop_duplicates(subset='ASIN')

    # Calculate the max unit slold
    df_unique['Max_Unit_Sold'] = df_unique['Unit_Sold'].apply(calculate_maxunitsold)

    df_unique[['No_Of_Ratings', 'MRP_Price','Selling_Price']] = df_unique[['No_Of_Ratings', 'MRP_Price','Selling_Price']].astype(float)
#     df_unique[['Per_100ml_price']] = df_unique[['Per_100ml_price']].astype(float)

    # Calculate the revenue
    df_unique['Revenue'] = df_unique['Unit_Sold'] * df_unique['Selling_Price']
    df_unique['Max_Revenue'] = df_unique['Max_Unit_Sold'] * df_unique['Selling_Price']

    # Calculate the contribution of each product
    total_revenue = df_unique['Revenue'].sum()
    df_unique['Contribution'] = (df_unique['Revenue'] / total_revenue)

    # Sort the DataFrame by contribution in descending order
    df_unique = df_unique.sort_values('Contribution', ascending=False)
    df_unique['Rolling_sum'] = df_unique['Contribution'].cumsum()

#     df_unique.to_csv(f'{keyword}_MS_without_brand.csv',index=False)
    return df_unique
    
def classify(bt, key):
    f = 0
    for keyword in key.index:
        if keyword in bt.split(','):
            f = 1
            return keyword
    if f == 0:
        return bt.split(",")[0]

def cleaning(df, col):
    df[col] = df[col].astype(str)
    df[col] = df[col].str.replace('&', ',')
    df[col] = df[col].str.replace('|', ',')
    df[col] = df[col].str.replace('and', ',')
    df[col] = df[col].str.lower()

    Benefits = []
    cleaned_list = []
    for row in df[col]:
        Benefits.extend(row.split(','))

    cleaned_list = [item.strip() for item in Benefits if item != '0']

    keyword_counts = pd.Series(cleaned_list).value_counts()
    keyword_counts = keyword_counts.head(20)

    df[col+'_type'] = df.apply(lambda row: classify(row[col], keyword_counts), axis=1)

    return df
def convert_weight(weight_str):

    weight_str = weight_str.lower().strip()

    if weight_str in ['0', 'nan', '', None]:
        return 0

    value, unit = weight_str.split()

    value = float(value)

    if unit == 'pounds':
        return value * 453.59237
    elif unit == 'ounces':
        return value * 28.3495
    elif unit == 'kilograms':
        return value * 1000
    elif unit == 'milligrams':
        return value * 0.001
    else:
        return value


def convert_volume(weight_str):
    if(isinstance(weight_str, str)):
        weight_str = weight_str.lower().strip()

        if weight_str in ['0', 'nan', '', None]:
            return 0
        value, *unit = weight_str.split()
        return float(value)
    else:
        return float(weight_str)
    
def datatype_normalizing(mtdf,column_dtype_mapping):
    
    for col in mtdf.columns:
        type_count = mtdf[col].apply(type).nunique()

        if type_count > 1:
            print(f"Converting column '{col}' to string due to mixed data types.")
            mtdf[col] = mtdf[col].astype(str)
        else:
            print(f"Column '{col}' is consistent with a single data type.")

        
    for col, dtype in column_dtype_mapping.items():
        if col in mtdf.columns:
            try:
                print(f"Converting column '{col}' to {dtype}.")
                mtdf[col] = mtdf[col].astype(dtype)
            except ValueError as e:
                print(f"Error converting column '{col}' to {dtype}: {e}")
    return mtdf

def write_to_gbq(df):
    #  Define your project ID and destination table
    project_id = 'shopify-pubsub-project'
    destination_table = 'Amazon_Market_Sizing.AMZ_Market_Sizing'
    to_gbq(df, destination_table, project_id=project_id, if_exists='append')  # use 'replace', 'append', or 'fail' as needed
    print("Done appending in the table")
    
    


keyword = 'Hair Oil'
No_of_pages = 10

print('Card Scraping Started....',datetime.datetime.now())
pdf = Search_page(No_of_pages,keyword)

time.sleep(2)
print('Child Asin Scraping Started....',datetime.datetime.now())
cdf = Scrape_child_asin(pdf)

merged_df = pd.concat([pdf, cdf], axis=0, ignore_index=True)


time.sleep(2)
print('Child Asin Details Scraping Started....',datetime.datetime.now())
sales_df = child_details(merged_df)


print('Calculating the min max Revenue....',datetime.datetime.now())
mtrcsdf = Metrics_derivation(sales_df)

final_op = mtrcsdf 


final_op['Category'] = keyword
final_op['Date'] = date.today()
# date.today().replace(day=1) - relativedelta(months=1)

column_dtype_mapping = {
    'Product_Title': 'str',
    'Parent_ASIN': 'str',
    'ASIN': 'str',
    'Product_URL': 'str',
    'No_Of_Ratings': 'float',
    'MRP_Price': 'float',
    'Per_100ml_price': 'str',
    'Selling_Price': 'float',      
    'Unit_Sold': 'float',     
    'AZ_URL': 'str',
    'Size_of_SKU': 'str',
    'Brand_Name': 'str',
    'Brand_value': 'str',
    'Best_Seller_in_Beauty': 'int',
    'Best_Seller_in_Category': 'int',
    'Scent_type': 'str',
    'Skin_type': 'str',
    'Benefits': 'str',
    'Item_form': 'str',
    'Item_weight': 'str',
    'Active_ingredient': 'str',
    'Net_volume': 'str',
    'Skin_tone': 'str',
    'Item_volume': 'str',
    'Special_feature': 'float',    
    'Pack_size': 'str',
    'SPF_factor': 'str',
    'Hair_type': 'str',          
    'Material_type_free': 'str',
    'Recomended_for': 'str',
    'Reviews_Summary': 'str',
    'Max_Unit_Sold': 'float',      
    'Revenue': 'float',
    'Max_Revenue': 'float',        
    'Contribution': 'float',
    'Rolling_sum': 'float',
    'Category': 'str',
    'Date': 'datetime64[ns]',      
    'Benefits_type': 'str',
    'Active_ingredient_type': 'str',
    'Material_type_free_type': 'str',
    'Item_weight_in_gm':'float',
    'Net_volume_in_ml':'float',
    'Item_volume_in_ml':'float',
    'SPF_factor_type':'float',
}


# Load the CSV file into a DataFrame
# df = pd.read_csv("D:\MRP_discount_scraping\Sunscreen_MS_with_brand.csv")
bfdf = cleaning(final_op,'Benefits')
aidf = cleaning(bfdf,'Active_ingredient')
mtdf = cleaning(aidf,'Material_type_free')
stdf = cleaning(mtdf,'Scent_type')
ifdf = cleaning(stdf,'Scent_type')
htdf = cleaning(ifdf,'Hair_type')
rfdf = cleaning(htdf,'Recomended_for')

rfdf['Item_weight_in_gm'] = 0.0
rfdf['Net_volume_in_ml'] = 0.0
rfdf['Item_volume_in_ml'] = 0.0
rfdf['SPF_factor_type'] = 0.0

datatype_normalizing(rfdf,column_dtype_mapping)

rfdf['Item_weight_in_gm'] = rfdf['Item_weight'].apply(convert_weight)
rfdf['Net_volume_in_ml'] = rfdf['Net_volume'].apply(convert_volume)
rfdf['Item_volume_in_ml'] = rfdf['Item_volume'].apply(convert_volume)
rfdf['SPF_factor_type'] = rfdf['SPF_factor'].apply(convert_volume)


print("Congrats!!! Market Sizing Done")
print('Scraping Completed at....',datetime.datetime.now())


rfdf = rfdf[rfdf['Parent_ASIN'] != '0']
datatype_normalizing(rfdf,column_dtype_mapping)
# write_to_gbq(rfdf)
rfdf.to_csv(f'{keyword}_MS_with_brand.csv',index=False)

print("Done writing the table")
