import csv
from selenium import webdriver
import pandas as pd
from pandas_gbq import to_gbq

def write_to_gbq(df):
    #  Define your project ID and destination table
    project_id = 'shopify-pubsub-project'
    destination_table = 'Amazon_Market_Sizing.AMZ_ASIN_missing_category'
    to_gbq(df, destination_table, project_id=project_id, if_exists='append')  # use 'replace', 'append', or 'fail' as needed
    print("Done appending in the table")
    
def Extract_table(drive,path):
    try:
        content = drive.find_element('xpath',path)
        return content.text.strip()
    except:
        return 0
    
def child_details(df):
    rich_inbuilt_category = []
    asin = []
    Category = []
    Inbuilt_category = []
    driver = webdriver.Firefox()

    url_base = 'https://www.amazon.in/dp/'

    for index, row in df.iterrows():
        driver.get(url_base+row['Child_ASIN'])
        bsic = Extract_table(driver,'//div[@id="detailBulletsWrapper_feature_div"]/ul[1]/li//ul/li[1]')
        if bsic:
            temp = ' '.join(bsic.split(" ")[2:])
            rich_inbuilt_category.append(temp.split("\n")[0])
            asin.append(row['Child_ASIN'])                          
        else:
            rich_inbuilt_category.append(bsic)
            asin.append(row['Child_ASIN'])   
    
    driver.close()
    
    df = pd.DataFrame({'Child_ASIN':asin,'Inbuilt_category':rich_inbuilt_category})
    
    return df

a = pd.read_csv('Asin_empty_cat1.csv')
opdf = child_details(a)
opdf.to_csv('Inbuilt_Category_scraping_output.csv',index = False)
opdf['Inbuilt_category'] = opdf['Inbuilt_category'].astype('string')
write_to_gbq(opdf)
print('Done')
