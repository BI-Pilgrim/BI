import csv
from selenium import webdriver
import pandas as pd


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
        driver.get(url_base+row['Child_Asin'])
        bsic = Extract_table(driver,'//div[@id="detailBulletsWrapper_feature_div"]/ul[1]/li//ul')
        if bsic:
            temp = ' '.join(bsic.split(" ")[2:])
            rich_inbuilt_category.append(temp.split("\n")[0])
            Category.append(row['Category'])
            asin.append(row['Child_Asin'])                          
        else:
            rich_inbuilt_category.append(bsic)
            Category.append(row['Category'])
            asin.append(row['Child_Asin'])   
    
    driver.close()
    
    df = pd.DataFrame({'Category':Category,'Child_asin':asin,'Inbuilt_category':rich_inbuilt_category})
    
    return df

a = pd.read_csv('Inbuilt_Category_scraping_input.csv')
opdf = child_details(a)
opdf.to_csv('Inbuilt_Category_scraping_output.csv',index = False)
print(opdf)
print('Done')
