from playwright.sync_api import sync_playwright, Browser, Page, Playwright
import pandas as pd
from datetime import date, timedelta
from dateutil.relativedelta import relativedelta
from google.cloud import bigquery
from google.oauth2 import service_account
import json
import base64 
from datetime import datetime
from airflow.models import Variable

def get_bq_client(credentials_info:str)->bigquery.Client:
    credentials_info = base64.b64decode(credentials_info).decode("utf-8")
    credentials_info = json.loads(credentials_info)

    credentials = service_account.Credentials.from_service_account_info(credentials_info)
    client = bigquery.Client(credentials=credentials, project="shopify-pubsub-project")
    return client



def write_to_gbq(client,df,p_id,t_id):
    print('Inside the gbq write function ------------')
    # WRITE_APPEND to append in the existing table WRITE_TRUNCATE to create a new table by deleting the existing one
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
    table_id = f"{p_id}.{t_id}" 
    job = client.load_table_from_dataframe(df, table_id,job_config=job_config) 
    job.result()  
    
    print(f"Data loaded successfully to {table_id}")

def read_from_gbq(bq_client,p_id,t_id):
    df = bq_client.query(f"SELECT * FROM `{p_id}.{t_id}`").to_dataframe() 
    return df

def Rating_Normalization(Title,Product_url,No_of_ratings,No_of_reviews,Avg_rating,One_star_ratings,Two_star_ratings,Three_star_ratings,Four_star_ratings,Five_star_ratings):

    # Create a dictionary with column names and their corresponding lists
    data = {'Title1':Title,'Product_url1':Product_url,'No_of_ratings1':No_of_ratings,'No_of_reviews1':No_of_reviews,'Avg_rating1':Avg_rating,'One_star_ratings1':One_star_ratings,
            'Two_star_ratings1':Two_star_ratings,'Three_star_ratings1':Three_star_ratings,'Four_star_ratings1':Four_star_ratings,'Five_star_ratings1':Five_star_ratings}

    # Create a DataFrame from the dictionary
    mtdf = pd.DataFrame(data)
    datatypes= {
        'Title1':'str',
        'Product_url1':'str',
        'No_of_ratings1':'int64',
        'No_of_reviews1':'int64',
        'Avg_rating1':'float64',
        'One_star_ratings1':'int64',
        'Two_star_ratings1':'int64',
        'Three_star_ratings1':'int64',
        'Four_star_ratings1':'int64',
        'Five_star_ratings1':'int64'
        # 'Scraped_date':'datetime64[ns]'
    }
    for col, dtype in datatypes.items():
        if col in mtdf.columns:
            try:
                print(f"Converting column '{col}' to {dtype}.")
                mtdf[col] = mtdf[col].astype(dtype)
            except ValueError as e:
                print(f"Error converting column '{col}' to {dtype}: {e}")
    
    return mtdf

def Review_Normalization(prod_title,prod_link,prod_star,review_title,review_desc,cust_name,review_date):

    # Create a dictionary with column names and their corresponding lists
    data = {'prod_title1':prod_title,'prod_link1':prod_link,'prod_star1':prod_star,'review_title1':review_title,'review_desc1':review_desc,'cust_name1':cust_name,
            'review_date1':review_date}

    mtdf = pd.DataFrame(data)
    datatypes= {
      'prod_title1':'str',
      'prod_link1':'str',
      'prod_star1':'int64',
      'review_title1':'str',
      'review_desc1':'str',
      'cust_name1':'str',
      'review_date1':'str'
    #   'Scraped_date':'datetime64[ns]'
    }
    for col, dtype in datatypes.items():
        if col in mtdf.columns:
            try:
                print(f"Converting column '{col}' to {dtype}.")
                mtdf[col] = mtdf[col].astype(dtype)
            except ValueError as e:
                print(f"Error converting column '{col}' to {dtype}: {e}")
    return mtdf

def search_element(page,class_name):
    try:
        return page.locator(f"{class_name}").inner_text()
    except Exception as e:
        return -1
    


def Ratings_scraper(df):
    Title = []
    Product_url = []
    No_of_ratings = []
    No_of_reviews = []
    Avg_rating = []
    One_star_ratings = []
    Two_star_ratings = []
    Three_star_ratings = []
    Four_star_ratings = []
    Five_star_ratings = []

    with sync_playwright() as p:
        browser = p.firefox.launch(headless=True) 
        page = browser.new_page()
        for url in df['FK_URL']:
            page.goto(url,wait_until="load")
            Product_url.append(url)
            # To get the title of the product
            title = search_element(page,".Vu3-9u")
            print(title)
            Title.append(title[8:-8] if title!=-1 else 0)

            # To get the rating and review count and avg rating of the product
            rating_review = page.locator(".j-aW8Z").all() 
            if rating_review:
                rating_cnt = rating_review[0].inner_text().split(" ")[0].replace(",","")
                review_cnt = rating_review[1].inner_text().split(" ")[0].replace(",","")
                
            else:
                rating_cnt = 0
                review_cnt = 0
               
            No_of_ratings.append(rating_cnt)
            No_of_reviews.append(review_cnt)

            avg_rating = search_element(page,".ipqd2A")
            Avg_rating.append(avg_rating if avg_rating!=-1 else 0)
            
            # To get star wise rating count
            star_rating = page.locator("[class^='+psZUR']")
            star_rating_counts = star_rating.locator(".fQ-FC1").all()
            if len(star_rating_counts)>0:
                onestar = star_rating_counts[4].inner_text().replace(",","")
                twostar = star_rating_counts[3].inner_text().replace(",","")
                threestar = star_rating_counts[2].inner_text().replace(",","")
                fourstar = star_rating_counts[1].inner_text().replace(",","")
                fivestar = star_rating_counts[0].inner_text().replace(",","")
            else : 
                onestar = 0
                twostar = 0
                threestar = 0
                fourstar = 0
                fivestar = 0

            One_star_ratings.append(onestar)
            Two_star_ratings.append(twostar)
            Three_star_ratings.append(threestar)
            Four_star_ratings.append(fourstar)
            Five_star_ratings.append(fivestar)

            print(f'Done with {title} product ')
        browser.close()
    rat_final = Rating_Normalization(Title,Product_url,No_of_ratings,No_of_reviews,Avg_rating,One_star_ratings,
                                     Two_star_ratings,Three_star_ratings,Four_star_ratings,Five_star_ratings)
    print('Done with ratings')
    return rat_final

def Reviews_scraper(df):
    with sync_playwright() as p:
        browser = p.firefox.launch(headless=True)  # Change to .firefox or .webkit if needed
        page = browser.new_page()
        prod_link = []
        prod_star = []
        review_title = []
        review_desc = []
        cust_name = []
        review_date = []
        prod_title = []
        for link in df['FK_URL']:
            
            for pg_no in range(1,11):
                for s in ['NEGATIVE_FIRST','POSITIVE_FIRST']:
                    url = link+f"&sortOrder={s}&page={pg_no}"
                    print(url)
                    
                    page.goto(url,wait_until="load")
                    # To get the title of the product
                    Review_block = page.locator(".EPCmJX").all()
                    for block in Review_block:

                        title = search_element(page,".Vu3-9u")
                        prod_title.append(title[8:-8] if title!=-1 else 0)

                        star = search_element(block,".XQDdHH")
                        prod_star.append(star if star!=-1 else 0)

                        r_title = search_element(block,".z9E0IG")
                        review_title.append(r_title if r_title!=-1 else 0)

                        desc = search_element(block,".ZmyHeo")
                        review_desc.append(desc if desc!=-1 else 0)
                        months = ['jan,', 'feb,', 'mar,', 'apr,', 'may,', 'jun,', 'jul,', 'aug,', 'sep,', 'oct,', 'nov,', 'dec,']
                        name_date = block.locator("._2NsDsF").all()
                        if len(name_date)>0:
                            name = name_date[0].inner_text()
                            rv_date = name_date[1].inner_text()
                            date_text = rv_date.split(" ")
                            if rv_date =="Today":
                                rvw_date= date.today()
                            elif date_text[1] == 'day' or date_text[1] == 'days':
                                rvw_date = date.today()-timedelta(days=int(date_text[0]))
                            elif date_text[1] == 'month' or date_text[1] == 'months':
                                rvw_date = date.today()-relativedelta(months=int(date_text[0]))
                            elif date_text[0].lower() in months:
                                i =  months.index(date_text[0].lower())
                                rvw_date = f"{date_text[1]}-{str(i+1).zfill(2)}-01"
                            else:
                                rvw_date = date_text

                        else:
                            name = 0
                            rvw_date = 0
                        cust_name.append(name)
                        review_date.append(rvw_date)
                        prod_link.append(url)

            print("Done Scraping for the product ",title)
      
    Rv_df = Review_Normalization(prod_title,prod_link,prod_star,review_title,review_desc,cust_name,review_date)
    print("Done with reviews scraping")
    return Rv_df

def main():
    project_id = 'shopify-pubsub-project'
    FK_top_products = 'Project_GOONJ.Flipkart_top_25_products'
    FK_ratings = 'Project_GOONJ.Flipkart_Rating_top_products'
    FK_reviews = 'Project_GOONJ.Flipkart_Reviews_top_products'
    FK_test = 'Project_GOONJ.Flipkart_Reviews_test'
    credentials_info = Variable.get("GOOGLE_BIGQUERY_CREDENTIALS") 
    bq_client = get_bq_client(credentials_info)
    product_list = read_from_gbq(bq_client,project_id, FK_top_products)  
   
    print('Starting with Rating Scraping',datetime.now())
    Ratings_op = Ratings_scraper(product_list)
    Ratings_op['Scraped_date1'] = date.today()
    

    old_ratings = read_from_gbq(bq_client,project_id, FK_ratings)

    merged_df = pd.merge(Ratings_op,old_ratings, 
                    left_on=['Title1', 'Product_url1', 'Scraped_date1'], right_on=['Title', 'Product_url', 'Scraped_date'],
                    how='left') 

    # Filter where B.prod_title is null
    filtered_df = merged_df[merged_df['Title'].isnull()]

    # Select only columns from df1
    new_col = [
        'Title1','Product_url1','No_of_ratings1','No_of_reviews1','Avg_rating1','One_star_ratings1',
        'Two_star_ratings1','Three_star_ratings1','Four_star_ratings1','Five_star_ratings1','Scraped_date1']
    filtered_df = filtered_df[new_col]
    filtered_df = filtered_df.rename(columns={
        'Title1':'Title',
        'Product_url1':'Product_url',
        'No_of_ratings1':'No_of_ratings',
        'No_of_reviews1':'No_of_reviews',
        'Avg_rating1':'Avg_rating',
        'One_star_ratings1':'One_star_ratings',
        'Two_star_ratings1':'Two_star_ratings',
        'Three_star_ratings1':'Three_star_ratings',
        'Four_star_ratings1':'Four_star_ratings',
        'Five_star_ratings1':'Five_star_ratings',
        'Scraped_date1':'Scraped_date'
})
    filtered_df.drop_duplicates(subset=['Title', 'Product_url','Scraped_date'],inplace=True)
    write_to_gbq(bq_client,filtered_df,project_id,FK_ratings)

    print('Starting with Review Scraping',datetime.now())
    Review_op = Reviews_scraper(product_list)
    Review_op['Scraped_date1'] = date.today()
    # write_to_gbq(bq_client,Review_op,project_id,FK_reviews)
    print('Ending with Review Scraping',datetime.now())
    # Review_op.to_csv('Review_op.csv',index=False)

    old_reviews = read_from_gbq(bq_client,project_id,FK_reviews)

    merged_df = pd.merge(Review_op,old_reviews, 
                    left_on=['prod_title1', 'prod_star1', 'review_title1', 'review_desc1'], right_on=['prod_title', 'prod_star', 'review_title', 'review_desc'],
                    how='left') 

    filtered_df = merged_df[merged_df['prod_title'].isnull()]


    new_col = [
      'prod_title1',
      'prod_link1',
      'prod_star1',
      'review_title1',
      'review_desc1',
      'cust_name1',
      'review_date1',
      'Scraped_date1']
    filtered_df = filtered_df[new_col]
    filtered_df = filtered_df.rename(columns={
    'prod_title1':'prod_title',
    'prod_link1':'prod_link',
    'prod_star1':'prod_star',
    'review_title1':'review_title',
    'review_desc1':'review_desc',
    'cust_name1':'cust_name',
    'review_date1':'review_date',
    'Scraped_date1':'Scraped_date'
})
    filtered_df.drop_duplicates(subset=['prod_title', 'prod_star','review_title','review_desc','cust_name'],inplace=True)
    write_to_gbq(bq_client,filtered_df,project_id,FK_reviews)
