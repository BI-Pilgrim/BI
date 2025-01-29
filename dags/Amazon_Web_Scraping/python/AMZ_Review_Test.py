from playwright.sync_api import sync_playwright, Browser, Page, Playwright
import time 
import pandas as pd 
import pandas_gbq as pgbq 
import datetime
from google.oauth2 import service_account
from google.cloud import bigquery
import json
import base64 
from airflow.models import Variable

# Login function - using Sync API (no async involved)
def Login_AMZ(homepage, page, username, password, df): 
    page.goto(homepage,wait_until='load') 
    page.locator('#nav-link-accountList').click() 
    page.wait_for_load_state('load') 
    if page.locator('input#ap_email_login').is_visible():
        # If present, use the 'ap_email_login' field
        page.fill('input#ap_email_login',username)
    else:
        # If not present, use 'ap_email' field as a fallback
        page.fill('input#ap_email',username)
    page.locator('span#continue').click() 
    page.wait_for_load_state('load') 
    page.wait_for_selector('input#ap_password')
    page.fill('input#ap_password',password) 
    time.sleep(2)
    page.locator('#signInSubmit').click() 
    time.sleep(4)

    op_df = ratings_count(page, df)  # Use page here instead of driver 
     # Hover over the account menu
    account_menu = page.locator("#nav-link-accountList") 
    page.wait_for_selector("#nav-link-accountList")
    account_menu.hover()
    
    # Wait for the logout link to be clickable
    logout_link = page.locator("#nav-item-signout")
    logout_link.wait_for(state="attached")  # Wait until the element is attached to the DOM
    
    # Click on the logout link
    logout_link.click()
    
    # Optionally add a sleep here to observe the result
    time.sleep(1) 

    return op_df


def scrape_page(url, page):
    reviews = []
    ratings = []
    dates = []
    names = [] 
    
    #review_blocks = page.locator('//div[id="cm_cr-review_list"]//div[@class="a-section a-spacing-none review-views celwidget"]') #"a-section a-spacing-top-large a-text-center no-reviews-section"
    print('i am here')
    """ if (review_blocks).count() != 0:
        return reviews, ratings, dates, names 
    else:""" 
    review_blocks = page.locator('//li[@class="review aok-relative"]')
    print(f"Found {review_blocks.count()} review blocks")
        #review_elements = review_blocks.element_handles()
    for block in review_blocks.element_handles():
            #print('i am inside loop')
            rating_element = block.query_selector('.a-icon-alt')
            rating_html = rating_element.inner_html()
            rating= float(rating_html.split()[0].replace(',', '.')) 
            ratings.append(rating) 
            review_element = block.query_selector('.a-size-base.review-text.review-text-content span')
            if review_element:
                review_element = review_element.inner_text()  # Get the review text
            else:
                review_element = None  # If not found, set it to None

                # Append review text or NaN if None
            if review_element is None:
                reviews.append(float('nan'))  # Append NaN for missing review text
            else:
                 reviews.append(review_element)  # Append the actual review text
            """review_element = review_element.inner_text()  # Use inner_text() here 
            reviews.append(review_element) """
            date = block.query_selector('.review-date').inner_text()
            date = date.split(' on ')[1]  
            dates.append(date)
            name = block.query_selector('.a-profile-name').inner_text()  # Use inner_text() here
            names.append(name) 

    return reviews, ratings, dates, names


def scrape_multiple_pages(page, base_url, num_pages):
    all_reviews = []
    all_ratings = []
    all_dates = []
    all_names = []
    page_url = []

    for i in range(1, num_pages + 1):
        inp = base_url + '&pageNumber=' + str(i)
        page.goto(inp)
        reviews, ratings, dates, names = scrape_page(inp, page)
        if len(reviews) == 0:
            print('check visit')
            break

        all_reviews.extend(reviews)
        all_ratings.extend(ratings)
        all_dates.extend(dates)
        all_names.extend(names)
        page_url.extend([inp] * len(reviews))
        time.sleep(1)
    return all_reviews, all_ratings, all_dates, all_names, page_url


def ratings_count(page, df):
    num_pages = 1 #10
    df_combined = pd.DataFrame({})
    
    for index, row in df.iterrows():
        amazon_url = row['amazon_url']
        print(amazon_url)
        sku = row['sku'] 
        asin = row['asin']
        title = row['title']
        sku_det = [sku, asin, title]
        rat_rev_cnt = []
        avg_rating = []
        star = ['one_star', 'two_star', 'three_star', 'four_star', 'five_star']     #, 'two_star', 'three_star', 'four_star', 'five_star']
        df_all_stars = pd.DataFrame({})
    
        for s in star:        
            url = amazon_url + '&filterByStar=' + s
            
            page.goto(url,wait_until="load")
            rating_counts = page.locator('div[data-hook="cr-filter-info-review-rating-count"]')
            print(f'rating_counts for {s} star', rating_counts.inner_text())

            # Get average rating
            avg_star = page.locator('span[data-hook="rating-out-of-text"]') 
            avg_rating = float(avg_star.text_content().split(' ')[0]) 
            
            # Get number of ratings and reviews
            rat_rev_cnt = rating_counts.inner_text().split(' ') 
            no_of_ratings = rat_rev_cnt[0]#.replace(',', '')
            no_of_reviews = rat_rev_cnt[3]#.replace(',', '')
            
            reviews, ratings, dates, names, page_URL = scrape_multiple_pages(page, url, num_pages) 
            #print(reviews[0])
            
            # Create a DataFrame for this star rating
            df = pd.DataFrame({
                'Review1': reviews,
                'Rating1': ratings,
                'Date1': dates,
                'Names1': names,
                'Page_url1': page_URL
            })
            df_star = pd.DataFrame({
                'Star1': [s] * len(df),
                'No_of_ratings1': [no_of_ratings] * len(df),
                'No_of_reviews1': [no_of_reviews] * len(df),
                'Avg_Rating1': [avg_rating] * len(df)
            })
            
            df = pd.concat([df, df_star], axis=1)
            df_all_stars = pd.concat([df_all_stars, df])

        # Add product details to the DataFrame
        df_new_cols = pd.DataFrame({
            'Product_Title1': [title] * len(df),
            'SKU1': [sku] * len(df),
            'ASIN1': [asin] * len(df)
        })
        
        df_all_stars = pd.concat([df_all_stars, df_new_cols], axis=1)
        df_combined = pd.concat([df_combined, df_all_stars])

    return df_combined 

def get_bq_client(credentials_info:str)->bigquery.Client:
    credentials_info = base64.b64decode(credentials_info).decode("utf-8")
    credentials_info = json.loads(credentials_info)

    credentials = service_account.Credentials.from_service_account_info(credentials_info)
    client = bigquery.Client(credentials=credentials, project="shopify-pubsub-project")
    return client 

def write_to_gbq(client,df,p_id,t_id):
    table_id = f"{p_id}.{t_id}" 
    job = client.load_table_from_dataframe(df, table_id) 
     # Wait for the load job to complete
    job.result()  # This will block until the job is finished
    
    print(f"Data loaded successfully to {table_id}")


"""def write_to_gbq(df,p_id,t_id):
    pgbq.to_gbq(df, t_id, project_id=p_id, if_exists='append')  # use 'replace', 'append', or 'fail' as needed """

def read_from_gbq(bq_client,p_id,t_id):
    df = bq_client.query(f"SELECT * FROM `{p_id}.{t_id}`").to_dataframe() 
    return df


def datatype_normalizing(mtdf,datatypes):
        
    for col, dtype in datatypes.items():
        if col in mtdf.columns:
            try:
                print(f"Converting column '{col}' to {dtype}.")
                mtdf[col] = mtdf[col].astype(dtype)
            except ValueError as e:
                print(f"Error converting column '{col}' to {dtype}: {e}")
    return mtdf


def delete_latest_month(p_id,t_id,date_col):
    sql = f"""
            DELETE FROM `{p_id}.{t_id}`
            WHERE
            DATE_TRUNC(date({date_col}), MONTH) >= DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 2 MONTH), MONTH)
        """
    df = pgbq.read_gbq(sql, project_id=p_id)
    print("Done Deleting the latest month data from the Rating review") 

def past_table_join(past_table,new_table):
    df_merged = pd.merge(new_table,past_table,left_on=['ASIN1','Names1','Review1','Rating1'],right_on=['ASIN','Names','Review','Rating'],how='left') 
    df_new_review = df_merged[df_merged['SKU'].isna() & df_merged['ASIN'].isna()] 
    return df_new_review 



# Main execution
def main():
    home_page = "https://www.amazon.in/"
    username = "kr.aryan2745@gmail.com"
    password = "harshit27" 
    project_id = 'shopify-pubsub-project'
    table_review = 'Amazon_Market_Sizing.AMZ_Rating_Reviews_Top_products_Playwright_testing' 
    new_table_review_write = 'Amazon_Market_Sizing.AMZ_New_Review_Write'
    table_top_25 = 'Amazon_Market_Sizing.Top_products_for_review_scraping' 
    credentials_info = Variable.get("GOOGLE_BIGQUERY_CREDENTIALS") 
    bq_client = get_bq_client(credentials_info)
    print(bq_client)
    df = read_from_gbq(bq_client,project_id, table_top_25)  
    #df = list(df) 
    #df = pd.DataFrame(df,columns = ['sku', 'asin', 'title', 'amazon_url'] )
    print(df.columns)
    print(len(df))
    print('data read from gbq sucessfully')
    batch_size = 5 

    Review_col_dt = {
        'Review1': 'str',
        'Rating1': 'float',
        'Date1': 'datetime64[ns]',
        'Names1': 'str',
        'Page_url1': 'str',
        'Star1': 'str',
        'No_of_ratings1': 'float',
        'No_of_reviews1': 'float',
        'Product_Title1': 'str',
        'SKU1': 'str',
        'ASIN1': 'str',
        'Avg_Rating1': 'float64',
    }
    
    result_dfs = []
    for i in range(0, len(df), batch_size):
        batch_df = df.iloc[i:i+batch_size]
        with sync_playwright() as p:
            browser = p.firefox.launch(headless=True)
            page = browser.new_page()
            processed_df = Login_AMZ(home_page, page, username, password, batch_df)
            page.close()
            browser.close() 
            result_dfs.append(processed_df)
            time.sleep(2)

    df_combined = pd.concat(result_dfs, ignore_index=True)
    final_df = datatype_normalizing(df_combined, Review_col_dt)
    final_df['Date1'] = pd.to_datetime(final_df['Date1'], format='%d %B %Y')
    df_combined['Scraped_date1'] = datetime.date.today() 
    print('Scrapping success-------') 
    print(df_combined.iloc[0])
    
    past_table = read_from_gbq(bq_client,project_id, table_review) 
    df_new_review = past_table_join(past_table,df_combined) 
    columns_to_keep = ['Review1', 'Rating1', 'Date1', 'Names1', 'Page_url1', 'Star1', 
                   'No_of_ratings1', 'No_of_reviews1', 'Avg_Rating1', 'Product_Title1', 
                   'SKU1', 'ASIN1']
    df_new_review = df_new_review[columns_to_keep]
    write_to_gbq(bq_client,df_new_review,project_id,new_table_review_write)
    print('Done writng in the GBQ ------------')


