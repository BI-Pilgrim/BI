from playwright.sync_api import sync_playwright, Browser, Page, Playwright
import time 
import pandas as pd 
import pandas_gbq as pgbq 
import datetime
import numpy as np 
from fuzzywuzzy import fuzz
# Login function - using Sync API (no async involved)
def Login_AMZ(homepage, username, password, df): 
    with sync_playwright() as p:
        browser = p.firefox.launch(headless=True)
        page = browser.new_page()
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

        op_df = ratings_count(page, df)   
     # Hover over the account menu
        account_menu = page.locator("#nav-link-accountList") 
        page.wait_for_selector("#nav-link-accountList")
        account_menu.hover()
    
    # Wait for the logout link to be clickable
        logout_link = page.locator("#nav-item-signout")
        logout_link.wait_for(state="attached")  # Wait until the element is attached to the DOM
    
    # Click on the logout link
        logout_link.click() 
        page.close()
        browser.close()
    
    # Optionally add a sleep here to observe the result
        time.sleep(1) 

    return op_df

def scrape_page(url, page):
    reviews = []
    ratings = []
    dates = []
    names = [] 
    
    print('i am here')
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
    num_pages = 10#10
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
        star = ['one_star', 'two_star', 'three_star', 'four_star', 'five_star']     
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
            no_of_ratings = rat_rev_cnt[0].replace(',', '')
            no_of_reviews = rat_rev_cnt[3].replace(',', '')
            
            reviews, ratings, dates, names, page_URL = scrape_multiple_pages(page, url, num_pages) 
            #print(reviews[0])
            
            # Create a DataFrame for this star rating
            df = pd.DataFrame({
                'Review1': reviews,
                'Rating1': ratings,
                'Date_R1': dates,
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

def check_keywords(review):
    # Define keyword lists
    Efficacyw = ['ineffective','disappoint','useless','hyped','unreliable','result','misleading','faulty','inferior','works ','improve ','change ','difference ','satisfied ','happy ','expect','waste ','false promsie ','advertise','ads','worse','long ','slow','faded ','drying ','frizzy ','hydrate ','smooth','tanning ','sun burn ','coverage ','moisturizing ','tangles ','no shine ','no glow ','dries','no protection ','not moisturizing ','build up ','efficacy ','efficient ','skin','hair ','face ','problem ','solve ','didnt ','doesnt ','poor quality','product','worse ','pilgrim ','brand ','awful ','recommend','dont buy ','dont believe','bad','not good','claimed ','social media ','cheap','hydration ','influncer ','fool','did not ','not like ','cheek','forehead','nose','lips','body ','scalp ','average ','review','marketing ','experince ','darkspot ','exfoliating','flaky','rough ','brittle ','purchase','chemical','not to buy ','kharab','blunder ','control','stop ','neck ','shoulders','dandruff issue ','resolve ','not buy ','pigmentation','pathetic','bekar','quality','bakwas ','match','worthless','ruin','doent work ','mask ','serum ','conditioner ','shampoo ','cream ','moisturizer ','wrinkle','fine line ','nothing ','scars ','significant ','ingredient','notice ','hair grown','brighten ','garbage','bald','trust ','expectation','dislike']
    Packagingw = ['leakage','broken','defective','pack','packaging','dispense ','poor dispenser','difficult to use ','box ','wrong product ','misleading size','underfilled','damage ','plastic ','glass','material ','delivery ','shipping ','expired ','container ','late','delayed ','bottle ','cap ','seal ','pump ','tube ','package ','spill','on time ','missing item ','fake product ','false product ','missing ','service ','spray ','nozzle','lid ','arrived ','not original','expire']
    Pricingw = ['finishes fast ','affordable ','money ','value ','worth ','price ','cost ','expensive ','over priced ','amount ','costly','waste of money','deal','investment','expected more ','less quantity','smaller size ','budget ','pocket ','premium','offer ','bucks ','high price','charge']    
    Fragnancew = ['smell','strong','overpowering','artificial','chemical','fragrance ','long lasting ','unpleasant','foul','fades ','scent','heavy','sick','stale','rotten','pungent']    
    Texturew = ['greasy ','patchy ','thick','thin ','sticky','heavy','oily','lightweight','creamy','lumpy','watery','patchy','white cast ','hard to spread','uneven','dry','chalky','cakey','runny','absorb','spread ','heavy ','consistency ','sticky ','feel on skin ','texture ','gel','cream','rubbery ','lumps ','blend','layer']
    Colorw = ['colour ','color ','light ','dark color ','uneven ','changed colour','shade ','milky ','transperent ','faded']
    Sideeffectw = ['acne ','pimple ','suit ','breakout ','irritate ','dull ','dark skin','allergy ','black','thinning ','burning','itchy','rashes','redness','breakouts','acne','clogging','allergic','peeling','irritating','sticky','overheating','greasy','flaky','swelling','painful','stinging','hair fall','tingling','tightness','super dry','roughness','patches','scalp pain','headache ','side effect ','severe ','dermatitis','bumps ','hair loss','destroy','sweat ','red dots ']
    
    Efficacy = 0
    Packaging = 0
    Pricing = 0
    Fragnance = 0
    Texture = 0
    Sideeffect = 0
    Color = 0
    l = review.split(" ")
    for r in l:
        for keyword in Efficacyw:
            if fuzz.partial_ratio(r, keyword) > 80:
                Efficacy = 1
                break

        for keyword in Packagingw:
            if fuzz.partial_ratio(r, keyword) > 80:
                Packaging = 1
                break

        for keyword in Pricingw:
            if fuzz.partial_ratio(r, keyword) > 80:
                Pricing = 1
                break

        for keyword in Fragnancew:
            if fuzz.partial_ratio(r, keyword) > 80:
                Fragnance = 1
                break

        for keyword in Texturew:
            if fuzz.partial_ratio(r, keyword) > 80:
                Texture = 1
                break

        for keyword in Sideeffectw:
            if fuzz.partial_ratio(r, keyword) > 80:
                Sideeffect = 1
                break

        for keyword in Colorw:
            if fuzz.partial_ratio(r, keyword) > 80:
                Color = 1
                break

    return Efficacy,Packaging,Pricing,Fragnance,Texture,Sideeffect,Color
def write_to_gbq(df,p_id,t_id):
    pgbq.to_gbq(df, t_id, project_id=p_id, if_exists='append')  # use 'replace', 'append', or 'fail' as needed

def read_from_gbq(p_id,t_id):
    sql = f"""
        select 
          *
          FROM `{p_id}.{t_id}` 
        """
    df = pgbq.read_gbq(sql, project_id=p_id)
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


def past_table_join(past_table,final_df):
    pass


def main():
    home_page = "https://www.amazon.in/"
    username = Variable.get("AMAZON_LOGIN_MAIL")
    password = Variable.get("AMAZON_LOGIN_PASSWORD") #check
    project_id = 'shopify-pubsub-project'
    table_top_25 = 'Amazon_Market_Sizing.Top_products_for_review_scraping'
    
    df = read_from_gbq(project_id, table_top_25)
    print(df.columns)
    print(len(df))
    print('data read from gbq sucessfully') 
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
    start_index =0 
    end_index = 25
    batch_df = df.iloc[start_index:end_index]
    processed_df = Login_AMZ(home_page, username, password, batch_df)  
    final_df = datatype_normalizing(processed_df, Review_col_dt) 
    final_df['Scraped_date1'] = datetime.date.today()  
    final_df['Scraped_date1'] = pd.to_datetime(final_df['Scraped_date1']) 
    final_df['Efficacy1'], final_df['Packaging1'],final_df['Pricing1'],final_df['Fragnance1'],final_df['Texture1'],final_df['Sideeffect1'],final_df['Color1'] = zip(*final_df['Review1'].apply(lambda x: check_keywords(x))) 
    print('bucketization done') 

    # Creating A Seperate Rating Dataframe
    Rating_df_columns = ['Product_Title1', 'SKU1', 'ASIN1', 'Scraped_date1', 'Rating1', 'No_of_ratings1', 'No_of_reviews1', 'Avg_Rating1']
    final_df_rating = final_df[Rating_df_columns]

    # Group by 'ASIN1', 'Scraped_date1', 'Rating1' and aggregate other columns
    grouped_final_df_rating = final_df_rating.groupby(['ASIN1', 'Scraped_date1', 'Rating1'], as_index=False).agg({
    'Product_Title1': 'first',   # Take the first 'Product_Title1' in each group
    'SKU1': 'first',             # Take the first 'SKU1' in each group
    'No_of_ratings1': 'mean',    # Calculate the mean for 'No_of_ratings1'
    'No_of_reviews1': 'mean',    # Calculate the mean for 'No_of_reviews1'
    'Avg_Rating1': 'mean'  })      # Calculate the mean for 'Avg_Rating1'

    # Reorder the columns to ensure 'Product_Title1' and 'SKU1' appear first
    grouped_final_df_rating = grouped_final_df_rating[['Product_Title1', 'SKU1', 'ASIN1', 'Scraped_date1', 'Rating1', 'No_of_ratings1', 'No_of_reviews1', 'Avg_Rating1']] 
    grouped_final_df_rating.columns = ['Product_Title', 'SKU', 'ASIN', 'Scraped_date', 'Rating', 'No_of_ratings', 'No_of_reviews', 'Avg_Rating']

    
    # Creating A Seperate Review Dataframe
    Review_df_columns = ['Review1','Date_R1','Names1','Page_url1','Star1','Product_Title1','SKU1','ASIN1','Scraped_date1','Efficacy1','Packaging1','Pricing1','Fragnance1','Texture1','Sideeffect1','Color1']
    final_df_review = final_df[Review_df_columns]  
    final_df_review['Date_R1'] = final_df_review['Date_R1'].astype('datetime64[ns]') 

    project_id = 'shopify-pubsub-project'
    rating_df_write = 'Project_Goonj_asia.AMZ_Ratings_Top_products' 
    review_df_write = 'Project_Goonj_asia.AMZ_Reviews_Top_products' 
    past_review_df = 'Project_Goonj_asia.AMZ_Reviews_Top_products' 
    
    
    past_table = read_from_gbq(project_id,past_review_df) 
    merged_df = pd.merge(final_df_review,past_table, 
                    left_on=['Names1','Review1','SKU1'], right_on=['Names','Review','SKU'],
                    how='left') 
    print('join done')
    merged_df = merged_df[merged_df['Review1'] != 'nan']
    merged_df = merged_df[merged_df['Review1'] != ""] 
    filtered_df = merged_df[merged_df['SKU'].isna()] 
    filtered_df = filtered_df[Review_df_columns] 
    Review_df_columns = ['Review', 'Date_R', 'Names', 'Page_url', 'Star', 'Product_Title', 'SKU', 'ASIN', 'Scraped_date', 'Efficacy', 'Packaging', 'Pricing', 'Fragnance', 'Texture', 'Sideeffect', 'Color']
    filtered_df.columns = Review_df_columns 

    print(len(filtered_df))
    write_to_gbq(grouped_final_df_rating,project_id,rating_df_write)  
    write_to_gbq(filtered_df,project_id,review_df_write)

main()
