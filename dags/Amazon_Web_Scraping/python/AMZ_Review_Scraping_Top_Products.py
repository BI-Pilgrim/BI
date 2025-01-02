import requests
import csv
from selenium import webdriver
import pandas as pd
import time
from selenium.webdriver.common.keys import Keys
from time import sleep
import pandas_gbq as pgbq
import nltk
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from nltk.stem import PorterStemmer
import datetime
from fuzzywuzzy import fuzz
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

def Login_AMZ(homepage, driver, username, password,df):

    driver.get(homepage)
    # Find the sign-in button and click it
    time.sleep(1)
    
    sign_in_button = driver.find_element('xpath', './/a[@id="nav-link-accountList"]') 
    sign_in_button.click()
    time.sleep(1)
    
    # Find the email/phone number field and enter your credentials
    email_field = driver.find_element('xpath', './/input[@id="ap_email"]') 
    email_field.send_keys(username)
    time.sleep(1)
    
    # Find the continue button and click it
    continue_button = driver.find_element('xpath', './/input[@id="continue"]')
    continue_button.click()
    time.sleep(1)
    
    # Find the password field and enter your password
    password_field = driver.find_element('xpath', './/input[@id="ap_password"]')
    password_field.send_keys(password)
    time.sleep(1)
    
    # Find the sign-in button and click it
    sign_in_button = driver.find_element('xpath', './/input[@id="signInSubmit"]')
    sign_in_button.click()
    time.sleep(1)
    
    op_df = ratings_count(driver,df)
    account_menu = driver.find_element(By.ID, "nav-link-accountList")
    actions = ActionChains(driver)
    actions.move_to_element(account_menu).perform()
    logout_link = WebDriverWait(driver, 10).until(
    EC.element_to_be_clickable((By.ID, "nav-item-signout")))
   
    logout_link.click()
   
    time.sleep(1)
    return op_df


def scrape_page(url,driver):
    reviews = []
    ratings = []
    dates = []
    names = []
    review_blocks = driver.find_elements('xpath', '//div[@id="cm_cr-review_list"]//div[@class="a-section a-spacing-top-large a-text-center no-reviews-section"]')    
   
    
    if len(review_blocks) != 0:
        return reviews, ratings, dates, names
    
    else:
        review_blocks = driver.find_elements('xpath', '//li[@class="review aok-relative"]')
        for block in review_blocks:
            
            try:
                rating_element = block.find_element('xpath', './/span[@class="a-icon-alt"]')
                rating = float(rating_element.get_attribute("innerHTML").split()[0].replace(',', '.'))
            except :
                print('rating element not found for the url-----',url)
                
                
            review_element = block.find_element('xpath', './/div[@class="a-row a-spacing-small review-data"]')
            date = block.find_element('xpath', './/span[@data-hook="review-date"]')
            
            name = block.find_element('xpath', './/span[@class="a-profile-name"]')
            
            if review_element:
                reviews.append(review_element.text.strip())
                
            if name:
                names.append(name.text.strip())

            if rating:
                ratings.append(rating)

            if date:
                date_f=date.text.strip().split(' on ')[1]
                dates.append(date_f)


    return reviews, ratings, dates, names

def scrape_multiple_pages(driver,base_url, num_pages):
    all_reviews = []
    all_ratings = []
    all_dates = []
    all_names = []
    page_url = []
    rat_rev_cnt =[]
    
    for i in range(1, num_pages + 1):
        inp = base_url+'&pageNumber='+str(i)
        driver.get(inp)
        reviews, ratings, dates, names = scrape_page(inp,driver)
        if (len(reviews)==0):
            break

        all_reviews.extend(reviews)
        all_ratings.extend(ratings)
        all_dates.extend(dates)
        all_names.extend(names)
        page_url.extend([inp]*len(reviews))
        time.sleep(1)
    return all_reviews, all_ratings, all_dates, all_names, page_url


def ratings_count(driver,df):
    num_pages = 10
    df_combined = pd.DataFrame({})
    for index, row in df.iterrows():
        amazon_url = row['amazon_url']
        print(amazon_url)
        sku = row['sku'] 
        asin = row['asin']
        title = row['title']
        sku_det = [sku,asin,title]
        rat_rev_cnt = []
        avg_rating = []
        star = ['one_star','two_star','three_star','four_star','five_star']
        df_all_stars = pd.DataFrame({})
    
        for s in star:        
            url = amazon_url+'&filterByStar='+s
            
            driver.get(url)
            time.sleep(3)
            rating_counts = driver.find_element('xpath', '//div[@id="filter-info-section"]//div[@data-hook="cr-filter-info-review-rating-count"]')
            print(f'rating_counts for {s} star',rating_counts.text)
            
            avg_star = driver.find_element('xpath', '//span[@data-hook="rating-out-of-text"]')
        
            avg_rating = float(avg_star.text.split(' ')[0])
            rat_rev_cnt = rating_counts.text.split(' ')
            no_of_ratings = rat_rev_cnt[0].replace(',','')
            no_of_reviews = rat_rev_cnt[3].replace(',','')
            
            reviews, ratings, dates, names, page_URL = scrape_multiple_pages(driver, url, num_pages)
            
            df = pd.DataFrame({'Review1': reviews, 'Rating1': ratings, 'Date1': dates, 'Names1':names, 'Page_url1':page_URL})
            df_star = pd.DataFrame({'Star1':[s]*len(df),'No_of_ratings1':[no_of_ratings]*len(df),
                                    'No_of_reviews1':[no_of_reviews]*len(df),'Avg_Rating1':[avg_rating]*len(df)})
            
            df =pd.concat([df,df_star],axis=1)
            df_all_stars =pd.concat([df_all_stars,df])
            
        df_new_cols = pd.DataFrame({'Product_Title1': [title] * len(df),
                                    'SKU1': [sku] * len(df),
                                    'ASIN1': [asin] * len(df)})
        df_all_stars = pd.concat([df_all_stars, df_new_cols],axis=1)
        df_combined = pd.concat([df_combined,df_all_stars])
    
    return df_combined

def datatype_normalizing(mtdf,datatypes):
        
    for col, dtype in datatypes.items():
        if col in mtdf.columns:
            try:
                print(f"Converting column '{col}' to {dtype}.")
                mtdf[col] = mtdf[col].astype(dtype)
            except ValueError as e:
                print(f"Error converting column '{col}' to {dtype}: {e}")
    return mtdf


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
    

def past_table_join(past_table,final_df):
    New_reviews = final_df
    
#     merged_df = final_df.merge(past_table, left_on=['Rating1','Names1','ASIN1'],right_on=['Rating','Names','ASIN'], how='left')

#     New_reviews = merged_df[merged_df['Review'].isna()]
#     New_reviews = New_reviews.drop(['Review','Rating','Date_R','Names','Page_url','Star','No_of_ratings','No_of_reviews','Product_Title','SKU','ASIN','Avg_Rating'], axis=1)
    New_reviews = New_reviews.rename(columns={'Review1': 'Review','Rating1':'Rating','Date1':'Date_R','Names1':'Names','Page_url1':'Page_url',
                                              'Star1':'Star','No_of_ratings1':'No_of_ratings','No_of_reviews1':'No_of_reviews','Scraped_date1':'Scraped_date',
                                              'Product_Title1':'Product_Title','SKU1':'SKU','ASIN1':'ASIN','Avg_Rating1':'Avg_Rating'})
    return New_reviews


def delete_latest_month(p_id,t_id,date_col):
    sql = f"""
            DELETE FROM `{p_id}.{t_id}`
            WHERE
            DATE_TRUNC(date({date_col}), MONTH) >= DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 2 MONTH), MONTH)
        """
    df = pgbq.read_gbq(sql, project_id=p_id)
    print("Done Deleting the latest month data from the Rating review")


# Function to clean and tokenize text
def clean_text(text):
    # Create a PorterStemmer object
    stemmer = PorterStemmer()
    words = word_tokenize(text)
    words = [word.lower() for word in words if word.isalpha()]
    words = [stemmer.stem(word) for word in words if word not in stopwords.words('english')]
    return words

def extract_top_keywords(df, review_column):
    # Download NLTK data if not already downloaded
    nltk.download('stopwords')
    nltk.download('punkt')

    # Apply cleaning and tokenization to the review column
    df['cleaned_reviews'] = df[review_column].apply(clean_text)

    # Create a list of all words
    all_words = [word for review in df['cleaned_reviews'] for word in review]

    # Create a word frequency dictionary
    word_freq = pd.Series(all_words).value_counts()

    # Get the top 5 most frequent words
    top_5_words = word_freq.head(5)

    # Add a new column to the DataFrame
    df['top_keywords'] = df['cleaned_reviews'].apply(lambda x: ', '.join(x[:5]))

    return df

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



home_page = "https://www.amazon.in/"
username = "005.kshafiq@gmail.com"
password = "(He11o)"
project_id = 'shopify-pubsub-project'
table_review = 'Amazon_Market_Sizing.AMZ_Rating_Reviews_Top_products'
table_top_25 = 'Amazon_Market_Sizing.Top_products_for_review_scraping'
table_bad_review = 'Amazon_Market_Sizing.AMZ_Bad_Review_Bucketing'


Review_col_dt = {
    'Review1':'str',
    'Rating1':'float',
    'Date1':'datetime64[ns]',
    'Names1':'str',
    'Page_url1':'str',
    'Star1':'str',
    'No_of_ratings1':'float',
    'No_of_reviews1':'float',
    'Product_Title1':'str',
    'SKU1':'str',
    'ASIN1':'str',
    'Avg_Rating1':'float64',
    'Scraped_date1':'datetime64[ns]',
}

bad_review_col = {
    'SKU':'str',
    'Month_R':'datetime64[ns]',
    'No_of_reviews':'float64',
    'Review':'str',
    'cleaned_reviews':'str',
    'top_keywords':'str',
    'Avg_Rating':'float64',
    'Product_Title':'str',
    'Scraped_date':'datetime64[ns]',
    'Efficacy':'int',
    'Packaging':'int',
    'Delivery':'int',
    'Pricing':'int',
    'Fragnance':'int',
    'Texture':'int',
    'Sideeffect':'int',
    'Color':'int',
}

print(pd.Timestamp.now())

df = read_from_gbq(project_id,table_top_25)
# Replace with your preferred WebDriver

batch_size = 5
# Iterate over the DataFrame in batches
result_dfs = []
for i in range(0, len(df), batch_size):
    start_index = i
    end_index = min(i + batch_size, len(df))
    batch_df = df.iloc[start_index:end_index]
    driver = webdriver.Firefox() 
    processed_df = Login_AMZ(home_page, driver, username, password,batch_df)
    driver.close()
    result_dfs.append(processed_df)
    time.sleep(2)
    

# Concatenate all processed DataFrames
df_combined = pd.concat(result_dfs, ignore_index=True)

# After getting the reviews for top products for all the stars and pages writing it to the CSV file
df_combined.to_csv('review_output.csv',index=False)

df_combined['Scraped_date1'] = datetime.date.today()

# Normalizing the df to write in GBQ
final_df = datatype_normalizing(df_combined,Review_col_dt)

# Deleting the latest month data from Rating Review Table
delete_latest_month(project_id,table_review,'Date_R')

# Deleting the latest month data from Summary Table
delete_latest_month(project_id,table_bad_review,'Month_R')


# Reading the Past data from GBQ 
past_table = read_from_gbq(project_id,table_review)
  
# Joining both the past and current month data 
New_reviews = past_table_join(past_table,final_df)

# Bucketization
New_reviews['Efficacy'], New_reviews['Packaging'],New_reviews['Pricing'],New_reviews['Fragnance'],New_reviews['Texture'],New_reviews['Sideeffect'],New_reviews['Color'] = zip(*New_reviews['Review'].apply(lambda x: check_keywords(x)))

New_reviews.to_csv('New_Reviews.csv',index = False)
# Appending the latest months summary in the GBQ summary table
write_to_gbq(New_reviews,project_id,table_review)
print("Customer-product level review written in Review table")

# Extracting Month from date
New_reviews['Month_R'] = New_reviews['Date_R'].dt.to_period('M').dt.to_timestamp()
New_reviews = New_reviews[New_reviews['Rating'] < 4]
# Grouping the reviews at SKU, STAR, RATING, and MONTH level
grouped_df = New_reviews.groupby(['SKU','Product_Title','Month_R','Scraped_date']).agg({
    'Rating': 'count',
    'Avg_Rating': 'mean',
    'Efficacy':'sum',
    'Packaging':'sum',
    'Pricing':'sum',
    'Fragnance':'sum',
    'Texture':'sum',
    'Sideeffect':'sum',
    'Color':'sum',
    'Review': lambda x: '<-->'.join(str(item) for item in x)
}).reset_index()

# Create a wordcloud frequency count of the words
word_cloud = extract_top_keywords(grouped_df, 'Review')

# Normalizing the word cloud df to write in GBQ
Normalize_wc = datatype_normalizing(word_cloud,bad_review_col)

# Appending the latest months summary in the GBQ summary table
write_to_gbq(Normalize_wc,project_id,table_bad_review)
print("Product_month level review written in summary table")
Normalize_wc.to_csv('Normalize_wc.csv',index = False)
print(pd.Timestamp.now())
