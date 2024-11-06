# Instagram Scraper Script
# Required imports
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait
from selenium.common.exceptions import NoSuchElementException, TimeoutException
import time
import requests
from bs4 import BeautifulSoup
import re
import config
import json
import os
from urllib.parse import urlparse
import csv

# Initialize Selenium WebDriver
def initialize_driver():

    options = webdriver.ChromeOptions()
    #options.add_argument('--headless')  # Run in headless mode (runs in invisible/background mode without opening a window)

    # adding headers to prevent scraper from being blocked
    options.add_argument(user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.6723.70 Safari/537.36")
    options.add_argument("accept-language=en-US,en;q=0.9")
    options.add_argument("referer=https://www.google.com/")   

    # Initialize Chrome with the correct driver and options
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
    return driver

# Login to Instagram
def login(driver):
    driver.get('https://www.instagram.com/')
    username = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.CSS_SELECTOR, "input[name='username']")))
    password = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.CSS_SELECTOR, "input[name='password']")))
    username.clear()
    username.send_keys(config.username)
    password.clear()
    password.send_keys(config.password)
    WebDriverWait(driver, 2).until(EC.element_to_be_clickable((By.CSS_SELECTOR, "button[type='submit']"))).click()
    WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, '//button[contains(text(), "Not Now")]'))).click()

# Search for a user or keyword
def search(driver, keyword):
    searchbox = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, "//input[@placeholder='Search']")))
    searchbox.clear()
    searchbox.send_keys(keyword)
    time.sleep(2)
    if keyword.startswith('@'):
        keyword = keyword[1:]
    first_result = driver.find_element(By.XPATH, f'//span[text()="{keyword}"]')
    first_result.click()

# Scroll to load all posts
def scroll_posts(driver):
    soups = []
    initial_height = driver.execute_script('return document.body.scrollHeight')
    while True:
        driver.execute_script('window.scrollTo(0, document.body.scrollHeight);')
        time.sleep(5)
        html = driver.page_source
        soups.append(BeautifulSoup(html, 'html.parser'))
        current_height = driver.execute_script('return document.body.scrollHeight')
        if current_height == initial_height:
            break
        initial_height = current_height
    return soups

# Extract post URLs
def extract_post_urls(soups):
    post_urls = [anchor['href'] for soup in soups for anchor in soup.find_all('a', href=True) if anchor['href'].startswith(('/p/', '/reel/'))]
    return list(set(post_urls))

# Scrape JSON data from posts
def scrape_json_data(driver, post_urls):
    json_list = []
    for url in post_urls:
        try:
            # issue: request parameter (?__a=1&__d=dis) is being dynamically updated periodically.
            # not able to access individual posts in order to download video, scrape captions, comments.
            modified_url = 'https://www.instagram.com/' + url + '?__a=1&__d=dis' 
            driver.get(modified_url)
            time.sleep(1)
            pre_tag = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.XPATH, '//pre')))
            json_script = pre_tag.text
            json_list.append(json.loads(json_script))
        except (NoSuchElementException, TimeoutException, json.JSONDecodeError):
            pass
    return json_list

# Download media from URLs
def download_media(json_list, keyword):
    os.makedirs(keyword, exist_ok=True)
    image_dir = os.path.join(keyword, 'images')
    video_dir = os.path.join(keyword, 'videos')
    os.makedirs(image_dir, exist_ok=True)
    os.makedirs(video_dir, exist_ok=True)
    for json_data in json_list:
        for item in json_data.get('items', []):
            download_content(item, image_dir, video_dir)

# Helper function to download each item
def download_content(item, image_dir, video_dir):
    media = item.get('carousel_media', []) or [item]
    for media_item in media:
        url = media_item.get('image_versions2', {}).get('candidates', [{}])[0].get('url') or media_item.get('video_versions', [{}])[0].get('url')
        if url:
            download_file(url, image_dir if 'image' in url else video_dir)

# Function to download each file
def download_file(url, directory):
    response = requests.get(url, stream=True)
    file_name = os.path.join(directory, os.path.basename(urlparse(url).path))
    with open(file_name, 'wb') as file:
        for chunk in response.iter_content(chunk_size=8192):
            file.write(chunk)
    print(f'Downloaded: {file_name}')

# Main function
def main(keyword='@sample-handle'):
    driver = initialize_driver()
    try:
        login(driver)
        search(driver, keyword)
        soups = scroll_posts(driver)
        post_urls = extract_post_urls(soups)
        json_list = scrape_json_data(driver, post_urls)
        download_media(json_list, keyword)
    finally:
        driver.quit()

# Run script
if __name__ == '__main__':
    main()