# -*- coding: utf-8 -*-
"""
Created on Wed Aug  9 16:31:50 2023

@author: julens001
"""

from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
import pandas as pd
from bs4 import BeautifulSoup
from collections import defaultdict
import re
from tqdm import tqdm
import time
from random import randint
import undetected_chromedriver as uc

browser = uc.Chrome(use_subprocess=True)
website="https://www.amazon.com"
url = "https://www.amazon.com/stores/page/1CE3F71E-D238-41FC-B822-8B23993005A0?ingress=0&visitId=3f59e065-fbc4-4178-b3f7-dadf961915d8"
browser.get(website)
browser.set_page_load_timeout(20)

browser.refresh()
time.sleep(5)

##Logs into Account
browser.find_element(By.ID , "nav-link-accountList-nav-line-1").click()

username = browser.find_element(By.ID , "ap_email")

username.send_keys("juan.pablo.jose.ulens@pwc.com") ##Here goes the email account
continue_button = browser.find_element(By.ID, "continue")
continue_button.click()

password = browser.find_element(By.ID , "ap_password")
password.send_keys("Amaz+05Ju") ##Here goes the password
login_button = browser.find_element(By.ID, "signInSubmit")
login_button.click()

browser.refresh()
time.sleep(5)

###Scraper starts

browser.get(url)

soup = BeautifulSoup(browser.page_source, "html.parser")
products_url = [x.find('a').get('href') for x in soup.find_all("div", class_ = "EditorialTile__innerContent__n92i8")] 

products_price = [x.find('span', class_ = "EditorialTileProduct__price__CeZLD").text if x.find('span', class_ = "EditorialTileProduct__price__CeZLD") is not None else "" for x in soup.find_all("div", class_ = "EditorialTile__innerContent__n92i8")] 

product_list = []

for p in products_url:
    browser.get(website + p)
    soup = BeautifulSoup(browser.page_source, "html.parser")
    
    ##Gets Prod Name
    product_name = soup.find("div",{"id":"titleSection"}).find("span", {"id":"productTitle"}).text.strip()
    
    ##Gets Prod Rating
    rating = str([x.find("span", class_ = "reviewCountTextLinkedHistogram noUnderline").text for x in soup.find_all("div",{"id":"averageCustomerReviews"})])[12:-12]
    if rating == '':
        rating = "no rating included"
    else:
        rating = rating
    
    ##Gets Number of Reviews
    n_rating = str([x.find("span", {"id" : "acrCustomerReviewText"}).text for x in soup.find_all("div",{"id":"averageCustomerReviews"})])[2:-2]
    if n_rating == '':
        n_rating = "no rating included"
    else:
        n_rating = n_rating
        
    brand = soup.find("div", class_ = "a-section a-spacing-small a-spacing-top-small").find("span", class_ = "a-size-base po-break-word").text.strip()
    category = soup.find("a", class_ = "a-link-normal a-color-tertiary").text.strip()
    
##'price': price, 'original price': original_price,    
    
    prod_item = {'brand': brand,'category': category, 'product name': product_name, 'product url': website + p, 'rating': rating, 'rating amount': n_rating}
    product_list.append(prod_item)


##Removing any duplicates that might have been generated    
unique_list = []
for prod in product_list:
    if prod not in unique_list:
        unique_list.append(prod)

product_list = unique_list






