# -*- coding: utf-8 -*-
"""
Created on Fri Jul 28 11:16:28 2023

@author: fbarreto010
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
from selenium import webdriver
import selenium.webdriver as webdriver



browser = uc.Chrome(use_subprocess=True) ##use_subprocess=True para uc.Chrome hay que tener cuidado con las updates
website="https://www.amazon.com"
urls = ['https://www.amazon.co.uk/stores/page/BE0B08DB-4583-4C8A-A689-CDE644FC7629?ingress=0&visitId=53b3f803-1726-4a1a-88ea-720b07c22064']
browser.set_page_load_timeout(20)
# Accept CAPTCHA + Set up address in country

browser.refresh()
time.sleep(5)

##Logs into Account
browser.find_element(By.ID , "nav-link-accountList-nav-line-1").click()

username = browser.find_element(By.ID , "ap_email")

username.send_keys("ariel.hernan.limes@pwc.com") ##Here goes the email account
continue_button = browser.find_element(By.ID, "continue")
continue_button.click()

password = browser.find_element(By.ID , "ap_password")
password.send_keys("CompraLicencias") ##Here goes the password
login_button = browser.find_element(By.ID, "signInSubmit")
login_button.click()

browser.refresh()
time.sleep(5)


#%%
product_list = []
i=0
j=0

for url in tqdm(urls):
    try:
        browser.get(url)
    except:
        print("check this url that may have failed: ", url)


    while True:
        time.sleep(randint(2,4))
        #Scroll to the bottom
        soup = BeautifulSoup(browser.page_source, "html.parser")
        brand = soup.find("h1",{"class":"Breadcrumbs__breadcrumb__M5DAz"}).text
        category = soup.find("span",{"class":"Breadcrumbs__breadcrumb__M5DAz"}).text
        
        for k in range(20):
            browser.find_element("xpath", '*').send_keys(Keys.PAGE_DOWN)
            time.sleep(1.5)
        soup = BeautifulSoup(browser.page_source, "html.parser")
        
        products = soup.find_all("li",{"class":"ProductGridItem__itemOuter__KUtvv ProductGridItem__fixed__DQzmO"})
        
        
        j = j + 1
            
        for p in products:
                i = i + 1
                #ranking = p.find("div",{"class":"a-section zg-bdg-body zg-bdg-clr-body aok-float-left"}).find("span",{"class":"zg-bdg-text"}).text
                try:
                    product_name = p.find("div",{"class":"Title__truncateTitle__DGaow"}).find("a",{"class":"Title__title__z5HRm Title__fixed__bJ2c2"}).text
                except:
                    product_name = p.find("div",{"class":"_cDEzb_p13n-sc-css-line-clamp-4_2q2cc"}).text
                url_prod = "https://www.amazon.com" + p.find("div",{"class":"Title__truncateTitle__DGaow"}).find("a",{"class":"Title__title__z5HRm Title__fixed__bJ2c2"})["href"]
                #sumarle el base url adelante
                try:
                    rating = p.find("div",{"class":"ProductGridItem__rating__o7nge"}).find("span",{"class":"Icon__icon__alt-text__z_0FG"}).text
                except:
                    rating = "no rating included"
                try:
                    n_rating = p.find("div",{"class":"ProductGridItem__rating__o7nge"}).find("span",{"class":"ProductGridItem__reviewCount__laMDa"}).text
                except:
                    n_rating = "no rating included"
                try:
                    price = p.find("div",{"data-testid":"grid-item-buy-price"}).text
                except:
                    price = ""
                try:
                    original_price = p.find("div",{"data-testid":"grid-item-buy-price"}).find("span", {"class":"Price__price__LKpWT Price__small__Y4NDm StrikeThroughPrice__strikePrice__stBvh Price__strikethrough__lQg8R Price__fixedSize__jmsXS"}).text
                except:
                    original_price = ""
                
                prod_item = {'brand': brand,'category': category, 'product name': product_name, 'product url': url_prod, 'price': price, 'original price': original_price,'rating': rating, 'rating amount': n_rating}
                product_list.append(prod_item)
               
        try:
            browser.find_element(By.XPATH,"//li[@class='a-last']").click()
        except:
            j = 0
            break

df = pd.DataFrame(product_list)
df.to_excel(r"C:\Users\julens001\OneDrive - pwc\Project Clear Skies\Inputs Scraper\Amazon_ProductList_v1.xlsx")

#%%

urls2 = pd.read_excel(r"C:\Users\julens001\OneDrive - pwc\Project Clear Skies\Inputs Scraper\Amazon_ProductList_v1.xlsx")["product url"].tolist()

product_info = []
missing_urls = []

for url in tqdm(urls2):
    try:
        browser.get(url)
    except:
        missing_urls.append(url)
        
    time.sleep(randint(1,2))
    parsed = BeautifulSoup(browser.page_source,"html.parser")
    
    try:
        product_title = parsed.find("div",{"id":"titleSection"}).find("h1",{"id":"title"}).text.strip()
    except:
        product_title = ""
    try:
        review_url = browser.find_element(By.XPATH, value = '//a[@data-hook="see-all-reviews-link-foot"]').get_attribute('href')
    except:
        review_url = ""
    
    
    prod_item = {'product url': url, 'product title': product_title, 'review url': review_url}
    product_info.append(prod_item)

product_df = pd.DataFrame(product_info)
product_df.to_excel(r'C:\Users\julens001\OneDrive - pwc\Project Clear Skies\Inputs Scraper\Amazon_ProductList_v2.xlsx', index=False)


#%%
df3 = df.merge(product_df,left_on = "product url", right_on ="product url")
df3.to_excel(r'C:\Users\julens001\OneDrive - pwc\Project Clear Skies\Inputs Scraper\Amazon_ProductList_v1.xlsx', index=False)

