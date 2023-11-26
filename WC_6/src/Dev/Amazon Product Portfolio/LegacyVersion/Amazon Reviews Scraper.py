# -*- coding: utf-8 -*-
"""
Created on Wed Jul 26 11:38:39 2023

@author: bfernandez034
"""

import pandas as pd
import time
from tqdm import tqdm
from bs4 import BeautifulSoup
from collections import defaultdict
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from random import randint
import undetected_chromedriver as uc
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.wait import WebDriverWait

browser = uc.Chrome(use_subprocess=True)
#change .de for country
website="https://www.amazon.com"
#change _de for country
urls = pd.read_excel(r"C:/Users/julens001/OneDrive - pwc/Project Clear Skies/Inputs Scraper/Amazon_ProductList.xlsx")["review url"].tolist()
urls = [x for x in urls if str(x)!="nan"]
browser.get(website)
browser.set_page_load_timeout(20)
#IMPORTANT!!!
#It asks for a CAPTCHA + Accept cookies + Log in to your account 
#+ set location to New York 10001 + Language to EN

#%%
#SET START AND END
start = 0
end = len(urls)
urls = urls[start:end]
mode = "by_star" # mode can be "top" or "by_star"
urls = [x+"&formatType=current_format" for x in urls]
if mode == "by_star":
    suffixes = ["one_star","two_star","three_star","four_star","five_star"]
    urls=[x+"&filterByStar="+y for x in urls for y in suffixes]
else:
    pass

#

reviews_dict = defaultdict(list)
reviews_backup = defaultdict(list)
i = 0
j=0

for url in tqdm(urls):
    try:
        browser.get(url)
    except:
        print("check this url that may have failed: ", url)
        
    #Scroll to the bottom
    for k in range(200):
        browser.find_element("xpath", '*').send_keys(Keys.DOWN)
        
    while True:
        time.sleep(randint(2,4))

        parsed = BeautifulSoup(browser.page_source,"html.parser")
        reviews = parsed.find_all("div",{"data-hook":"review"})
        url_product = url.split("&formatType=")[0]
        j = j + 1
        reviews_backup["url_product"].append(url_product)
        reviews_backup["page"].append(j)
        reviews_backup["HTML"].append(browser.page_source)
        for review in reviews:
            i = i + 1
            try:
                rating = review.find("i",{"data-hook":"review-star-rating"}).text.replace(" out of 5 stars","")
            except:
                try:
                    rating = review.find("i",{"data-hook":"cmps-review-star-rating"}).text.replace(" out of 5 stars","")
                except:
                    rating = ''
            try:
                date = review.find("span",{"data-hook":"review-date"}).text.split(" on ")[1]
            except:
                date = ''
            try:
                location = review.find("span",{"data-hook":"review-date"}).text.split(" on ")[0].replace("Reviewed in ","")
            except:
                location = ''
            try:
                user_profile = review.find("a",{"class":"a-profile"})["href"]
            except:
                user_profile = ''
            try:
                title = review.find("a",{"data-hook":"review-title"}).find("span",{"class":"cr-original-review-content"}).text
            except:
                try:
                    title = review.find("a",{"data-hook":"review-title"}).text.split("stars")[1].strip()
                except:
                    try:
                        title = review.find("span",{"data-hook":"review-title"}).find("span",{"class":"cr-original-review-content"}).text
                    except:
                        try:
                            title = review.find("span",{"data-hook":"review-title"}).text
                        except:
                            title = ''
            try:
                text = review.find("span",{"data-hook":"review-body"}).find("span",{"class":"cr-original-review-content"}).text
            except:
                try:
                    text = review.find("span",{"data-hook":"review-body"}).text.strip()
                except:
                    text = ''
           

            
            reviews_dict["ID_review"].append(i)
            reviews_dict["url_product"].append(url_product)
            reviews_dict["url_with_suffix"].append(url)
            reviews_dict["title"].append(title)
            reviews_dict["text"].append(text)
            reviews_dict["date"].append(date)
            reviews_dict["location"].append(location)
            reviews_dict["rating"].append(rating)
            reviews_dict["user_profile"].append(user_profile)
            
            
        try:
            browser.find_element(By.XPATH,"//li[@class='a-last']").click()
        except:
            j = 0
            break
        if j==11:
            break

reviews_df = pd.DataFrame(reviews_dict).drop_duplicates(subset=["url_product","url_with_suffix","title","text","date","location","rating","user_profile","flavour_size"])
backup = pd.DataFrame(reviews_backup)
#change _de for country
reviews_df.to_excel(fr'C:/Users/julens001/OneDrive - pwc/Project Clear Skies/Reviews Output/Amazon_Reviews.xlsx')
time.sleep(10)
backup.to_csv(fr'C:\Users\julens001\OneDrive - pwc\Project Clear Skies\Reviews Output\Backup_amazon_reviews_{mode}_{start}_{end}.csv')


##Flashea las fechas llegan a otra columna
##Levantar el texto entero no solo lo de adentro