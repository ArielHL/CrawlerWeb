# -*- coding: utf-8 -*-
"""
Created on Fri Jul 28 11:16:28 2023

@author: fbarreto010
"""

from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
import pandas as pd
from bs4 import BeautifulSoup
from tqdm import tqdm
import time
from random import randint
import undetected_chromedriver as uc




browser = uc.Chrome()
website="https://www.amazon.nl"
urls = pd.read_excel(r"C:/Users/DE139204/Downloads/TopProducts Amazon PL and NL.xlsx")["Link"].tolist()
browser.get(website)
browser.set_page_load_timeout(20)

#CP: 10117
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
        for k in range(20):
            browser.find_element("xpath", '*').send_keys(Keys.PAGE_DOWN)
            time.sleep(1.5)
        soup = BeautifulSoup(browser.page_source, "html.parser")
        products = soup.find_all("div",{"class":"a-column a-span12 a-text-center _cDEzb_grid-column_2hIsc"})
        category = soup.find("div",{"class":"_cDEzb_card-title_2sYgw"}).find("h1",{"class":"a-size-large a-spacing-medium a-text-bold"}).text
        j = j + 1
        
        for p in products:
            i = i + 1
            ranking = p.find("div",{"class":"a-section zg-bdg-body zg-bdg-clr-body aok-float-left"}).find("span",{"class":"zg-bdg-text"}).text
            try:
                product_name = p.find("div",{"class":"p13n-sc-uncoverable-faceout"}).find("div",{"class":"_cDEzb_p13n-sc-css-line-clamp-3_g3dy1"}).text
            except:
                product_name = p.find("div",{"class":"_cDEzb_p13n-sc-css-line-clamp-4_2q2cc"}).text
            url_prod = "https://www.amazon.de" + p.find("div",{"class":"p13n-sc-uncoverable-faceout"}).find("a",{"class":"a-link-normal"})["href"]
            #sumarle el base url adelante
            try:
                rating = p.find("div",{"class":"a-icon-row"}).find("span",{"class":"a-icon-alt"}).text
            except:
                rating = "no rating included"
            try:
                n_rating = p.find("div",{"class":"a-icon-row"}).find("span",{"class":"a-size-small"}).text
            except:
                n_rating = "no rating included"
            try:
                price = p.find("div",{"class":"_cDEzb_p13n-sc-price-animation-wrapper_3PzN2"}).find("span",{"class":"_cDEzb_p13n-sc-price_3mJ9Z"}).text
            except:
                try:
                    price = p.find("span",{"class":"a-size-base"}).find("span",{"class":"p13n-sc-price"}).text
                except:
                    price = ""
            
            prod_item = {'category': category, 'product name': product_name, 'product url': url_prod, 'ranking': ranking, 'price': price, 'rating': rating, 'rating amount': n_rating}
            product_list.append(prod_item)
            
        try:
            browser.find_element(By.XPATH,"//li[@class='a-last']").click()
        except:
            j = 0
            break

df = pd.DataFrame(product_list)

df.to_excel(r'G:\Shared drives\DE Deal Analytics - FINIA\03_Assesments\20230724 - VERSUNI\02_Data\Amazon BestSellers\Amazon_BestSellers_ProductList_v2.xlsx')
