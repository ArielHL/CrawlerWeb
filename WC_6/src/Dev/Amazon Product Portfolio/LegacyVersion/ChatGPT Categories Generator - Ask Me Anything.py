# -*- coding: utf-8 -*-
"""
Created on Thu Aug 31 17:57:02 2023

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
from selenium import webdriver
import selenium.webdriver as webdriver
import csv

##There's a limit to the amount you can ask per hour
browser = uc.Chrome(use_subprocess=True) ##use_subprocess=True para uc.Chrome hay que tener cuidado con las updates
website="https://chat.openai.com/"
browser.get(website)
browser.set_page_load_timeout(20)

# Load the Excel file into a DataFrame
excel_file = "C:/Users/Dell/Downloads/job titles.xlsx"
df = pd.read_excel(excel_file)

# Category mapping
category_mapping = ["administrative", "design", "engineering", 
    "management", "operations", "project management", 
    "working student/intern", "other"]


# =============================================================================
# BEFORE RUNNING YOU MUST SPECIFY THE CATEGORIES AND WHAT YOU WANT TO CHATGPT
#      --- Be as specific as posible and ask for a table format ---
# =============================================================================

# Initialize CSV data
csv_data = [["Job Title", "Category"]]
j = 1

# Categorize job titles 
for title in df["Job_Title"]:
    try:
              
        textbox = browser.find_element(By.ID , "prompt-textarea")
    
        textbox.send_keys(title) 
        textbox.send_keys(Keys.ENTER)
       
        time.sleep(2)
        soup = BeautifulSoup(browser.page_source, "html.parser")
        table = soup.find_all("tbody")
        Category = table[j].find_all("tr")[1].contents[1].text
        j = j + 1
        # Assign category based on category mapping
        csv_data.append([title, Category])
    
    except:
        Category = "Other"
        csv_data.append([title, Category])
    time.sleep(2)        

# Write CSV data to a file
csv_filename = "job_titles_categorized.csv"
with open(csv_filename, "w", newline="") as csv_file:
    writer = csv.writer(csv_file)
    writer.writerows(csv_data)

print(f"Job titles categorized and saved to {csv_filename}")
