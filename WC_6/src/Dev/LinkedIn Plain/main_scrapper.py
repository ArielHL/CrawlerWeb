# %%
# import external libraries
import pandas as pd
from selenium import webdriver 
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.common.by import By 
from selenium.webdriver.chrome.service import Service as ChromeService 
from webdriver_manager.chrome import ChromeDriverManager 
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By

from tqdm import tqdm
import re 
from typing import *
import time
import random
    
import warnings
warnings.filterwarnings('ignore')

# import local modules
from middlewares.scraper_functs import login, data_gatter,runner

# Selenium configuration
options = Options()
options.headless = False 
driver = webdriver.Chrome(service=ChromeService(ChromeDriverManager(version='114.0.5735.90').install()),options=options)
driver.maximize_window()

# Dictionary Definition

dataDict={
    'Company':[],
    'Company_Web_Site':[],
    'Category':[], 
    'Value':[],
    'Value_Cat':[],
    'Scope':[]
}

# Url 
url_base = 'https://www.linkedin.com/feed/?trk=onboarding-landing'


if __name__ == '__main__':
    

    # Definint the driver
    driver.get(url_base)

    # login
    login(driver)


    # Reading Companies
    companies = pd.read_excel(r'C:\Users\alimes001\pwc\Deal Analytics - Project Avatar\Target Long list Enrichment\2. Data gathering\LinkedIn scraping\01_Input\AI companies overview 31052023.xlsx')
    companies=companies.dropna(subset=['Linkedin URL'])
    companies['Linkedin URL people']=companies['Linkedin URL'] + '/people/' + '?facetGeoRegion=102890719'
    data_Source=companies[['Name','Linkedin URL people']]

    # Calculate Unique Source Companies
    unique_companies=data_Source.Name.nunique()

    # Initialize the driver
    runner(data_Source=data_Source,driver=driver,dataDict=dataDict)

    # Create a DF 
    df=pd.DataFrame(dataDict)
    # calculate Companies retrieved
    unique_companies_after=df.Company.nunique()
    # Export DF to Excel
    df.to_excel("./data/dataExported_nh_1.xlsx",index=False)

    # Create a control DF to understand what is missing
    data_control = data_Source.merge(df, left_on='Name',right_on='Company', how='outer', indicator=True)

    # which companies have not been processed properly?
    df_left=data_control[data_control['_merge']=='left_only'][['Name','Linkedin URL people']]


