
# import external libraries
import pandas as pd
from selenium import webdriver 
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.common.by import By 
from selenium.webdriver.chrome.service import Service as ChromeService 
from webdriver_manager.chrome import ChromeDriverManager 
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from tqdm import tqdm
from typing import *
import random
    
import warnings
warnings.filterwarnings('ignore')

# import local modules
from middlewares.scraper_functs import login, runner,runner_jobs

# Selenium configuration
options = Options()
options.headless = False 
driver = webdriver.Chrome(service=ChromeService(ChromeDriverManager(version='114.0.5735.90').install()),options=options)
driver.maximize_window()
wait = WebDriverWait(driver, 5)

# Dictionary Definition

dataDict={
    'Company':[],
    'Company_Web_Site':[],
    'Category':[], 
    'Value':[],
    'Value_Cat':[],
    'Scope':[]
}

JobDict={
    'Index':[],
    'Company':[],
    'Company_Web_Site':[],
    'Title':[], 
    'Job_Description':[],
    'Scope':[]
}

# Global Variables definition

unique_companies_source:int = None
unique_companies_after:int= None

# Url 
url_base = 'https://www.linkedin.com/feed/?trk=onboarding-landing'


# Definint the driver
driver.get(url_base)

# login
login(driver)

# # Reading Companies
companies = pd.read_excel(r'C:\Users\alimes001\pwc\Deal Analytics - Project Avatar\Target Long list Enrichment\2. Data gathering\LinkedIn scraping\01_Input\AI companies overview 31052023.xlsx')
companies=companies.dropna(subset=['Linkedin URL'])
companies['Linkedin URL jobs']=companies['Linkedin URL'] + '/jobs'
data_Source=companies[['Number','Name','Linkedin URL jobs']]

# Calculate Unique Source Companies
unique_companies_source=data_Source.Name.nunique()

# Execution Wrapper

def execution(function,
              working_dict:dict,
              webdriver,
              dataSource:pd.DataFrame) -> pd.DataFrame:
    
    """
    Summary:
    
    Wrapper to execute the linkedIn Elements
    
    agrs:
    
    function: function type that contain the main function to execute the exploration
    working_dict: dict type that holds the dict to be filled by the exploration job
    WebDriver: WebDriver type that holds the execution driver
    dataSource: Pandas Dataframe Type, contains the data to be iterate and explore
 
    """
    global unique_companies_after

    # Initialize the driver
    function(data_Source=dataSource,driver=webdriver,data_dict=working_dict)

    # Create a DF 
    df=pd.DataFrame(JobDict)

    # Replace line scape
    df['Job_Description'].replace(regex=True,inplace=True,to_replace=r'\n',value=r' ')

    # calculate Companies retrieved
    unique_companies_after=df.Company.nunique()
    
    return df


if __name__ == "__main__":

    df_jobs=execution(function=runner_jobs,
                    working_dict=JobDict,
                    webdriver=driver,
                    dataSource=data_Source,
                    )

    df_jobs.to_excel('jobResult.xlsx', index=False)


    
    

