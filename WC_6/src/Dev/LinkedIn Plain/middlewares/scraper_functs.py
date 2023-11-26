"""_summary_

Contain main functions to perform element seeking

the main improvement would be replace the use of time for EC.Wait Selenium class

Returns:
    None
"""

# import external libraries

from selenium import webdriver 
from selenium.webdriver.common.by import By 
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.common.exceptions import NoSuchElementException
import time 
from typing import *
import time
import random   
import re
import pandas as pd
from tqdm import tqdm

 
def login(driver) -> None:
# Login
    sign_in_link=driver.find_element(By.XPATH,'/html/body/div[1]/main/div/p/a')
    sign_in_link.click()
    time.sleep(3*random.random())

    email_input=driver.find_element(By.XPATH,'//*[@id="username"]')
    email_input.send_keys('ariel.hernan.limes@pwc.com')
    time.sleep(1*random.random())

    password_input=driver.find_element(By.XPATH,'//*[@id="password"]')
    password_input.send_keys('MiVida1855')
    time.sleep(1*random.random())

    signIn_Button=driver.find_element(By.XPATH,'//*[@id="organic-div"]/form/div[3]/button')
    signIn_Button.click()
    time.sleep(3*random.random())
    


# ******************************  People Tab ************************************************

    
# gatter information function
def data_gatter(driver:webdriver,
                url:str,
                company:str,
                data_dict:dict) -> None:
    
    ul_carrusel=driver.find_elements(By.XPATH,'//ul[@class="artdeco-carousel__slider ember-view"]/li')
        
    for li in ul_carrusel:
        
        if 'active' in li.get_attribute('class'):
  
            try:
                active_wrapper=li.find_element(By.XPATH,'//ul[@class="artdeco-carousel__slider ember-view"]/li[contains(@class,"active")]')
                # wrapper where each column of data is stored
                div_wrapper=active_wrapper.find_element(By.XPATH,'.//div[@class="insight-container"]')
                # title of the column 
                active_title=div_wrapper.find_element(By.XPATH,'//ul[@class="artdeco-carousel__slider ember-view"]/li[contains(@class,"active")]/div/div/div/div/h3').text
                element_list=div_wrapper.find_elements(By.XPATH,'//ul[@class="artdeco-carousel__slider ember-view"]/li[contains(@class,"active")]/div/div/div/button')
                
                for element in element_list:
                    element_data = element.text
                    pattern = r'(\d+)\s+(.+)'
                    match = re.search(pattern, element_data)
                    number = match.group(1) if match else None
                    character = match.group(2) if match else None
                    
                    data_dict['Company'].append(company)
                    data_dict['Company_Web_Site'].append(url)
                    data_dict['Category'].append(active_title)
                    data_dict['Value'].append(number)
                    data_dict['Value_Cat'].append(character)
                    data_dict['Scope'].append('Netherlands') 
                       
            except NoSuchElementException:
                           
                    data_dict['Company'].append(None)
                    data_dict['Company_Web_Site'].append(None)
                    data_dict['Category'].append(None)
                    data_dict['Value'].append(None)
                    data_dict['Value_Cat'].append(None)
                    data_dict['Scope'].append('Netherlands')    
                
                
    return data_dict



            


def runner(data_Source:pd.DataFrame,
           driver:webdriver,
           dataDict:dict) -> None:
    
    # Search Company
    for index,row in tqdm(data_Source.iterrows()):
        
        if (index+1) % 10 == 0:
            driver.refresh()
        
        url=row['Linkedin URL people']
        company=row['Name']
        
        driver.get(url)
        time.sleep(6*random.random())
        
        # More Button
        try:
            driver.find_element(By.XPATH,'//button[@class="org-people__show-more-button t-16 t-16--open t-black--light t-bold"]').click()
        except NoSuchElementException:
            driver.refresh()
        
            
        time.sleep(6*random.random())
        # get elements in the carrusel
      
        ul_carrusel_len=len(driver.find_elements(By.XPATH,'//ul[@class="artdeco-pagination__pages artdeco-pagination__pages--dot"]/li'))
    
        # Iterate over the carrusel of the page
        for i in range(ul_carrusel_len):
            
            dot=driver.find_element(By.XPATH,f'//ul[@class="artdeco-pagination__pages artdeco-pagination__pages--dot"]/li[@data-test-pagination-page-btn={i+1}]')
            dot.click()
            time.sleep(1)
            data_gatter(driver,url,company,dataDict)


# ********************************* Jobs Tab *************************************

# gatter information function
def data_gatter_jobs(driver:webdriver,
                    index:int,
                    url:str,
                    company:str,
                    data_dict:dict) -> None:
    

  
    # Catch Job Lists
    try:
        jobs_lists=driver.find_elements(By.XPATH,'.//a[@class="disabled ember-view job-card-container__link job-card-list__title"]')
        time.sleep(3*random.random())
        for job in jobs_lists:
            time.sleep(1)
            job.click()
            # Create a Funcion to get the job details
            job_title=driver.find_element(By.XPATH,'//h2[@class="t-24 t-bold jobs-unified-top-card__job-title"]').text
            job_description=driver.find_element(By.XPATH,'//div[contains(@class,"jobs-description__content jobs-description-content")]').text
        
            data_dict['Index'].append(index)       
            data_dict['Company'].append(company)
            data_dict['Company_Web_Site'].append(url)
            data_dict['Title'].append(job_title)
            data_dict['Job_Description'].append(job_description)
            data_dict['Scope'].append('Netherlands') 
                       
    except NoSuchElementException as e:
       
            data_dict['Index'].append(index)
            data_dict['Company'].append(company)
            data_dict['Company_Web_Site'].append(url)
            data_dict['Title'].append(None)
            data_dict['Job_Description'].append(None)
            data_dict['Scope'].append('Netherlands')    
   
        
           
    return data_dict


def runner_jobs(data_Source:pd.DataFrame,
           driver:webdriver,
           data_dict:dict) -> None:
    
    # Search Company
    for index,row in tqdm(data_Source.iterrows()):
        
        if (index+1) % 10 == 0:
            driver.refresh()
        
        url=row['Linkedin URL jobs']
        company=row['Name']
        index=row['Number']
        
        driver.get(url)
        # More Button
        try:
            driver.find_element(By.XPATH,'//a[contains(@class,"ember-view link-without-hover-visited mt5")]').click()
            time.sleep(1)
        except NoSuchElementException:
            driver.refresh()

      
        
        try:
            # Select Netherland
            time.sleep(2*random.random())
            location_input=driver.find_element(By.XPATH,'//input[contains(@id,"jobs-search-box-location")]')
            time.sleep(1*random.random())
            location_input.clear()
            time.sleep(1*random.random())
            location_input.send_keys('Netherlands')
            time.sleep(1)
            location_input.send_keys(Keys.ENTER)
            time.sleep(2)

            # check if no jobs banner exists
            
            no_jobs_banner=driver.find_elements(By.XPATH,'//div[contains(@class,"jobs-search-no-results-banner__image")]')

            if len(no_jobs_banner) == 0:
            
                ul_carrusel_len=len(driver.find_elements(By.XPATH,'//li[contains(@class,"artdeco-pagination__indicator artdeco-pagination__indicator--number")]'))
                

                if ul_carrusel_len > 0:
                    for i in range(ul_carrusel_len):
                        if i <= 4:
                        
                            
                            driver.find_element(By.XPATH,f'//li[@data-test-pagination-page-btn="{i+1}"]').click()
                            time.sleep(1*random.random())
                            data_gatter_jobs(driver=driver,index=index,url=url,company=company,data_dict=data_dict)
                else:
                    data_gatter_jobs(driver=driver,index=index,url=url,company=company,data_dict=data_dict)
            
        except NoSuchElementException as e:
         
            data_dict['Index'].append(index)
            data_dict['Company'].append(company)
            data_dict['Company_Web_Site'].append(url)
            data_dict['Title'].append(None)
            data_dict['Job_Description'].append(None)
            data_dict['Scope'].append('Netherlands')   