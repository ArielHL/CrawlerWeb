# %%
# import external libraries
import pandas as pd
from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.common.by import By 
from selenium.webdriver.chrome.service import Service as ChromeService 
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from tqdm import tqdm
from typing import *
import random
from itertools import islice
import pandas as pd
from collections import defaultdict
import time

import warnings
warnings.filterwarnings('ignore')


# Selenium configuration
service = ChromeService()
options = webdriver.ChromeOptions()
driver = webdriver.Chrome(service=service, options=options)
# driver.implicitly_wait(10)
wait = WebDriverWait(driver, 5)
driver.maximize_window()


url = 'https://www.glassdoor.co.uk/Reviews/PwC-Reviews-E8450.htm'
driver.get(url)

next_button=None



data_dict={
    
    'review_advice': [],    
    'review_date': [],
    'review_title': [],
    'review_position': [],
    'review_location': [],
    'review_pros': [],
    'review_cons': [],
    'review_rating': [],
    'employee_status': [],
    
}

def review_gather() -> None:

    global data_dict
    
    
    review_rating=[element.text  if element.text!='' else 'NA' for element in 
                    driver.find_elements(By.XPATH,'//span[@class="review-details__review-details-module__overallRating"]') ]

    review_title=[element.text if element.text!='' else 'NA' for element in 
                    driver.find_elements(By.XPATH,'//div[@class="review-details__review-details-module__titleHeadline"]') ]

    review_position=[element.text  if element.text!='' else 'NA' for element in
                    driver.find_elements(By.XPATH,'//span[@class="review-details__review-details-module__employee"]') ]

    review_pros=[element.text  if element.text !='' else 'NA' for element in
                    driver.find_elements(By.XPATH,'//span[@data-test="pros"]') ]

    review_cons=[element.text if element.text !='' else 'NA' for element in 
                    driver.find_elements(By.XPATH,'//span[@data-test="cons"]') ]

    review_date=[element.text if element.text!='' else 'NA' for element in
                    driver.find_elements(By.XPATH,'//div[@class="review-details__review-details-module__reporButtontWrapper"]/span') ]

    review_employee_status=[element.text if element.text!='' else 'NA' for element in
                    driver.find_elements(By.XPATH,'//div[@class="review-details__review-details-module__employeeDetails"]') ]

    review_location=[element.text if element.text!='' else 'NA' for element in
                    driver.find_elements(By.XPATH,'//div[@class="review-details__review-details-module__employeeDetails review-details__review-details-module__locationDetails"]/span') ]
    
    review_advice=[element.text if element.text!='' else 'NA' for element in
                    driver.find_elements(By.XPATH,'//span[@data-test="advice-management"]') ]
    
    review_date = list(islice(review_date + ['NA'] * 10, 10))
    review_title = list(islice(review_title + ['NA'] * 10, 10))
    review_position = list(islice(review_position + ['NA'] * 10, 10))
    review_pros = list(islice(review_pros + ['NA'] * 10, 10))
    review_cons = list(islice(review_cons + ['NA'] * 10, 10))
    review_employee_status = list(islice(review_employee_status + ['NA'] * 10, 10))
    review_location = list(islice(review_location + ['NA'] * 10, 10))
    review_advice = list(islice(review_advice + ['NA'] * 10, 10))
    review_rating = list(islice(review_rating + ['NA'] * 10, 10))
        
        

    data_dict['review_date'].extend(review_date)
    data_dict['review_title'].extend(review_title)
    data_dict['review_position'].extend(review_position)
    data_dict['review_location'].extend(review_location)
    data_dict['review_pros'].extend(review_pros)
    data_dict['review_cons'].extend(review_cons)
    data_dict['review_rating'].extend(review_rating)
    data_dict['employee_status'].extend(review_employee_status)
    data_dict['review_advice'].extend(review_advice)
    

# Getting next button
def get_next_button() -> None:
    global next_button
    
    try:
        next_button=wait.until(EC.presence_of_element_located((By.CLASS_NAME, "nextButton")))
        
    except NoSuchElementException:
        next_button=None    
        
# Eliminate login pop-up
def eliminate_loggin():
    try:

            # wait.until(EC.element_to_be_clickable((By.XPATH, '//a[@data-test="ei-nav-reviews-link"]'))).click()

            overlay_element = wait.until(EC.presence_of_element_located((By.ID, "HardsellOverlay")))
            driver.execute_script("arguments[0].style.display = 'none';", overlay_element)
            driver.execute_script("document.body.style.overflow = 'scroll';")
            driver.execute_script("document.body.style.position = 'relative';")
            time.sleep(1)  
    except:
        pass


def scrolling():
    # Scroll
    for scroll in range(0,15):
        body = driver.find_element(by=By.XPATH, value='/html/body')
        body.send_keys(Keys.PAGE_DOWN)
        time.sleep(0.3)
    time.sleep(2)
    
    
    
def select_most_recent():
    
    dropdown = wait.until(EC.element_to_be_clickable((By.XPATH, '//div[@class="selectedLabel"]')))
    dropdown.click()

    most_recent = wait.until(EC.element_to_be_clickable((By.XPATH, '//li[@id="option_DATE"]')))
    most_recent.click()    
    
    
# Run the script
def run() -> None:
    
    # Eliminate login pop-up
    eliminate_loggin()
    # Calling the get_next_function
    get_next_button()
    
    if next_button: 
        try:
            scrolling()
            review_gather()
            # WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, '//button[@class="nextButton css-1iiwzeb e13qs2072"]'))).click()
            if not next_button.get_attribute('disabled'):
                driver.execute_script("arguments[0].click();", next_button)
                time.sleep(random.random())
                run()
            else:
                print('No more pages')
        except Exception as e:
            driver.refresh()
            run()
    else:
        print('No more pages')


if __name__ == '__main__':

    # get rid of the logging pop-up
    eliminate_loggin()
    # Select most recent option
    select_most_recent()
    time.sleep(1)
    # Run the script
    run()



    #  Creating DataFrame
    df = pd.DataFrame(data_dict)
    # Save to csv
    df.to_csv('./data/PwC.csv', index=False)


