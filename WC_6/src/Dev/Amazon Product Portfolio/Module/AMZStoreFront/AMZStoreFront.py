from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import StaleElementReferenceException
import pandas as pd
import re
# from collections import defaultdict

# from tqdm import tqdm
import time
from random import randint, random
import undetected_chromedriver as uc
from selenium import webdriver
import selenium.webdriver as webdriver
from Logging.customLogger import CustomLogger
from pathlib import Path

# Setting path data
logger_path = Path(__file__).parents[1].joinpath('Logs')
logger_path.mkdir(parents=True, exist_ok=True)
logger_file=logger_path.joinpath('log.txt')

logger=CustomLogger(__name__,logger_file)


class AMZPBrowser(uc.Chrome):
        
    def __init__(self, 
                 data_target:dict=None,
                 *args,
                 **kwargs):
        super().__init__(*args, **kwargs)
        self.set_page_load_timeout(20)
        self.maximize_window()
        self.wait=WebDriverWait(self, 15)
        self.data=data_target
        
    
    
    @property
    def data(self):
        return self._data
    
    @data.setter
    def data(self, value):
        if value is not None and isinstance(value, dict):
            self._data = value
        else:
            raise ValueError("Data must be a dictionary")
            logger.error("Data must be a dictionary")
        
    
    def login_to_amazon(self, username, password):
        
        try:
            self.wait.until(
                EC.presence_of_element_located((By.ID, "nav-link-accountList-nav-line-1"))
            ).click()

            username_field = self.find_element(By.ID, "ap_email")
            username_field.send_keys(username)
            
            continue_button = self.find_element(By.ID, "continue")
            continue_button.click()

            password_field = self.find_element(By.ID, "ap_password")
            password_field.send_keys(password)

            login_button = self.find_element(By.ID, "signInSubmit")
            login_button.click()

        except Exception as e:
            logger.error(f"An error occurred: {str(e)}")
            
    def set_address(self,zipcode):
        try:
            
            self.wait.until(
                EC.presence_of_element_located((By.XPATH, '//span[@class="nav-line-2 nav-progressive-content"]'))
                ).click()
            
            
            address_input=self.wait.until(
                EC.presence_of_element_located((By.XPATH, '//input[@id="GLUXZipUpdateInput"]')))
            address_input.clear()
            address_input.send_keys(zipcode)
            address_input.send_keys(Keys.ENTER)
        except Exception as e:
            logger.error(f"An error occurred: {str(e)}")
            
    def scrolling(self):
    
        self.execute_script("document.body.style.overflow = 'scroll';")
        self.execute_script("document.body.style.position = 'relative';")
        time.sleep(1) 
        # Scroll
        for scroll in range(0,50):
            body = self.find_element(by=By.XPATH, value='/html/body')
            body.send_keys(Keys.PAGE_DOWN)
            time.sleep(0.3)
            
        time.sleep(random()*5)
    
    
    def get_info(self,product):
    
        global data_dict

        # **************************************************************************************************************************************
        try:
            product_title=product.find_element(By.CSS_SELECTOR,"[data-testid='product-grid-title']").text
        except:
            product_title="No title"
        
        # ***************************************************************************************************************************************
        
        try:
            element = product.find_element(By.CSS_SELECTOR,'[data-testid="icon-star"]')
        
            stars_text = self.execute_script('return arguments[0].textContent;', element)
            match = re.search(r'(\d+\.\d+)', stars_text)
            if match:
                rating = float(match.group(1))
            else:
                rating = 0
        except:
            rating=0
        
        # ***************************************************************************************************************************************
        
        try:
            n_rating=product.find_element(By.CSS_SELECTOR,"[data-testid='grid-item-review-count']").text
        except:
            n_rating=0
        
        # ***************************************************************************************************************************************
        
        try:
            element = product.find_element(By.CSS_SELECTOR,"[class*='Price__price__LKpWT Price__small__Y4NDm']")
            orig_prices = self.execute_script('return arguments[0].textContent;', element)

            pattern = r'£([\d.]+)'
            matches = re.search(pattern, orig_prices)
            if matches:
                original_price = matches.group(1)
            else:
                original_price = 0
        except:
            original_price=0    
        
        # ***************************************************************************************************************************************
        try:
            element = product.find_element(By.CSS_SELECTOR,'[data-testid="grid-item-buy-price"]')
                
            prices = self.execute_script('return arguments[0].textContent;', element)
            pattern = r'£([\d.]+)(?: – £([\d.]+))?|£([\d.]+)RRP:£([\d.]+)'

        # Match the pattern in the input string
            matches = re.search(pattern, prices)

            if matches:
                if matches.group(1):
                    group1 = matches.group(1)
                    group2 = matches.group(2) if matches.group(2) else '0'
                    begin_price= group1
                    to_price= group2
                elif matches.group(3):
                    group1 = matches.group(3)
                    group3 = matches.group(4)
                    begin_price = group1   
            else:
                begin_price=0
                to_price=0
                
        except:
            begin_price=0
            to_price=0


        # ***************************************************************************************************************************************
        page=self.current_url
        
        # ***************************************************************************************************************************************
        
        self.data['page'].append(page)
        self.data['product_title'].append(product_title)
        self.data['rating'].append(rating)
        self.data['n_rating'].append(n_rating)
        self.data['begin_price'].append(begin_price)
        self.data['to_price'].append(to_price)
        self.data['original_price'].append(original_price)
        
    def getting_data(self):
        product_wrapper = self.find_elements(By.XPATH, '//li[@data-testid="product-grid-item"]')
        for product in product_wrapper:
            self.get_info(product)
            
    def get_nav_bar_elements(self):
    
        nav_bar_elements=self.wait.until(EC.presence_of_all_elements_located(
            (By.XPATH,'//ul[contains(@class,"Navigation__navList")]/li[a[@tabindex=0]]')
            ))
        return nav_bar_elements          
    
    def explore_nav_bar_elements(self):
        nav_bar_elements=self.get_nav_bar_elements()
        index=0
        for _ in range(len(nav_bar_elements)):
            time.sleep(1)
            if index <= len(nav_bar_elements):
                nav_bar_elements[index].click()
                try:
                    nav_bar_elements[index].find_element(By.XPATH,'.//li[contains(@class,"Navigation__isHeading")]').click()
                except StaleElementReferenceException:
                    pass
                time.sleep(random()*1)
                logger.info('Scrolling to the bottom of the page')
                self.scrolling()
                time.sleep(random()*1)
                logger.info('Getting data')
                self.getting_data()
                time.sleep(random()*1)
                nav_bar_elements=self.get_nav_bar_elements()
                index+=1