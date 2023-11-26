
# import external libraries
import pandas as pd
import time
# import internal libraries
from AMZStoreFront.AMZStoreFront import AMZPBrowser
from Logging.customLogger import CustomLogger
from pathlib import Path
import random


logger_path = Path(__file__).parents[0].joinpath('Logs')
logger_path.mkdir(parents=True, exist_ok=True)
logger_file=logger_path.joinpath('log.txt')




# Define variables
username='ariel.hernan.limes@pwc.com'
password='CompraLicencias'
amazon_url='https://www.amazon.co.uk'

data_dict={
    'page': [],
    'product_title': [],
    'rating': [],    
    'n_rating': [],
    'original_price': [],
    'begin_price': [],
    'to_price': [],
}



if __name__ == '__main__':

    # Create a custom logger
    logger=CustomLogger(__name__,logger_file)
    # Create a browser object
    browser=AMZPBrowser(use_subprocess=True,data_target=data_dict)
    browser.get(amazon_url)
    logger.info('Amazon URL opened and ready to login')
    
    # Login to Amazon
    browser.login_to_amazon(username,password)
    logger.info('Logged in to Amazon')
    time.sleep(1)
    
    # Set address
    browser.set_address('AB11 6DA')
    logger.info('Address set')
    time.sleep(1)
    
    # Open brand URL
    brand_url='https://www.amazon.co.uk/stores/page/BE0B08DB-4583-4C8A-A689-CDE644FC7629?ingress=0&visitId=53b3f803-1726-4a1a-88ea-720b07c22064'
    browser.get(brand_url)
    browser.refresh()
    time.sleep(1)
    logger.info('Brand URL opened')
    
    # Explore nav bar elements
    logger.info('Exploring nav bar elements')
    browser.explore_nav_bar_elements()

    # Saving data
    logger.info('Saving data')
    df=pd.DataFrame(browser.data)
    print(df.head(15))
    df.to_excel('data.xlsx',index=False)
    
    # Closing browser
    logger.info('Closing browser')
    browser.quit()


