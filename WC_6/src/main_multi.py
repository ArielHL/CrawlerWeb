
import pandas as pd
import threading
from pathlib import Path
import multiprocessing
import queue
from queue import Queue, Empty
from Spiders.spider import Spider
from MiddleWares.middlewares import *
import time
import logging
from prompt_toolkit.shortcuts import ProgressBar
# setting the path

output_path = Path(__file__).parents[2].joinpath('Output')
logger_path = Path(__file__).parents[0].joinpath('Logs')
logger_path.mkdir(parents=True, exist_ok=True)
output_path.mkdir(parents=True, exist_ok=True)
logger_file=logger_path.joinpath('log.txt')

# setting the logger

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler(logger_file), logging.StreamHandler()]
)

# **************************************************** SETTINGS ****************************************************

SORT_WORDS_LIST = ['PRIMARIA','JARDIN','CONTACTO','INICIAL','SECUNDARIO','INGLES']
NUMBER_OF_THREADS = 10
CRAWLED_SIZE_LIMIT = 50
LINKS_LIMIT = 100


# *******************************************************************************************************************
# Wrapper function to iterate over the list of companies
def main(project_name:str, homepage:str):
    

    start=time.perf_counter()
    
    PROJECT_NAME = project_name
    HOMEPAGE =  homepage
    DOMAIN_NAME = get_domain_name(HOMEPAGE)
    PROJECT_PATH = output_path.joinpath('Companies',PROJECT_NAME)
    

    # Create Queue instance
    queue = Queue()

    # create spider instance
    spider=Spider(project_name=PROJECT_NAME,
                base_url=HOMEPAGE,
                domain_name=DOMAIN_NAME,
                keywords_list=SORT_WORDS_LIST,
                crawled_size=CRAWLED_SIZE_LIMIT,
                links_limit=LINKS_LIMIT,
                project_path=PROJECT_PATH
                )


    def crawl():
        """
        Read queue and crawled file into memory
        call the create jobs function
        
        """
        while len(spider.queue) > 0 and len(spider.crawled) < CRAWLED_SIZE_LIMIT:
        
            try:
                for link in spider.queue: 
                    queue.put(link)
                queue.join()
                
            except RuntimeError as e:
                logger.error(f'RuntimeError: {str(e)}')
       
        
    def create_workers():
        """
        Create worker threads (will die when main exits)
        
        """
     
        for _ in range(NUMBER_OF_THREADS):
            t = threading.Thread(target=work,daemon=False)
            t.start()

    def work():
        """
        Do the next job in the queue
        will check if the crawled size is bigger than the limit
        if so, it will clear the queue and end the process
        
        """
        while True :
            if len(spider.crawled) >= CRAWLED_SIZE_LIMIT:
                
                spider.queue.clear()
                while not queue.empty():
                    queue.get()
                    queue.task_done()
                    
            url = queue.get()    
            spider.crawl_page(threading.current_thread().name, url)
            queue.task_done()


    # initialize the worker threads waiting for adding links to the queue
    create_workers()

    # add links to the queue
    crawl()
    
    end=time.perf_counter()
    
    logger.info(f'\nCompany: {PROJECT_NAME} Processed : {len(spider.crawled)} links    \nFinished in {round(end-start,2)} seconds')
 
  
# *******************************************************************************************************************
        
if __name__ == '__main__':
    
    source_path = Path(__file__).parents[1].joinpath('Source')
    source_file = source_path.joinpath('Company_data.xlsx')
    df=pd.read_excel(source_file)
    
    start=time.perf_counter()
    
    # Setting number of workers (one for company)
    num_workers=df.shape[0]
    
    # Create a multiprocessing pool
    with multiprocessing.Pool(processes=num_workers) as pool:
        pool.starmap(main, [(row['Company'], row['WebSite']) for index, row in df.iterrows()])
        

    end=time.perf_counter()
    logger.info(f'\nFinished Complete process in {round(end-start,2)} seconds')
    
    