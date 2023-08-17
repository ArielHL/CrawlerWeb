
import threading
from concurrent.futures import ThreadPoolExecutor
import queue
from queue import Queue, Empty
from Spiders.spider import Spider
from MiddleWares.middlewares import *
import time
import sys
import logging

# *********************************************************************************************
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


# ****************************  CONFIGURATIONS  ***********************************************

PROJECT_NAME = 'SAN AGUSTIN'
HOMEPAGE =  'https://www.colegiosanagustin.com.ar/'
DOMAIN_NAME = get_domain_name(HOMEPAGE)
PROJECT_PATH = output_path.joinpath('Companies',PROJECT_NAME)


SORT_WORDS_LIST = ['PRIMARIA','JARDIN','CONTACTO','INICIAL','SECUNDARIO','INGLES']
NUMBER_OF_THREADS = 10
CRAWLED_SIZE_LIMIT = 100
LINKS_LIMIT = 100

# *********************************************************************************************



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


# *******************************************************************************************************************

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
       
# *******************************************************************************************************************   
        
def create_workers():
    
    for _ in range(NUMBER_OF_THREADS):
        t = threading.Thread(target=work,daemon=True)
        t.start()

# *******************************************************************************************************************
   
def work():
    
    while True :
        if len(spider.crawled) >= CRAWLED_SIZE_LIMIT:
            
            spider.queue.clear()
            while not queue.empty():
                queue.get()
                queue.task_done()
                
        url = queue.get()    
        spider.crawl_page(threading.current_thread().name, url)
        queue.task_done()
    
     
           
            
# *******************************************************************************************************************
        
if __name__ == '__main__':
    start=time.perf_counter()

    # initialize the worker threads waiting for adding links to the queue
    create_workers()

    # add links to the queue
    crawl()
    
    end=time.perf_counter()
    print(f'\nProcessed: {len(spider.crawled)}    \nFinished in {round(end-start,2)} seconds')
    print('\n')
    print(f'\nQueue Left:  {len(spider.queue)} with maximum Size {CRAWLED_SIZE_LIMIT}')