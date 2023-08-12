import os
workingPAth=r'c:\Users\Ariel\OneDrive\Programacion\REPOS\CrawlerWeb'
os.chdir(workingPAth)
os.getcwd()


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

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler('src/WC_3/Logs/log.txt'), logging.StreamHandler()]
)

# *********************************************************************************************

sys.setrecursionlimit(2000000)

PROJECT_NAME = 'VEA'
HOMEPAGE =  'https://www.laescuelitavea.org.ar/'
DOMAIN_NAME = get_domain_name(HOMEPAGE)
SORT_WORDS_LIST = ['JARDIN','PRIMARIA']
QUEUE_FILE = 'company/'+ PROJECT_NAME + '/queue.json'
CRAWLED_FILE = 'company/'+ PROJECT_NAME + '/crawled.json'
NUMBER_OF_THREADS = 25
CRAWLED_SIZE_LIMIT = 100
LINKS_LIMIT = 25

# *********************************************************************************************


# Create Queue instance
queue = Queue()
stop_event = threading.Event()

# create spider instance
spider=Spider(project_name=PROJECT_NAME,
              base_url=HOMEPAGE,
              domain_name=DOMAIN_NAME,
              keywords_list=SORT_WORDS_LIST,
              crawled_size=CRAWLED_SIZE_LIMIT,
              links_limit=LINKS_LIMIT,
             )



# *******************************************************************************************************************

def crawl():
    """
    Read queue and crawled file into memory
    call the create jobs function
    
    """

    while len(spider.queue) > 0:
        if not stop_event.is_set():
                try:
                    for link in spider.queue: 
                        queue.put(link)
                    queue.join()
                   
                except RuntimeError as e:
                    logger.error(f'RuntimeError: {str(e)}')
        else:
            logger.info(f'stop_event.is_set(): {stop_event.is_set()}, Queue Left:  {len(spider.queue)} with maximum Size {CRAWLED_SIZE_LIMIT}')
            return
    

# *******************************************************************************************************************   
        
def create_workers():

    for _ in range(NUMBER_OF_THREADS):
        t = threading.Thread(target=work,daemon=True)
        t.start()

# *******************************************************************************************************************
   
def work():
    
    while not stop_event.is_set() :
        if not len(spider.crawled) >= CRAWLED_SIZE_LIMIT:
        
            url = queue.get()
            spider.crawl_page(threading.current_thread().name, url)
            queue.task_done()
        else:
            stop_event.set()
           
            
# *******************************************************************************************************************
        
if __name__ == '__main__':
    start=time.perf_counter()

    # initialize the worker threads waiting for adding links to the queue
    create_workers()

    # add links to the queue
    crawl()
    
    end=time.perf_counter()
    print(f' \n Processed: {len(spider.crawled)}    \n Finished in {round(end-start,2)} seconds')
    print('\n')
    print(f'stop_event.is_set(): {stop_event.is_set()}, \nQueue Left:  {len(spider.queue)} with maximum Size {CRAWLED_SIZE_LIMIT}')