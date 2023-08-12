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

PROJECT_NAME = 'AERZEN_10'
HOMEPAGE =  'https://www.aerzen.com/en-es.html/'
DOMAIN_NAME = get_domain_name(HOMEPAGE)
SORT_WORDS_LIST = ['BLOWERS','PRODUCTS']
QUEUE_FILE = 'company/'+ PROJECT_NAME + '/queue.json'
CRAWLED_FILE = 'company/'+ PROJECT_NAME + '/crawled.json'
NUMBER_OF_THREADS = 30
CRAWLED_SIZE_LIMIT = 20
LINKS_LIMIT = 20

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
            spider.queue.clear()
            while not queue.empty():
                queue.get()
            logger.info('Queue has been cleared')
# *******************************************************************************************************************   
        
def create_workers():

    # threads_list=[]

    for _ in range(NUMBER_OF_THREADS):
        t = threading.Thread(target=work,daemon=True)
        t.start()

    # for thread in threads_list:
    #     thread.join()
    
# *******************************************************************************************************************
   
def work():
    
    while not stop_event.is_set() :
        if not len(spider.crawled) >= CRAWLED_SIZE_LIMIT:
            url = queue.get()
            spider.crawl_page(threading.current_thread().name, url)
            queue.task_done()
        else:
            stop_event.set()
    else:
        logger.info('Stop event is set, exiting...')
        return 
            
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