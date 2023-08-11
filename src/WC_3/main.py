import os
workingPAth=r'c:\Users\Ariel\OneDrive\Programacion\REPOS\CrawlerWeb'
os.chdir(workingPAth)
os.getcwd()


import threading
import queue
from queue import Queue
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

PROJECT_NAME = 'AERZEN'
HOMEPAGE =  'https://www.aerzen.com/en-es.html'
DOMAIN_NAME = get_domain_name(HOMEPAGE)
SORT_WORDS_LIST = ['BLOWERS','PRODUCTS']
QUEUE_FILE = 'company/'+ PROJECT_NAME + '/queue.json'
CRAWLED_FILE = 'company/'+ PROJECT_NAME + '/crawled.json'
NUMBER_OF_THREADS = 10
CRAWLED_SIZE_LIMIT = 50
LINKS_LIMIT = 50

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

    if len(spider.queue) > 0 and len(spider.crawled) < spider.crawled_size:
    
        print(str(len(spider.queue)) + ' links in the queue and ' + str(len(spider.crawled)) + ' crawled links' )
        create_jobs()

# *******************************************************************************************************************
      
def create_jobs():
    """_summary_
    Iterate over the queue set and put the links in the queue
    """
    if len(spider.crawled) < spider.crawled_size:
        logger.info(f'crawled size is less than the limit -- limit: {spider.crawled_size} and crawled size: {len(spider.crawled)}')
        try:
            for link in spider.queue: 
                queue.put(link)
            queue.join()
            crawl()
        except RuntimeError as e:
            logger.error(f'RuntimeError: {str(e)}')
    else:
        logger.info(f'crawled size is more than the limit -- limit: {spider.crawled_size} and crawled size: {len(spider.crawled)}')
        spider.queue.clear()
        
   
# *******************************************************************************************************************   
        
def create_workers():

    for _ in range(NUMBER_OF_THREADS):
        t = threading.Thread(target=work,daemon=True)
        t.start()


# *******************************************************************************************************************
   
def work():
    
    while not stop_event.is_set() :
        
        if len(spider.crawled) >= spider.crawled_size:
            stop_event.set()
            
        else:   
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
    print(f' \n Processed: {len(spider.crawled)}    \n Finished in {round(end-start,2)} seconds')