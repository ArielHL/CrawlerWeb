from concurrent.futures import ThreadPoolExecutor
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
NUMBER_OF_THREADS = 1
CRAWLED_SIZE_LIMIT = 5
LINKS_LIMIT = 1

# Create Queue instance
queue = Queue()
stop_event = threading.Event()


# *******************************************************************************************************************
    
# create spider instance
spider=Spider(project_name=PROJECT_NAME,
            base_url=HOMEPAGE,
            domain_name=DOMAIN_NAME,
            keywords_list=SORT_WORDS_LIST,
            crawled_size=CRAWLED_SIZE_LIMIT,
            links_limit=LINKS_LIMIT,
            )


# *******************************************************************************************************************
    
def create_jobs(spider, queue, stop_event):
    if len(spider.crawled) < spider.crawled_size:
        logger.info(f'crawled size is less than the limit -- limit: {spider.crawled_size} and crawled size: {len(spider.crawled)}')
        try:
            for link in spider.queue:
                queue.put(link)
            queue.join()
            crawl(spider, queue, stop_event)
        except RuntimeError as e:
            logger.error('RuntimeError: ' + str(e))
    else:
        logger.info(f'crawled size is more than the limit -- limit: {spider.crawled_size} and crawled size: {len(spider.crawled)}')
        stop_event.set()

# *******************************************************************************************************************

def crawl(spider, queue, stop_event):
    while not stop_event.is_set():
        if len(spider.queue) > 0 and len(spider.crawled) < spider.crawled_size:
            logger.info(str(len(spider.queue)) + ' links in the queue and ' + str(len(spider.crawled)) + ' crawled links')
            create_jobs(spider, queue, stop_event)


def work():
    while not stop_event.is_set() :
        try:
            url = queue.get(timeout=5) 
            spider.crawl_page(threading.current_thread().name, url)
            queue.task_done()
        except queue.Empty:
            pass

# *******************************************************************************************************************
    
def main():
            
    with concurrent.futures.ThreadPoolExecutor(max_workers=NUMBER_OF_THREADS) as executor:
        # Initialize the worker threads
        for _ in range(NUMBER_OF_THREADS):
            executor.submit(work, spider, queue, stop_event)



# *******************************************************************************************************************
        
if __name__ == '__main__':
    start=time.perf_counter()

    main()
    
    end=time.perf_counter()
    print(f' \n Processed: {len(spider.crawled)}    \n Finished in {round(end-start,2)} seconds')