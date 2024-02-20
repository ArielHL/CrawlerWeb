# import external libraries
import pandas as pd
import threading
from pathlib import Path
import multiprocessing
import queue
from queue import Queue, Empty
import time
import logging
from tqdm import tqdm

# import internal libraries
from Spiders.spider import Spider
from MiddleWares.middlewares import *
from MiddleWares.ProgressBar import CustomProgressBar 
from MiddleWares.CustomLogger import CustomLogger

# setting the path

output_path = Path(__file__).parents[2].joinpath('Output')
logger_path = Path(__file__).parents[0].joinpath('Logs')
results_path = Path(__file__).parents[2].joinpath('Output','Results')
logger_path.mkdir(parents=True, exist_ok=True)
output_path.mkdir(parents=True, exist_ok=True)
results_path.mkdir(parents=True, exist_ok=True)
logger_file=logger_path.joinpath('log.txt')

# setting the logger

logger = CustomLogger(name=__name__, 
                             level=logging.INFO,
                             logger_file=logger_file)


# **************************************************** SETTINGS ****************************************************

SOURCE_FILE_NAME='url_base.xlsx'
RUN_PROTOCOL_CHECK = False
SORT_WORDS_LIST = []
NUMBER_OF_THREADS = 10
CRAWLED_SIZE_LIMIT = 50
LINKS_LIMIT = 25
CHUNK_SIZE = 500

# *******************************************************************************************************************


# Wrapper function to iterate over the list of companies
def main(project_name: str, homepage: str, total_links: int, sum_df_list: list, sum_df_lock: threading.Lock):
    
    thread_id = threading.current_thread().ident  # Get the thread ID

    # progress_bar = tqdm(total=total_links, leave=True, desc=f'Company: {project_name}')
    progress_bar = CustomProgressBar(total=total_links, desc=f'Company: {project_name}',leave=True)

    start=time.perf_counter()
    
    PROJECT_NAME = project_name
    HOMEPAGE =  homepage
    DOMAIN_NAME = get_domain_name(HOMEPAGE)
    PROJECT_PATH = output_path.joinpath('Companies',PROJECT_NAME)
    

    # Create Queue instance
    queue = Queue()

    # create spider instance
    spider=Spider(  project_name=PROJECT_NAME,
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
            
            progress_bar.update(1)  # Update the progress bar

    # initialize the worker threads waiting for adding links to the queue
    create_workers()

    # add links to the queue
    crawl()
    
    end=time.perf_counter()
    
    # logger.info(f'\nCompany: {PROJECT_NAME} Processed : {len(spider.crawled)} links    \nFinished in {round(end-start,2)} seconds')

    with sum_df_lock:
        company_dict={'Company':PROJECT_NAME,'url_base':HOMEPAGE,'Links':len(spider.crawled),'Time':round(end-start,2)}
        sum_df_list.append(company_dict)
    progress_bar.close()
        

# *******************************************************************************************************************
        
if __name__ == '__main__':
    
    # definition of the folder for the source file
    logger.enable_terminal_logging()
    logger.info('Starting the process')
    # reading source File
    source_path = Path(__file__).parents[1].joinpath('Source')
    source_file = source_path.joinpath(SOURCE_FILE_NAME)
    logger.info(f'Reading Source file: {source_file}')
    df=pd.read_excel(source_file)
    df.drop_duplicates(subset=['WebSite'],inplace=True)
    df['Company']=df['Company'].apply(lambda text: remove_special_characters(text))
    
    # checking if the protocol option is on, if so, it will run the protocol check
    if RUN_PROTOCOL_CHECK == True:
        # 1. update protocol of websites
        ulr_crawl_modified = source_path.joinpath('Url_to_Crawl_modified.xlsx')
        if not ulr_crawl_modified.exists():
            logger.info(f'starting updating protocol of websites over: {len(df)} rows')
            df=protocol_handler(df)
            df.to_excel(source_path.joinpath('Url_to_Crawl_modified.xlsx'),index=False)
            logger.info(f'Finished updating protocol of websites over: {len(df)} rows')
        else:
            logger.info(f'File already exists. Skipping updating protocol of websites over: {len(df)} rows')
            df=pd.read_excel(ulr_crawl_modified)
            
    elif RUN_PROTOCOL_CHECK == False:
        logger.info(f'Protocol check is off. Skipping updating protocol of websites over: {len(df)} rows')
        df.rename(columns={'WebSite':'WebSite_full'},inplace=True)
        ulr_crawl_modified = source_path.joinpath('Url_to_Crawl_modified.xlsx')
        df.to_excel(source_path.joinpath('Url_to_Crawl_modified.xlsx'),index=False)
          
    # Setting number of workers (one for company)
    num_cores = multiprocessing.cpu_count()*2
    
    logger.info(f'Starting Multiprocess with Number of workers: {num_cores}')
    with multiprocessing.Manager() as manager:
        
        start=time.perf_counter()
        
        sum_df_list = manager.list()
        sum_df_lock = manager.Lock()
        
        # Split the DataFrame into chunks based on CHUNK_SIZE
        num_chunks = len(df) // CHUNK_SIZE + (len(df) % CHUNK_SIZE != 0)
        for i in range(num_chunks):
            logger.info(f'Starting Chunk: {i+1} of {num_chunks}')
            logger.info('*'*160)
            start_idx = i * CHUNK_SIZE
            end_idx = (i + 1) * CHUNK_SIZE
            chunk_df = df[start_idx:end_idx]
    
            # Create a multiprocessing pool
            with multiprocessing.Pool(processes=num_cores) as pool:
                pool.starmap(main, [(row['Company'], row['WebSite_full'],CRAWLED_SIZE_LIMIT,sum_df_list,sum_df_lock) for index, row in chunk_df.iterrows()])
        

        end=time.perf_counter()
        
        # Convert the shared list to a DataFrame
        sum_df = pd.DataFrame(list(sum_df_list))
        sum_df.to_excel(results_path.joinpath('Summary.xlsx'), index=False)
        logger.info(f'\nFinished Complete process in {round(end-start,2)} seconds')
    
    
