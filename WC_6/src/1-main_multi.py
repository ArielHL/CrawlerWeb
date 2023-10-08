
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
from tqdm import tqdm
import concurrent.futures
# setting the path

output_path = Path(__file__).parents[2].joinpath('Output')
logger_path = Path(__file__).parents[0].joinpath('Logs')
logger_path.mkdir(parents=True, exist_ok=True)
output_path.mkdir(parents=True, exist_ok=True)
logger_file=logger_path.joinpath('log.txt')

# logger setting function
def setup_logger(logger_name, log_file, level=logging.INFO):
    logger = logging.getLogger(logger_name)
    logger.setLevel(level)
    
    # Create formatter
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    
    # Create file handler and set formatter
    file_handler = logging.FileHandler(log_file)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    # Create console handler and set formatter
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    return logger

# **************************************************** SETTINGS ****************************************************

SORT_WORDS_LIST = ['DATA ANALYTICS','GEN AI','M&A','DATA SCIENCE']
NUMBER_OF_THREADS = 30
CRAWLED_SIZE_LIMIT = 500
LINKS_LIMIT = 100


# *******************************************************************************************************************

sum_df=pd.DataFrame()

# Wrapper function to iterate over the list of companies
<<<<<<< Updated upstream
def main(project_name:str, homepage:str):
=======
def main(project_name:str,
         homepage:str,
         sum_df_list:list,
         sum_df_lock:threading.Lock,
         logger_file:str=logger_file,
         global_progress_bar: tqdm=None):
    
    # Set up the logger with handlers in each worker process
    logger = setup_logger(__name__, logger_file)
>>>>>>> Stashed changes
    
    global sum_df

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
        global global_progress_bar
        crawled_progress_bar = tqdm(total=CRAWLED_SIZE_LIMIT, desc=f"Company: {project_name}")
        
        while len(spider.queue) > 0 and len(spider.crawled) < CRAWLED_SIZE_LIMIT:
        
            try:
                for link in spider.queue: 
                    queue.put(link)
                    crawled_progress_bar.update(1) # add to update the progress bar
                    global_progress_bar.set_postfix({"Company": project_name, "Links": len(spider.crawled)})

                queue.join()
                
            except RuntimeError as e:
                logger.error(f'RuntimeError: {str(e)}')
                
        crawled_progress_bar.close()  # Close the progress bar after processing for the current company is done
        global_progress_bar.update(1)  # Update global progress bar for each completed company


        
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
<<<<<<< Updated upstream
    company_dict={'Company':PROJECT_NAME,'Links':len(spider.crawled),'Time':round(end-start,2)}
    df=pd.DataFrame(company_dict, index=[0])
    sum_df=pd.concat([sum_df, df], ignore_index=True)
    
=======

    with sum_df_lock:
        company_dict={'Company':PROJECT_NAME,'Links':len(spider.crawled),'Time':round(end-start,2)}
        sum_df_list.append(company_dict)
        

def process_company(args):
    global_progress_bar, project_name, homepage, sum_df_list, sum_df_lock, log_file = args
    # Set up the logger again inside the process_company function
    logger = setup_logger(__name__, log_file)
    
    main(project_name, homepage, sum_df_list, sum_df_lock, global_progress_bar, log_file)



>>>>>>> Stashed changes
# *******************************************************************************************************************
        
if __name__ == '__main__':
    
    source_path = Path(__file__).parents[1].joinpath('Source')
    source_file = source_path.joinpath('URLs_for_crawler.xlsx')
    df=pd.read_excel(source_file)
    
<<<<<<< Updated upstream
    start=time.perf_counter()
=======
   # Create a global progress bar for all companies
    global_progress_bar = tqdm(total=df.shape[0], desc="Processing Companies")


>>>>>>> Stashed changes
    
    # Setting number of workers (one for company)
    num_workers=df.shape[0]
    num_cores = multiprocessing.cpu_count()
    
<<<<<<< Updated upstream
    # Create a multiprocessing pool
    with multiprocessing.Pool(processes=num_workers) as pool:
        pool.starmap(main, [(row['Company'], row['WebSite']) for index, row in df.iterrows()])
        

    end=time.perf_counter()
    logger.info(f'\n\nTotal Processed : {sum_df.Links.sum()} links   \nFinished in {round(end-start,2)} seconds')
    logger.info(f'\nFinished Complete process in {round(end-start,2)} seconds')
    
=======
    
    with multiprocessing.Manager() as manager:
        
        # Create a list of arguments tuples for each company
        
        # Create the output directory if it does not exist
        output_directory = output_path.joinpath('Companies')
        output_directory.mkdir(parents=True, exist_ok=True)
        
        
        
        start=time.perf_counter()
        
        sum_df_list = manager.list()
        sum_df_lock = manager.Lock()
    
    
        args_list = [(global_progress_bar, row['Company'], row['WebSite'], sum_df_list, sum_df_lock, str(logger_file)) for index, row in df.iterrows()]

       # Threading logic within the main block
        with concurrent.futures.ThreadPoolExecutor(max_workers=num_workers) as executor:
            executor.map(process_company, args_list)
        

        end=time.perf_counter()
        
        # Convert the shared list to a DataFrame
        sum_df = pd.DataFrame(list(sum_df_list))
        sum_df.to_excel(output_path.joinpath('Companies','Summary.xlsx'), index=False)
        
        # Log the final process completed message in the main process
        final_logger = setup_logger(logger_name='final_logger', level=logging.INFO,log_file=logger_file)
        final_logger.info(f'\nFinished Complete process in {round(end-start, 2)} seconds')
>>>>>>> Stashed changes
    