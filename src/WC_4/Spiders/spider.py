from urllib.request import urlopen, Request
from urllib import error
from Spiders.link_finder import LinkFinder
from MiddleWares.middlewares import *
from os import path, getcwd
from bs4 import BeautifulSoup

import logging

# *********************************************************************************************

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler('src/WC_3/Logs/log.txt'), logging.StreamHandler()]
)

# *********************************************************************************************

class Spider:

    project_name = None
    base_url = None
    domain_name = None
    queue_file = None
    crawled_file = None
    sort_keywords_list= None
    queue = set()
    crawled = set()
    
    def __init__(self,
                 project_name:str,
                 base_url:str,
                 domain_name:str,
                 keywords_list:List[str],
                 links_limit:int=10,
                 crawled_size:int = 100,
                 project_path:str=None):
        
        # defining class parameters
        Spider.project_name = project_name
        Spider.base_url = base_url
        Spider.domain_name = domain_name
        Spider.sort_keywords_list = [word.lower() for word in keywords_list]
        Spider.exception_list = ['mailto:','json','tel:','javascript:','whatsapp:','.png','.ico','php','css','feed','xlm']
        
        Spider.project_path = project_path if project_path else os.path.join(os.getcwd(),'companies',Spider.project_name) 
        Spider.queue_file =  Spider.project_path + '/queue.json'
        Spider.crawled_file = Spider.project_path + '/crawled.json'
        Spider.links_limit = links_limit
        Spider.crawled_size = crawled_size
        
        # define object parameters
        self.html_string_status = False
        
        self.boot()
        self.crawl_page('First spider',Spider.base_url)
    
    @staticmethod    
    def boot():
        # create a project directory
        create_project_dir(Spider.project_path)
        # create queue and crawled files if not created
        create_data_files(Spider.project_path, Spider.base_url)
        # load queue and crawled files
        Spider.queue = file_to_list(file_name=Spider.queue_file,dict_key='url')
        Spider.crawled = file_to_list(file_name=Spider.crawled_file,dict_key='url')
        Spider.html = file_to_list(file_name=Spider.crawled_file,dict_key='html_string')
        Spider.html_lang = file_to_list(file_name=Spider.crawled_file,dict_key='html_lang')
        
  
    def crawl_page(self,thread_name:str,page_url:str):
        
        """_summary_
        
        this method crawl the page_url and add the links to the queue
        
        args:
        thread_name (str): name of the thread
        page_url (str): url to be crawled

        """        
    
        # check if page_url is not in crawled
        
        if page_url not in Spider.crawled:
            logger.info(thread_name + ' now crawling ' + page_url)
            perct =  round(len(Spider.crawled)/(len(Spider.queue)+len(Spider.crawled)),2) if len(Spider.crawled) > 0 else 0
            logger.info('thread '+ thread_name +' | Queue ' + str(len(Spider.queue)) + ' | crawled ' + str(len(Spider.crawled)) + '| % ' + str(perct*100))
            # add page_url to queue and html string to html_sring
            Spider.add_links_to_queue(Spider.gather_links(self,page_url),links_limit=Spider.links_limit)
            # remove page_url from queue and add to crawled (cause has been crawled)
            list_remove(value=page_url,my_list=Spider.queue)
            list_add(value=page_url,my_list=Spider.crawled) if self.html_string_status else None
            # Sort the links
            Spider.queue=Spider.sort_links(keywords=Spider.sort_keywords_list,target_list=Spider.queue)    
            Spider.crawled=Spider.sort_links(keywords=Spider.sort_keywords_list,target_list=Spider.crawled)        
            # update queue and crawled files
            Spider.update_files()
      
            
    
    def gather_links(self,page_url) -> (List[str],str):
        """_summary_
        this method gather links from the html page
        these links will be processed by the claw method
        
        args:
        html_string (str): html page to be process
        """        
        html_string = None
        self.html_string_status = False
        
        # use link_finder to gather links from html_string
        finder = LinkFinder(Spider.base_url,page_url)
        header = {  'User-Agent': 
                    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36'}
        
        try:
            request = Request(page_url,headers=header)
            response = urlopen(request)
            response_status = response.getcode()
            
            # Checking if the response is valid
            if 200 <= response_status < 300:
                
                # Checking if the response is html
                if 'text/html' in response.getheader('Content-Type'):
                    html_bytes = response.read()
                    html_string = html_bytes.decode('utf-8')
                    self.html_string_status = True  
                    
            if self.html_string_status:

                # process html_string and extract links and stores in _links
                finder.feed(html_string)
                
                # add html_string to html list
                list_add(value=html_string,my_list=Spider.html) 
            
            # detect the language of the html_string and add it to html_lang list   
            language = response.getheader('Content-Language')
            
            if language == None:
            
                soup = BeautifulSoup(html_string, 'html.parser')                                                 # create a soup object
                list_of_string = soup.text.split("\n")                                                          # get the text of the soup object
                first_string = [part_of_text.strip(" ") for part_of_text in list_of_string if part_of_text]     # get only the words

                max_string = max(first_string, key=len)                                                                  # get the largest sentence
                language = detect(max_string)                                                                            # detect the language
                    

            Spider.html_lang.append(language)

        except Exception as e:
            # logger.error(f'Page {page_url} could not be crawled due to {str(e)} will be excluded from the queue')
            self.html_string_status = False
            return list()
        
        return finder.page_links()
    
    
    # Sorts the links based on specified keywords
    @staticmethod
    def sort_links(keywords:list[str],
                   target_list:list[str]) -> None:
      
        def key_func(url_list):
            for keyword in keywords:
                if keyword in url_list:
                    return keyword
            return ''
        
        return sorted(target_list, key=key_func,reverse=True)
    


    @staticmethod
    def add_links_to_queue(links:list,links_limit:int) -> None:
        """_summary_
        This add the links to the queue if they are not already in the 
        queue or crawled
        The domain name is checked to make sure that the links are 
        from the same domain
        
        args:
        links (set): set of links obtained from the html page
        
        """
        
       
        
        # Sort temporary list
        links=Spider.sort_links(keywords=Spider.sort_keywords_list,
                          target_list=links)
        
        # avoid duplication
        links=[link for link in links if link not in Spider.queue and link not in Spider.crawled]
        # avoid external links
        links=[link for link in links if Spider.domain_name == get_domain_name(link)]
        # avoid links that are not html
        links=[link for link in links if not contain_values(link,Spider.exception_list)]
        # limit to N links
        links=links[:links_limit]  
        # logger.info(f'Adding {len(links)} links to queue')
        
        
        for url in links:
            # add url to queue
            list_add(value=url,my_list=Spider.queue)
    
            
            
    @staticmethod
    def update_files():
        list_to_file(links=Spider.queue, 
                    file=Spider.queue_file,
                    project_name=Spider.project_name,
                    url_base=Spider.base_url)
        
        list_to_file(links=Spider.crawled,
                    file=Spider.crawled_file,
                    project_name=Spider.project_name,
                    url_base=Spider.base_url,
                    html_string=Spider.html,
                    html_lang=Spider.html_lang)

        
    