
# from urllib.parse import urlparse
from html.parser import HTMLParser
from urllib.parse import urljoin
from urllib.request import urlopen
import logging
from MiddleWares.middlewares import list_add, list_remove

# *********************************************************************************************

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler('src/WC_4/Logs/log.txt'), logging.StreamHandler()]
)

# *********************************************************************************************


class LinkFinder(HTMLParser):
    
    def __init__(self,base_url,page_url,max_links=100):
        super().__init__()
        self._base_url = base_url
        self._page_url = page_url
        self._links = list()
        self._max_links = max_links
        self._links_extracted = 0
    

    def error(self,message):
        pass
    
    # will find all links in a page and add to the set
    def handle_starttag(self,tag,attrs):

        if tag in ['a','link']:
            for (attr,value) in attrs:
                if attr == 'href':
                    if value != '':
                        url = urljoin(self._base_url,value)
                        # add here a sort logic
                        list_add(value=url,my_list=self._links)
            
                   

    def page_links(self):   
        return self._links






