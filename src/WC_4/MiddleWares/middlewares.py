
import os
import concurrent.futures
from typing import *
import requests
from urllib.parse import urlparse
from urllib.request import urlopen
import pandas as pd
import numpy as np
import json
import tldextract
from langdetect import detect



# create a project directory
def create_project_dir(directory:str) -> None:
    """_summary_
    create a directory if does not exist

    Args:
        directory (str): _description_
    """
    if not os.path.exists(directory):
        print('Creating project ' + directory)
        os.makedirs(directory)  # create a directory


def check_url_type(page_url):
    try:
        response = urlopen(page_url)
        if 'text/html' in response.getheader('Content-Type'):
            return True
        else:
            return False
    except Exception as e:
    
        return False


def create_data_files(project_name:str, base_url:str):
    """_summary_
    create queue and crawled files if not created

    Args:
        directory (str): _description_
        base_url (str):
"""

    queue=os.path.join(project_name, 'queue.json')
    crawled=os.path.join(project_name, 'crawled.json')
    
    queue_dict={'Project':project_name,'url_base':base_url,'url':[base_url]}
    crawled_dict={'Project':project_name,'url_base':base_url,'url':list(),'html_string':list(),'html_lang':list()}
    
    
    # Check if the file exists
    if not os.path.isfile(queue):
        write_file(path=queue, data_dict=queue_dict)
    # Check if the file exists
    if not os.path.isfile(crawled):
        write_file(path=crawled,data_dict=crawled_dict)
    
    
def write_file(data_dict:dict,path:str) -> None:
    
    """_summary_
    create a new file and write data

    Args:
        path (str): _description_
        data (str): _description_
    """

    
    with open(path,'w') as f:
        json.dump(data_dict,f)


def file_to_list(file_name:str,dict_key:str) -> List[str]:
  
    with open(file_name,'rb') as f:
        json_data = json.load(f)
        
    return list(json_data[dict_key])


def list_to_file(links:list,
                file:str,
                project_name:str,
                url_base:str,
                html_string:list=None,
                html_lang:list=None) -> None:
   

    if 'crawled' in file:
        
        crawler_dict={'Project':project_name,'url_base':url_base,'url':list(links),
                     'html_string':list(html_string),'html_lang': list(html_lang)}
    
        with open(file,'w') as f:
            json.dump(crawler_dict,f)
            
    if 'queue' in file:
        
        queue_dict={'Project':project_name,'url_base':url_base,'url':list(links)}

        with open(file,'w') as f:
            json.dump(queue_dict,f)

        
def list_add(value:str,
             my_list:list) -> None:
    
    if value not in my_list:
        my_list.append(value)

def list_remove(value:str,
                my_list:list) -> None:
    
    if value in my_list:
        my_list.remove(value)       
        
   
        
def get_domain_name(url):
    try:
        results = tldextract.extract(url).domain
        return results
        
        
    except Exception as e:
        print(str(e))
        return ''
    
    
def contain_values(url,values):
    return any(value in url for value in values)


def check_url_wrapper(url):
    try:
        response = requests.head(url)
        content_type = response.headers.get("content-type", "")
        if "text/html" in content_type or "text/plain" in content_type:
            return url, True
        else:
            return url, False
    except Exception as e:
        return url, False
    
def check_url(url_list):
    
    new_list= []
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=100) as executor:
        results = list(executor.map(check_url_wrapper, url_list))

    for url, is_html in results:
        if is_html:
           
            new_list.append(url)

    return new_list