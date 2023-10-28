
import os
from pathlib import Path
import concurrent.futures
from typing import *
import requests
from urllib.parse import urlparse
from urllib.request import urlopen
import pandas as pd
import numpy as np
import json
import tldextract
import threading
import re


# create a project directory
def create_project_dir(directory:str) -> None:
    """_summary_
    create a directory if does not exist

    Args:
        directory (str): _description_
    """
    if not directory.exists():
        
        directory.mkdir(parents=True, exist_ok=True) # create a directory


def check_url_type(page_url):
    try:
        response = urlopen(page_url)
        if 'text/html' in response.getheader('Content-Type'):
            return True
        else:
            return False
    except Exception as e:
    
        return False


def create_data_files(  project_name:str,
                        queue_file:Path,
                        crawled_df_file:Path,
                        base_url:str):

    """_summary_
        create a queue and crawled files if not created
        
        args:
        project_name (str): name of the project
        queue_file (str): path to the queue file
        crawled_file (str): path to the crawled file
        base_url (str): base url of the project
        
    """

    queue_dict={'Project':project_name,'url_base':base_url,'url':[base_url]}
    crawled_dict={'Project':project_name,'url_base':base_url,'url':list(),'html_string':list(),'html_lang':list()}
    
    crawled_df=pd.DataFrame(crawled_dict)

    # Check if the file exists
    if not queue_file.exists():
            write_file(path=queue_file, data_dict=queue_dict)
            

    if not crawled_df_file.exists():
            crawled_df.to_parquet(crawled_df_file,index=False)
    
def write_file(data_dict:dict,path:str) -> None:
    
    """_summary_
    create a new file and write data

    Args:
        path (str): _description_
        data (str): _description_
    """

    
    with path.open(mode='w') as f:
        json.dump(data_dict,f)


def file_to_list(file_name:Path,dict_key:str) -> List[str]:
  
    with file_name.open(mode='rb') as f:
        json_data = json.load(f)
        
    return list(json_data[dict_key])



def file_to_df(file_name:Path) -> pd.DataFrame:
        
        return pd.read_parquet(file_name)
   



def df_to_file(df:pd.DataFrame,file_name:Path) -> None:
    
    df.to_parquet(file_name,index=False)
    df.to_csv(file_name.with_suffix('.csv'))




def list_to_file(links:list,
                file:Path,
                project_name:str,
                url_base:str,
                html_string:list=None,
                html_lang:list=None) -> None:
   

    if 'crawled' in file.stem:
        
        crawler_dict={'Project':project_name,'url_base':url_base,'url':list(links),
                     'html_string':list(html_string),'html_lang': list(html_lang)}
    
        with file.open(mode='w') as f:
            json.dump(crawler_dict,f)
            
    if 'queue' in file.stem:
        
        queue_dict={'Project':project_name,'url_base':url_base,'url':list(links)}

        with file.open(mode='w') as f:
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

def remove_special_characters(input_string):
    # Use regular expression to remove special characters, excluding spaces
    cleaned_string = re.sub(r'[^a-zA-Z\s]', '', input_string)
    cleaned_string = cleaned_string.strip()
    cleaned_string = cleaned_string.encode('utf-8')
    cleaned_string = cleaned_string.decode('utf-8', 'ignore')
    return cleaned_string