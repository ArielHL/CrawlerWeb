from pathlib import Path
import pandas as pd
import json
import numpy as np
import time
import regex as re
from bs4 import BeautifulSoup
from swifter import set_defaults
import swifter
from tqdm import tqdm
import logging
from collections import Counter

set_defaults(progress_bar=True,
             allow_dask_on_strings=True,
             force_parallel=True,
             npartitions=64)
tqdm.pandas()


output_path=Path(__file__).parents[2].joinpath('Output','Companies')
results_path=Path(__file__).parents[2].joinpath('Output','Results')
results_path.mkdir(parents=True, exist_ok=True)

logger_path = Path(__file__).parents[0].joinpath('Logs')
logger_path.mkdir(parents=True, exist_ok=True)
logger_file=logger_path.joinpath('log.txt')


# setting the logger

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler(logger_file), logging.StreamHandler()]
)

# *********************************************************************************************************************

WORDS_TO_COUNT = ['DATA ANALYTICS','GEN AI','M&A','DATA SCIENCE']

# *********************************************************************************************************************

def html_transform():
    
    full_df = pd.DataFrame()

    for company_dir in output_path.iterdir():
        if company_dir.is_dir():
            crawled_file = company_dir / "crawled.json"
            if crawled_file.is_file():
                try:
                    with open(str(crawled_file),'r') as f:
                        data = json.load(f)
                    max_length = max(len(data['url']), len(data['html_lang']), len(data['html_string']))
                    df_data = {
                            'Project': [data['Project']] * max_length,
                            'url_base': [data['url_base']] * max_length,
                            'url': data['url'] + [''] * (max_length - len(data['url'])),
                            'html_lang': data['html_lang'] + [''] * (max_length - len(data['html_lang'])),
                            'html_string': data['html_string'] + [''] * (max_length - len(data['html_string'])),
                    }
                    df = pd.DataFrame(df_data)
                    full_df = pd.concat([full_df, df], ignore_index=True)
                except Exception as e:
                    print(f'Error: {e}')
    # Replace White Space with Null
    full_df.replace('', np.NaN, inplace=True)
    # Drop Null Values
    full_df.dropna(inplace=True)
    
    return full_df


def df_combiner():
    
    full_df = pd.DataFrame()

    for company_dir in output_path.iterdir():
        if company_dir.is_dir():
            crawled_file = company_dir / "crawled_df.parquet"
            if crawled_file.is_file():
                try:
                    df=pd.read_parquet(crawled_file)
                    full_df = pd.concat([full_df, df], ignore_index=True)
                except Exception as e:
                    print(f'Error: {e}')
    
    full_df.dropna(inplace=True)
    
    return full_df


# Function to count occurrences of target words in a list
def count_words_in_list(sentence_list, target_words):
    sentence_lower = ' '.join(sentence_list).lower()
    word_counts = {word: sentence_lower.count(word.lower()) for word in target_words}
    return word_counts
    

def html_to_text(html: str):
    try:
        soup = BeautifulSoup(html, 'html.parser')
        text = soup.text
        list_of_string = soup.text.split("\n")   
        final_text = [part_of_text.strip(" ") for part_of_text in list_of_string if part_of_text]   

        return final_text
    except Exception:
        #print("Error with html: ", html)
        pass
    
    
    
if __name__ == '__main__':
    
    logger.info(f'Starting process \nCombining all crawled files into one file') 
    full_df=df_combiner()   
    
    logger.info(f'Converting html to text')
    full_df["text"] = full_df.html_string.swifter.apply(lambda html: html_to_text(html))
        
    full_df.drop(columns="html_string", inplace=True)
    logger.info(f'Converting html to text completed')
    
    logger.info(f'Counting words in text')
    word_count_df = full_df['text'].apply(lambda lst: count_words_in_list(lst, WORDS_TO_COUNT)).apply(pd.Series)
    full_df = pd.concat([full_df, word_count_df], axis=1)
    
    logger.info(f'Saving results to parquet and excel files')
    parquet_file=results_path.joinpath('combined_file.parquet')
    excel_file=results_path.joinpath('combined_file.xlsx')

    full_df.to_parquet(parquet_file)
    full_df.to_excel(excel_file,index=False)
    



