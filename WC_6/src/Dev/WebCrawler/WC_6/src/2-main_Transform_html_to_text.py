
# import external libraries
from pathlib import Path
import pandas as pd
import numpy as np
import regex as re
from bs4 import BeautifulSoup
from swifter import set_defaults
import swifter
from tqdm import tqdm
import logging
from collections import Counter
import warnings
warnings.filterwarnings("ignore")

# import internal libraries
from MiddleWares.Translate_v3 import Translator

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

WORDS_TO_COUNT = ['COMPRESSOR','BLOWER','CONTROL']

# *********************************************************************************************************************


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
def count_words_in_list(sentence, target_words):
    sentence_lower = sentence.lower()
    word_counts = {word: sentence_lower.count(word.lower()) for word in target_words}
    return word_counts
    

def html_to_text(html: str):
    try:
        soup = BeautifulSoup(html, 'html.parser')
        text = soup.text
        list_of_string = soup.text.split("\n")   
        final_text = [part_of_text.strip(" ") for part_of_text in list_of_string if part_of_text]   
        plain_text = ' '.join(final_text)
        plain_text = plain_text[:512]

        return plain_text
    
    except Exception as e:
        logger(f"Error: {str(e)} with html: ", html)
        pass
    

    
    
   

    
if __name__ == '__main__':
    
    # 1. read all crawled files
    logger.info(f'Starting process \nCombining all crawled files into one file') 
    full_df=df_combiner()   
    
    # 2. convert html in text
    logger.info(f'Converting html to text')
    full_df["text"] = full_df.html_string.swifter.apply(lambda html: html_to_text(html))
        
    full_df.drop(columns="html_string", inplace=True)
    logger.info(f'Converting html to text completed')
    
    # 3. translate to english
    logger.info(f'Translating to english')
    not_english_df = full_df[full_df['html_lang'] != 'en']
    
    translator = Translator()
    translated_df=translator.translate_df(df=not_english_df, column_name='text', new_column_name='translated_review')
    translated_df.drop(columns=['text'], inplace=True)
    translated_df.rename(columns={'translated_review':'text'}, inplace=True)
    
    full_df=full_df[full_df['html_lang'] == 'en'].reset_index(drop=True)
    translated_df=translated_df.reset_index(drop=True)
    
    final_df=pd.concat([full_df, translated_df])
    
    print(f'Original df shape: {full_df.shape}\nTranslated df shape: {translated_df.shape}\nFinal df shape: {final_df.shape}')
    

    # 4. counting words
    logger.info(f'Counting words in text')
    word_count_df = final_df['text'].apply(lambda lst: count_words_in_list(lst, WORDS_TO_COUNT)).apply(pd.Series)
    final_df = pd.concat([final_df, word_count_df], axis=1)
    final_df.reset_index(drop=True, inplace=True)
    
    
    logger.info(f'Saving results to parquet and excel files')
    parquet_file=results_path.joinpath('combined_file.parquet')
    excel_file=results_path.joinpath('combined_file.xlsx')

    final_df.to_parquet(parquet_file)
    final_df.to_excel(excel_file,index=False)
    



