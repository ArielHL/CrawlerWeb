
# import external libraries
from pathlib import Path
import pandas as pd

from tqdm import tqdm
import logging
from collections import Counter
import warnings
warnings.filterwarnings("ignore")

# import internal libraries
from MiddleWares.CustomLogger import CustomLogger
from MiddleWares.Translate_v1 import Translator
from MiddleWares.middlewares import count_words_in_list, html_to_text, df_combiner


tqdm.pandas()


output_path=Path(__file__).parents[2].joinpath('Output','Companies')
results_path=Path(__file__).parents[2].joinpath('Output','Results')
results_path.mkdir(parents=True, exist_ok=True)

logger_path = Path(__file__).parents[0].joinpath('Logs')
logger_path.mkdir(parents=True, exist_ok=True)
logger_file=logger_path.joinpath('log.txt')


# setting the logger

logger = CustomLogger(name=__name__, 
                             level=logging.INFO,
                             logger_file=logger_file)

# *********************************************************************************************************************

WORDS_TO_COUNT = ['Assisted Living','Nursing Home','Senior Care Facility','Independent Living','Memory Care']

# *********************************************************************************************************************


   
if __name__ == '__main__':
    
    logger.enable_terminal_logging()
    
    logger.info(f'Starting process \nCounting Words') 
    
    # 1.1 reading keywords
    source_path = Path(__file__).parents[1].joinpath('Source')
    keywords_source = source_path.joinpath('Keywords List.xlsx')
    WORDS_TO_COUNT=pd.read_excel(keywords_source)['Keywords'].tolist()
 
    parquet_file_path_html = results_path.joinpath('combined_file_before_translation.parquet')
    if not parquet_file_path_html.exists():
        
        # 1. read all crawled files
        full_df=df_combiner(output_path=output_path)   
        # 2. convert html in text
        logger.info(f'Converting html to text')
        tqdm.pandas()
        full_df["text"] = full_df["html_string"].progress_apply(lambda html: html_to_text(html, logger))   
        full_df.drop(columns="html_string", inplace=True)
        logger.info(f'Converting html to text completed')
        full_df.to_parquet(parquet_file_path_html)
    else:
        logger.info(f'Excel file already exists. Skipping conversion.')
        full_df=pd.read_parquet(parquet_file_path_html)
    
    
    logger.info(f'Combaining all crawled files into one file completed')
    



