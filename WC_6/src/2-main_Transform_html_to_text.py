
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
from MiddleWares.middlewares import count_words_in_list, html_to_text, df_combiner,neo_html_to_text


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
CHUNK_SIZE = 500

# *********************************************************************************************************************


   
if __name__ == '__main__':
    
    logger.enable_terminal_logging()
 
    logger.info(f'Starting process \nCombining all crawled files into one file') 

    # 1. read all crawled files
    parquet_file_path_combined = results_path.joinpath('combined_file_html.parquet')
    if not parquet_file_path_combined.exists(): 
    
        temp_df=df_combiner(output_path=output_path)   
        full_df=temp_df.copy(deep=False)
        
        temp_df.to_parquet(parquet_file_path_combined)
    
    else:
        logger.info(f'Parquet file already exists. Skipping combining files.\nLoading parquet file into dataframe')
        temp_df=pd.read_parquet(parquet_file_path_combined)
        full_df=temp_df.copy(deep=False)
    
    # 2. convert html in text
    logger.info(f'Converting html to text')
    parquet_file_path_html = results_path.joinpath('combined_file_html_to_text.parquet')
    if not parquet_file_path_html.exists(): 
        
        num_chunks = len(temp_df) // CHUNK_SIZE + (len(temp_df) % CHUNK_SIZE != 0)
        for i in range(num_chunks):
            logger.info(f'Starting Chunk: {i+1} of {num_chunks}')
            start_idx = i * CHUNK_SIZE
            end_idx = (i + 1) * CHUNK_SIZE
            chunk_temp_df = temp_df[start_idx:end_idx]

            # tqdm.pandas()
            # chunk_temp_df["text"] = chunk_temp_df["html_string"].progress_apply(lambda html: html_to_text(html, logger))
            
            chunk_temp_df['text'] = pd.Series(neo_html_to_text(chunk_temp_df['html_string'], logger)) 
            full_df.update(chunk_temp_df)
              
        full_df.drop(columns="html_string", inplace=True)
        logger.info(f'Converting html to text completed')
        full_df.to_parquet(parquet_file_path_html)
else:
    logger.info(f'Excel file already exists. Skipping conversion.')
     
    
    
    
    



