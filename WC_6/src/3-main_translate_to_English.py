
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
 
   

    # 3. translate to english
    
    parquet_file_path_translation=results_path.joinpath('combined_file_after_translation.parquet')
    if not parquet_file_path_translation.exists():
        
        parquet_file_path_html = results_path.joinpath('combined_file_html_to_text.parquet')
        full_df=pd.read_parquet(parquet_file_path_html)
        
        logger.info(f'Translating to english')
        full_df.reset_index(inplace=True)
        not_english_df = full_df[full_df['html_lang'] != 'en']
        english_df = full_df[full_df['html_lang'] == 'en']
        
        # initialize translator
        translator = Translator()
        
        # run translation
        not_english_df['translated_text'] = not_english_df['text'].progress_apply(lambda text: translator.translate_text_short(text))
        english_df['translated_text'] = english_df['text']

        # reseting index
        not_english_df.reset_index(drop=True, inplace=True)
        english_df.reset_index(drop=True, inplace=True)
        
        # concat dataframes
        final_df = pd.concat([not_english_df, english_df], ignore_index=True)
        final_df.to_parquet(parquet_file_path_translation,index=False,engine='xlsxwriter')
    else:
        logger.info(f'Excel file already exists. Skipping translation.')
     

    
    



