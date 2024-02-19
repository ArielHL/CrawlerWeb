# %%
# import external libraries
from pathlib import Path
import os
import time
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from tqdm import tqdm
import logging
from collections import Counter
import warnings
warnings.filterwarnings("ignore")

# import internal libraries

from MiddleWares.CustomLogger import CustomLogger
from MiddleWares.middlewares import *

tqdm.pandas()



output_path=Path(__file__).parents[2].joinpath('Output','Companies')
results_path=Path(__file__).parents[2].joinpath('Output','Results_3')
results_path.mkdir(parents=True, exist_ok=True)
source_path = Path(__file__).parents[1].joinpath('Source')

logger_path = Path(__file__).parents[0].joinpath('Logs')
logger_path.mkdir(parents=True, exist_ok=True)
logger_file=logger_path.joinpath('log.txt')


# setting the logger

logger = CustomLogger(name=__name__, 
                             level=logging.INFO,
                             logger_file=logger_file)


if __name__ == '__main__':
    
    logger.enable_terminal_logging()

    # 1. read the combined file with languages
    logger.info(f'Reading the combined file with languages')
    language_base= results_path.joinpath('combined_file_html_to_text.parquet')
    if language_base.exists(): 
        df_language=pd.read_parquet(language_base)

    # 2. Generate a list of languages
    logger.info(f'Generating a list of languages')
    language_raw=df_language[df_language['html_lang'].notna()]['html_lang'].unique().tolist()
    language_list=[lang[:2] for lang in language_raw]

    # 3. Read the keyword files
    logger.info(f'Reading the keyword files')
    keyword_source= source_path.joinpath('Keywords_Base.xlsx')
    if keyword_source.exists():
        keywords_original=pd.read_excel(keyword_source)


    # 4. Translate the Keywords to all languages detected in the combined file
    logger.info(f'Translating the Keywords to all languages detected in the combined file') 
    Keyword_full=pd.DataFrame()

    with tqdm(total=len(language_list)) as pbar:
        for language in tqdm(language_list):    
            df_temp=pd.DataFrame()
            df_temp[['Keywords','lang','Keyword_original']] = keywords_original['Keywords'].apply(lambda text: translate_text(text, 'en', language))
            Keyword_full=pd.concat([Keyword_full,df_temp],axis=0)
            pbar.update(1)

    # 5. Write the translated keywords to the keyword files
    logger.info(f'Writing the translated keywords to the keyword files')
    Keyword_full.to_excel(results_path.joinpath('Keywords_Full.xlsx'),index=False)




