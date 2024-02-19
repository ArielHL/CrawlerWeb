
# import external libraries
from pathlib import Path
import pandas as pd
import json
from tqdm import tqdm
import logging
from collections import Counter
import warnings
warnings.filterwarnings("ignore")

# import internal libraries
from MiddleWares.CustomLogger import CustomLogger
from MiddleWares.Translate_v1 import Translator
from MiddleWares.middlewares import multi_count_words_in_list


tqdm.pandas()


output_path=Path(__file__).parents[2].joinpath('Output','Companies')
results_path=Path(__file__).parents[2].joinpath('Output','Results_3')
results_path.mkdir(parents=True, exist_ok=True)

logger_path = Path(__file__).parents[0].joinpath('Logs')
logger_path.mkdir(parents=True, exist_ok=True)
logger_file=logger_path.joinpath('log.txt')


# setting the logger

logger = CustomLogger(name=__name__, 
                             level=logging.INFO,
                             logger_file=logger_file)

# *********************************************************************************************************************
   
if __name__ == '__main__':
    
    logger.enable_terminal_logging()

    # 1. read all crawled files
    parquet_file_path_html = results_path.joinpath('combined_file_html_to_text.parquet')
    full_df=pd.read_parquet(parquet_file_path_html)

    # 1.1 reading keywords
    source_path = Path(__file__).parents[1].joinpath('Source')
    keywords_source = results_path.joinpath('Keywords_Full.xlsx')
    keywords=pd.read_excel(keywords_source)
    keywords['keyId']=keywords['lang']+'-'+keywords['Keyword_original']

    # 4. counting words
    logger.info(f'Counting words in text')

    word_count_df = full_df.progress_apply(lambda row: multi_count_words_in_list(row=row,target_df=keywords),axis=1).apply(pd.Series)

    word_count_final=(word_count_df.T
        .reset_index()
        .rename(columns={'index':'keyword'})
        .merge(keywords,how='inner',left_on='keyword',right_on='keyId')
        .set_index(['Keyword_original'])
        .drop(columns=['Keywords','keyword','lang','keyId'])
        
        .T
        )

    # unify df
    final_df = pd.concat([full_df, word_count_final], axis=1)
    final_df.reset_index(drop=True, inplace=True)

    final_reorganized=(final_df.T
    .reset_index()
    .groupby('index').sum()
    .T
        )

    # reorganize columns
    desired_order=['Project','url_base','url','linkedin_profile','html_lang','text']
    remaining_columns=sorted(set(final_reorganized.columns)-set(desired_order))
    new_column_order=desired_order+remaining_columns
    export_df=final_reorganized[new_column_order]

    export_df['html_lang'].replace(0, pd.np.nan, inplace=True)
    export_df['linkedin_profile'].replace(0, pd.np.nan, inplace=True)

    data_dict=export_df.to_json(orient='records',lines=True)

    # 5. Combining files

    # reading summary file
    summary_df=pd.read_excel(results_path.joinpath('Summary.xlsx'))
    df_total=pd.merge(summary_df,export_df,how='left',on='url_base',suffixes=('_summ','_count'))

    # Saving results

    logger.info(f'Saving results')
    # defining paths
    parquet_file=results_path.joinpath('Final_file_Counting_words.parquet')
    json_file=results_path.joinpath('Final_file_Counting_words.json')
    excel_file=results_path.joinpath('Final_file_Counting_words.xlsx')
    final_results_excel_path=results_path.joinpath('Final_results.xlsx')

    # saving files
    data_dict=export_df.to_json(orient='records',lines=True)
    export_df.to_parquet(parquet_file,index=True)

    with open(json_file, 'w') as json_file:
        json_file.write(data_dict)

    export_df.drop('text',axis=1).to_excel(excel_file,index=False,engine='openpyxl')
    df_total.drop('text',axis=1).to_excel(final_results_excel_path,index=False,engine='openpyxl')

    logger.info(f'Finished')