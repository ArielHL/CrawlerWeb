import pandas as pd
from deep_translator import GoogleTranslator
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm


#


class Translator:
    
    # Defining Model
    translator_model=GoogleTranslator(source='auto', target='en')
    
    def __init__(self, translate_model:object=translator_model):
        Translator.translate_model = translate_model

 

    @staticmethod
    def translate_multi(sentence):
        translated_text = Translator.translate_model.translate(sentence)
        return translated_text


    def translate_df(self, 
                     df:pd.DataFrame, 
                     column_name:str='plain_text', 
                     new_column_name:str='translated_review',
                     max_workers:int=30):
        
        translated_reviews = []
        max_workers = 30
        with ThreadPoolExecutor(max_workers=max_workers) as executor, tqdm(total=len(df)) as pbar:
            for translated_review in tqdm(executor.map(self.translate_multi, df[column_name]), total=len(df)):
                translated_reviews.append(translated_review)
                pbar.update(1)
        df[new_column_name] = translated_reviews
        return df





    
    


