import pandas as pd
from deep_translator import GoogleTranslator
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm

class Translator:
    
    # Defining Model
    translator_model=GoogleTranslator(source='auto', target='en')
    
    def __init__(self, translate_model:object=translator_model):
        Translator.translate_model = translate_model


 

    @staticmethod
    def translate_multi(sentence,index):
        if len(sentence) > 500:
            sentence = sentence[:500]
        translated_text = Translator.translate_model.translate(sentence)
        return translated_text,index


    def translate_df(self, 
                     df: pd.DataFrame, 
                     column_name: str = 'plain_text', 
                     new_column_name: str = 'translated_review',
                     max_workers: int = 30):
        
        translated_dict = {}
        with ThreadPoolExecutor(max_workers=max_workers) as executor, tqdm(total=len(df)) as pbar:
            for translated_review, index in tqdm(executor.map(self.translate_multi, df[column_name], df.index), total=len(df)):
                translated_dict[index] = translated_review
                pbar.update(1)
        
        # Sort the dictionary based on keys (indices)
        sorted_translations = [translated_dict[index] for index in sorted(translated_dict.keys())]
        control=pd.DataFrame(sorted_translations)
        control.to_excel('control.xlsx')
        # Update the DataFrame column with sorted translated texts
        df[new_column_name] = sorted_translations
        return df





    
    


