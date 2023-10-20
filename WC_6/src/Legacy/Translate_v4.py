import pandas as pd
from deep_translator import GoogleTranslator
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm
import dl_translate as dlt




class Translator:
    
    def __init__(self,model:str='m2m100',device:str='auto'):
        self.model=model
        self.device=device
        self.mt=dlt.TranslationModel(model_or_path=self.model,device=self.device)



    @staticmethod
    def translate(text,source_lng:str,target_lng:str='en',verbose:bool=True,batch_size:int=32):
        # translate the text into english
        translated = Translator.mt.translate(text,source=source_lng,target=target_lng,verbose=verbose,batch_size=batch_size)
        return translated

    @staticmethod
    def translate_multi(sentence):
        if len(sentence) < 500:
            translated_text = Translator.translate_model.translate(sentence)
            return translated_text
        else:
            translated_text = Translator.translate_model.translate(sentence[:500])
            return translated_text

    def translate_df(self, 
                     df:pd.DataFrame, 
                     column_name:str='plain_text', 
                     new_column_name:str='translated_review',
                     max_workers:int=30,):
        
        translated_reviews = []
        with ThreadPoolExecutor(max_workers=max_workers) as executor, tqdm(total=len(df)) as pbar:
            for translated_review in tqdm(executor.map(self.translate_multi, df[column_name]), total=len(df)):
                translated_reviews.append(translated_review)
                pbar.update(1)
        df[new_column_name] = translated_reviews
        return df





    
    


