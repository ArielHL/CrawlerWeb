import pandas as pd
from deep_translator import GoogleTranslator
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm
import time
import random
from collections import OrderedDict

class Translator:
    
    # Defining Model
    translator_model=GoogleTranslator(source='auto', target='en')
    
    def __init__(self, translate_model:object=translator_model):
        Translator.translate_model = translate_model
        
    @staticmethod
    def translate_text(sentence):
        
        # Split the sentence into chunks of 500 words each
        words = sentence.split()
        chunks = [words[i:i+450] for i in range(0, len(words), 450)]
        
        # Translate each chunk and join them back into a single sentence
        translated_chunks = []
        for chunk in chunks:
            chunk_sentence = ' '.join(chunk)
            translated_chunk = Translator.translate_model.translate(chunk_sentence)
            time.sleep(1.5)
            
            translated_chunks.append(translated_chunk)
        
        # Join the translated chunks into a single sentence
        translated_text = ' '.join(translated_chunks)
        return translated_text

    @staticmethod
    def translate_text_short(sentence):
        
        if len(sentence) > 4500:
            sentence = sentence[:4500]
        translated_text = Translator.translate_model.translate(sentence)
        return translated_text 

    @staticmethod
    def translate_dataframe(df, column_name,num_workers:int=4):
        
        df['indexed_text']=df.index.astype(str)+'_'+df[column_name]
        
        # Convert the DataFrame to a list of dictionaries for executor.map
        rows_as_dicts = df.to_dict(orient='records')

        def translate_row(row):
            translated_text = Translator.translate_text_short(row['indexed_text'])
            time.sleep(2)
            return row['index'], translated_text
    

        # Use multithreading to translate rows concurrently while preserving order
        with ThreadPoolExecutor(max_workers=num_workers) as executor:
            translated_results = list(tqdm(executor.map(translate_row, rows_as_dicts), total=len(df)))
            
         # Reorder the translated results based on the original index
        translated_results.sort(key=lambda x: x[0])
        
        # Extract the translated texts from the results
        translated_texts = [text for index, text in translated_results]

        df['translated_text'] = translated_texts
        return df

        






