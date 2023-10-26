import pandas as pd
from googletrans import Translator
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm
import time
import random

class Translator:

          
    model = Translator()
    
    @staticmethod
    def translate_text(sentence):
        
        # Split the sentence into chunks of 500 words each
        words = sentence.split()
        chunks = [words[i:i+490] for i in range(0, len(words), 490)]
        
        # Translate each chunk and join them back into a single sentence
        translated_chunks = []
        for chunk in chunks:
            chunk_sentence = ' '.join(chunk)
            translated_chunk = Translator.model.translate(chunk_sentence, dest='en').text
            translated_chunks.append(translated_chunk)
            time.sleep(random.random()*2)
        
        # Join the translated chunks into a single sentence
        translated_text = ' '.join(translated_chunks)
        return translated_text


    @staticmethod
    def translate_dataframe(df:pd.DataFrame,
                            column_name:str,
                            num_workers:int=4):
        
        df['indexed_text']=df.index.astype(str)+'_'+df[column_name]
        
        # Convert the DataFrame to a list of dictionaries for executor.map
        rows_as_dicts = df.to_dict(orient='records')

        def translate_row(row):
            translated_text = Translator.translate_text(row['indexed_text'])
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

        






