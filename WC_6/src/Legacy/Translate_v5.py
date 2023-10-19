import pandas as pd
from deep_translator import GoogleTranslator
from concurrent.futures import ThreadPoolExecutor,as_completed
from tqdm import tqdm

class Translator:
    # Defining Model
    translator_model = GoogleTranslator(source='auto', target='en')

    def __init__(self, translate_model: object = translator_model):
        Translator.translate_model = translate_model

    @staticmethod
    def translate_chunk(chunk):
        translated_chunk = Translator.translate_model.translate(chunk)
        return translated_chunk

    def translate_multi(self, sentence, max_chunk_length: int = 500):
        
        # Split the sentence into chunks of max_chunk_length
        chunks = [sentence[i:i + max_chunk_length] for i in range(0, len(sentence), max_chunk_length)]
        # Translate each chunk and join the translated chunks
        translated_text = ' '.join(
            chunk_translation for chunk_translation in map(self.translate_chunk, chunks)
        )
        return translated_text
    
    def translate_df(self,
                     df: pd.DataFrame,
                     column_name: str = 'plain_text',
                     new_column_name: str = 'translated_review',
                     max_workers: int = 30,
                     ) -> pd.DataFrame:
        
        translated_reviews = []
        with ThreadPoolExecutor(max_workers=max_workers) as executor, tqdm(total=len(df)) as pbar:
            for translated_review in tqdm(executor.map(self.translate_multi, df[column_name]), total=len(df)):
                translated_reviews.append(translated_review)
                pbar.update(1)
        df[new_column_name] = translated_reviews
        return df