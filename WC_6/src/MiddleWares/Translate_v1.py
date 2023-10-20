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
    def translate_multi(sentence):
        if len(sentence) > 500:
            sentence = sentence[:500]
        translated_text = Translator.translate_model.translate(sentence)
        return translated_text


