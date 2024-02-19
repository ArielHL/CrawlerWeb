import pandas as pd
from deep_translator import GoogleTranslator
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm
import time
import random
from collections import OrderedDict

class Translator:



    def translate_text(self, 
                       text:str,
                       source_lang:str,
                       target_lang:str) -> str:
        
        return GoogleTranslator(source=source_lang, target=target_lang).translate(text)

    
        
  

        






