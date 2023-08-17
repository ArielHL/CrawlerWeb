
from pathlib import Path
import pandas as pd
import json

output_path = Path(__file__).parents[2].joinpath('Output','Companies')

for company_dir in output_path.iterdir():
    if company_dir.is_dir():
        crawled_file = company_dir / "crawled.json"
        if crawled_file.is_file():
            with crawled_file.open() as f:
                crawled_dict = json.load(f)
                print(crawled_dict)



