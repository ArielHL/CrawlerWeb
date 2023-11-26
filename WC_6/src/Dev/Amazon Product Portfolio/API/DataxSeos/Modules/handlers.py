import pandas as pd
import json
import time
from tqdm import tqdm


def from_json_to_df(merchant:str,
                    task_type:str,
                    path:str=None,
                    is_json:bool=False,
                    data:str=None
                    ) -> pd.DataFrame:
    
    if is_json:
        if path:
            # reading json file
            result=read_json_to_dict(filename=path)
        else:
            raise Exception('No path provided')    
        
    else:
        result=data
    
    if task_type=='products':
       
        # get item list
        item_list=result['tasks'][0]['result'][0]['items']
        
        # Create DataFrame
        df=pd.DataFrame(item_list)
        
        # expand dataframe with info data
        if merchant=='google':
           
            df= pd.concat([df,df['product_rating'].apply(pd.Series,dtype='object')],axis=1)
            df= pd.concat([df,df['delivery_info'].apply(pd.Series,dtype='object')],axis=1)
            df= pd.concat([df,df['delivery_price'].apply(pd.Series,dtype='object')],axis=1)
            df= pd.concat([df,df['stores_count_info'].apply(pd.Series,dtype='object')],axis=1)
            df.drop(['product_rating','delivery_info','delivery_price',0,'stores_count_info'],axis=1,inplace=True)
        
        if merchant=='amazon':
         
            df= pd.concat([df,df['rating'].apply(pd.Series,dtype='object')],axis=1)
            df= pd.concat([df,df['delivery_info'].apply(lambda x: pd.Series(x, dtype=object))], axis=1)
            df.drop([0,'xpath','rating','delivery_info'],axis=1,inplace=True)
            df.drop_duplicates(subset=['data_asin'],keep='first',inplace=True)
            df.dropna(subset=['data_asin'],inplace=True)
        
        return df

    elif task_type=='reviews':
        
        # get item list
        item_list=result['tasks'][0]['result'][0]['items']
        
        # Create DataFrame
        df=pd.DataFrame(item_list)
        df['asin']=result['tasks'][0]['result'][0]['asin']
        df= pd.concat([df,df['rating'].apply(pd.Series,dtype='object')],axis=1)
        df= pd.concat([df,df['user_profile'].apply(pd.Series,dtype='object')],axis=1)
        df.drop(['rating','user_profile'],axis=1,inplace=True)
        
        return df
    
    else:
        raise Exception('Incorrect task type')

def read_json_path(client,
                merchant:str,
                task_type:str,
                task_id:str):

    # 2. Getting data path for json files
    data=client.get_task_data()['data']
    df_task=pd.DataFrame(data)

    reviews_path=df_task[df_task['task_file_name'].str.contains(task_id)]['task_file_name'].tolist()

    # 3. recover reviews files and transform in dataframes
    final_df=pd.DataFrame()
    for path in reviews_path:
        df=from_json_to_df(merchant=merchant,task_type=task_type,path=path,is_json=True)
        final_df=pd.concat([final_df,df],axis=0)
        
    return final_df
        

        
def save_dict_to_json(filename:str,data_dict:dict):
    if isinstance(filename, str) and isinstance(data_dict, dict):
        with open(filename, 'w') as outfile:
            json.dump(data_dict, outfile, indent=4)
    else:
        raise Exception('Incorrect input data')
    
def read_json_to_dict(filename:str):
    if isinstance(filename, str):
        with open(filename, 'r') as infile:
            data_dict = json.load(infile)
        return data_dict
    else:
        raise Exception('Incorrect input data')

def create_post(client:object,
                api_type:str,
                merchant:str,
                value_to_search:str,
                task_type:str,
                multiple_task:bool=False,
                **kwargs):
    
    if multiple_task==False:
    
        # define the post data for product download
        post_data = dict()
        post_data[0] = dict(
            location_name="Germany",
            language_name="English (United States)",
            tag=value_to_search
            )
        
        if task_type == "products" and isinstance(value_to_search, str):
            post_data[0].update(
                depth = kwargs.get("depth", 100),
                keyword=value_to_search
            )
            
        if (task_type == "asin" or task_type == 'reviews') and isinstance(value_to_search, str):  
            post_data[0].update(
                asin=value_to_search
            )
            
        
        # create a Task
        client.create_task( api_type=api_type, 
                            merchant=merchant,
                            post_data=post_data,
                            task_type=task_type)
        
    
    elif multiple_task==True:
        
        if isinstance(value_to_search, list):
            post_array=[]
            post_data = dict()
            
            for i, value in enumerate(value_to_search):
                
                post_data[i] = dict(
                    location_name="Germany",
                    language_name="English (United States)",
                    tag=value
                    )
                                
                if task_type == "products" and isinstance(value, str):
                    post_data[i].update(
                        depth = kwargs.get("depth", 100),
                        keyword=value
                    )
                    
                if (task_type == "asin" or task_type == 'reviews') and isinstance(value, str):  
                    post_data[i].update(
                        asin=value,
                        depth = kwargs.get("depth", 100)
                    )
                
                post_array.append((post_data[i]))
                

            # create a Task
            client.create_task( api_type=api_type,
                                merchant=merchant,
                                post_data=post_array,
                                task_type=task_type)