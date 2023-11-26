from http.client import HTTPSConnection
from base64 import b64encode
from json import loads
from json import dumps,dump
import datetime
import time


from Modules.handlers import *


class RestClient:
    domain = "api.dataforseo.com"

    def __init__(self,  username:str, 
                        password:str,
                        database_path:str):
        
        self.username = username
        self.password = password
        self.database_path = database_path
        self.task_data = None
        
        
    def load_database(self, path:str) -> None:
        if isinstance(path, str):
            self.task_data=read_json_to_dict(filename=path)
        else:
            raise Exception("Incorrect input data")
    
    def save_database(self, path:str) -> None:
        if isinstance(path, str):
            save_dict_to_json(data_dict=self.task_data,filename=path)
        else:
            raise Exception("Incorrect input data")
    
    def get_task_data(self):
        self.load_database(path=self.database_path)
        return self.task_data



    def request(self, path, method, data=None):
        connection = HTTPSConnection(self.domain)
        try:
            base64_bytes = b64encode(
                ("%s:%s" % (self.username, self.password)).encode("ascii")
                ).decode("ascii")
            headers = {'Authorization' : 'Basic %s' %  base64_bytes, 'Content-Encoding' : 'gzip'}
            connection.request(method, path, headers=headers, body=data)
            response = connection.getresponse()
            return loads(response.read().decode())
        finally:
            connection.close()

    def get(self, path):
        return self.request(path, 'GET')

    def post(self, path, data):
        if isinstance(data, str):
            data_str = data
        else:
            data_str = dumps(data)
        return self.request(path, 'POST', data_str)

    def validate_combinations(self, api_type: str, merchant: str, task_type: str) -> None:
        
        # Validate merchant and api_type combinations
        if api_type == merchant:
            if merchant not in ['amazon', 'google']:
                raise ValueError("Invalid combination: api_type must be 'amazon' or 'google' when merchant is specified.")

        # Validate task_type based on merchant
        if merchant == 'amazon':
            valid_task_types = ['products', 'asin', 'reviews']
            if task_type not in valid_task_types:
                raise ValueError(f"Invalid task_type for merchant 'amazon'. Must be one of {valid_task_types}.")
        elif merchant == 'google':
            if task_type != 'products':
                raise ValueError("Invalid task_type for merchant 'google'. Must be 'products'.")


    # Defining post task
    def create_task(self,
                    api_type:str,
                    merchant:str='amazon',
                    post_data:dict=None,
                    task_type:str='products') -> str:
        
        self.validate_combinations(api_type=api_type, merchant=merchant, task_type=task_type)
        
        # checking if the input data is correct
        if isinstance(task_type, str):
            # send post request to endpoint
            response = self.post(f"/v3/{api_type}/{merchant}/{task_type}/task_post", post_data)
            # if the response is correct
            if response["status_code"] == 20000:
                
                # Saving response to json
                date=datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
                path=f'./Task-Post/{merchant}_task_post_{date}_of_{task_type}.json'
                save_dict_to_json(data_dict=response,filename=path)

                for task in response['tasks']:
                    print("Task accepted")
                    print("Task id: %s." % task["id"])
                    # save response in class variable
                    self._response = response
                    

                    # read json database file
                    self.load_database(path=self.database_path)
                    # create new dict task
                    
                    if task_type!='reviews':
                        task_name=" ".join(task['path'])+" "+ task['data']['keyword']
                        keyword=task['data']['keyword']
                    else:
                        task_name=" ".join(task['path'])+" "+ task['data']['asin']
                        keyword=task['data']['asin']
                    
                    new_task={
                        'task_id':task['id'],
                        'task_name': task_name,
                        'task_creation_date':datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        'api':task['data']['api'],
                        'function':task['data']['function'],
                        'location_name':task['data']['location_name'],
                        'language_name':task['data']['language_name'],
                        'depth':task['data']['depth'],
                        'keyword':keyword,
                        'se_type':task['data']['se_type'],
                        'se':task['data']['se'],
                        'task_status':'Sent/in_progress',
                        'task_available_date':None,
                        'task_download_date':None,
                        'task_file_name':None,
                        'task_endpoint_advance':None
                        
                    }
                    # add new task to the database
                    self.task_data['data'].append(new_task)
                    # save database
                    self.save_database(path=self.database_path)
                
            else:
                print("error. Code: %d Message: %s" % (response["status_code"], response["status_message"]))
        
        else:
            raise Exception("Incorrect input data")

    # Defining get task list
    def get_task_list(self,
                      api_type:str,
                      merchant:str,
                      task_type:str) -> list:
        
        self.validate_combinations(api_type=api_type, merchant=merchant, task_type=task_type)
        
        if isinstance(task_type, str):
            # get task list from endpoint
            response = self.get(f"/v3/{api_type}/{merchant}/{task_type}/tasks_ready")
            # check if the response is correct 
            if response["status_code"] == 20000:
                # save the response to json
                path=f'./Task-Get/Task-List/{merchant}_task_ready_list_{task_type}.json'
                save_dict_to_json(data_dict=response,filename=path)
                # return list of task ids
                task_ready_list_ids=response['tasks'][0]['result']
                # read json database file
                self.load_database(path=self.database_path)
                
                if task_ready_list_ids:
                    for task in task_ready_list_ids:
                        # update task status to ready
                        for task_database in self.task_data['data']:
                            if task_database['task_id']==task['id']:
                                task_database['task_status']='Ready'
                                task_database['task_available_date']=datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") if task_database['task_available_date'] == None else task_database['task_available_date'] 
                                task_database['task_endpoint_advance']=task['endpoint_advanced']
                                
                        # save database
                        self.save_database(path=self.database_path)
                else:
                    print('No new tasks\nCalling API again')
                    self.get_task_list(merchant=merchant,task_type=task_type)
                          
                
            else:
                print("error. Code: %d Message: %s" % (response["status_code"], response["status_message"]))
        else:
            raise Exception("Incorrect input data")
            
            
    # Defining get task
    def get_task(self,
                 api_type:str,
                 merchant:str,
                 task_type:str,
                 id:str) -> None:
        
        self.validate_combinations(api_type=api_type, merchant=merchant, task_type=task_type)
        
        response = self.get(f"/v3/{api_type}/{merchant}/{task_type}/task_get/advanced/" + id)
        if response["status_code"] == 20000:
            # saving data to json
            path=f"./Task-Get/Task-ID/{merchant}_task_get_by_id_{id}.json"
            save_dict_to_json(data_dict=response,filename=path)
            
            return response

        else:
            print("error. Code: %d Message: %s" % (response["status_code"], response["status_message"]))
            
    def get_task_batch(self,
                        
                        merchant:str,
                        task_type:str) -> None:
            
                   
            
            self.load_database(path=self.database_path)
            for task_database in self.task_data['data']:
                if task_database['task_status']=='Ready':
                    response = self.get(task_database['task_endpoint_advance'])
                    if response["status_code"] == 20000:
                        # saving data to json
                        path=f"./Task-Get/Task-ID/{merchant}_task_get_by_id_{task_database['task_id']}_{task_type}_{task_database['keyword']}.json"
                        save_dict_to_json(data_dict=response,filename=path)
                        task_database['task_status']='Downloaded'
                        task_database['task_download_date']=datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        task_database['task_file_name']=path
                        # save database
                        self.save_database(path=self.database_path)
                    else:
                        print("error. Code: %d Message: %s" % (response["status_code"], response["status_message"]))

    def task_monitor(self,
                     api_type:str,
                     merchant:str,
                     task_type:str,
                     time_sec:int) -> None:
        
        self.validate_combinations(api_type=api_type, merchant=merchant, task_type=task_type)
        
        self.load_database(path=self.database_path)
        data=self.task_data['data']     
        data_lenght=len([item for item in data if item['task_status']!='Downloaded'])
        print(f'You have {data_lenght} tasks in progress')
        print('Please wait...')
        
        while data_lenght>0:
            self.get_task_list(api_type=api_type,merchant=merchant,task_type=task_type)
            self.get_task_batch(merchant=merchant,task_type=task_type)
            self.load_database(path=self.database_path)
            data=self.task_data['data']     
            data_lenght=len([item for item in data if item['task_status']!='Downloaded'])
            if data_lenght>0:
                time.sleep(time_sec)
        else:
            print('All tasks are downloaded')
            