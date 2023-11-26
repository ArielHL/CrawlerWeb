# Amazon and Google DataForSEO API Module

This Module is used to pull data from the DataForSEO API. It is used to pull data for Amazon and Google.

## Table of Contents

- [Getting Started](#getting-started)
  - [Prerequisites](#prerequisites)
  - [Installation](#installation)
- [Usage](#usage)
  - [Authentication](#authentication)
  - [Example Requests](#example-requests)
- [API Reference](#api-reference)
- [Troubleshooting](#troubleshooting)
- [Contributing](#contributing)
- [License](#license)

## Getting Started

This Module is used to pull data from the DataForSEO API. It is used to pull data for Amazon and Google.
It's built on top of the [DataForSEO API](https://docs.dataforseo.com/v3/) service.

### Prerequisites

This Module requires a DataForSEO API key. You can get one [here](https://app.dataforseo.com/api-dashboard).
Additionally you will need to install the requests library. You can do this by running the following command in your terminal:

```bash
pip install requests
```


### Installation

This Module can be installed by running the following command in your terminal:

```bash
pip install dataxseos
```

## Usage

**This Module has two main steps.**
- Create a task to the API. This will return a task ID that can be used to check the status of the task.
- Get the results of the post request based on the task ID.
this will return a json file that will be stored in the data folder of the project.

### Authentication

Authentication is done by passing your API key into the headers of the post request.
in the Dashboard, you can find your API key by clicking on the "API Dashboard" tab on the left side of the screen.

### Example Initialize the Client

First you need to inizialize the client object. This will be used to send the requests to the API.

```python

# Import Necesary Libraries
import pandas as pd
import configparser
from datetime import datetime

# import internal libraries
from Modules.client import RestClient
from Modules.handlers import *

# Setting the Configuration File
# if you have stored your API key in a config file you can use the following code
def read_config(filename=r'C:\Users\alimes001\Desktop\config.ini'):
    config = configparser.ConfigParser()
    config.read(filename)
    return config['Credentials']

credentials = read_config()
username = credentials['username']
password = credentials['password']

# Define the client object
client = RestClient(username=username, 
                    password=password,
                    database_path=r'./database/task_dict.json')

# Note that the database_path is the path to the json file that will store the task IDs and its status

```

### Example Creating a Task

There are two ways of creating a task:
- Sending one task in one request
- Sending multiple tasks in one request

**In the case of one Task, you can use the following code:**

```python

create_post(client=client,
            api_type='merchant',                        # this can be merchant or other API types supported by DataForSEO
            merchant='amazon',                          # this can be amazon or google
            task_type="products",                       # this can be products, ASIN or reviews
            value_to_search='Iphone 11',                # in Single task this need to be an string
            depth=700,                                  # this is the depth of the search (quantity of results)
            multiple_task=False)                        # this is a boolean that indicates if you want to send multiple tasks in one request 
                                                        # in sigle task this need to be False
```

**In the case of Multiple Task, you can use the following code:**

```python

create_post(client=client,
            api_type='merchant',                        # this can be merchant or other API types supported by DataForSEO
            merchant='amazon',                          # this can be amazon or google
            task_type="products",                       # this can be products, ASIN or reviews
            value_to_search=['Iphone 11','samsung 22'], # in Multiple task this need to be an List of Strings
            depth=700,                                  # this is the depth of the search (quantity of results)
            multiple_task=True)                         # this is a boolean that indicates if you want to send multiple tasks in one request
                                                        # in multiple task this need to be True