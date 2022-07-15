

import json
import pandas as pd
from pandas import DataFrame, json_normalize
import datetime as dt

def transform_weatherAPI(im_json : str):
    
    print(im_json)

    #load the string into a json object
    api_json = json.loads(im_json)

    # normalize the contents (flatten)
    normalized = json_normalize(api_json)

    # Create a timestamp in a nice looking format
    normalized['timestamp'] = normalized['location.localtime_epoch'].apply(lambda s : dt.datetime.fromtimestamp(s).strftime('%Y-%m-%dT%H:%M:%S+02:00'))

    # rename the columns to have simpler names
    normalized.rename(columns={'location.name': 'location', 
    'location.region': 'region',
    'current.temp_c': 'temp_c',
    'current.wind_kph': 'wind_kph'
    }, inplace=True)     

    # filter out only the columns that we need 
    ex_df = normalized.filter(['location','temp_c','wind_kph','timestamp'])      

    # return a json
    ex_json = DataFrame.to_json(ex_df, orient='records')

    return ex_json
