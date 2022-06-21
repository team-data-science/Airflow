

import json
import pandas as pd
from pandas import DataFrame, json_normalize
import datetime as dt

def transform_weatherAPI(im_json : str):
    
    print(im_json)
    api_json = json.loads(im_json)
    normalized = json_normalize(api_json)

    # timestamp format with +2.00 is very important otherwise it will not get shown on the board API returns local board
    normalized['TimeStamp'] = normalized['location.localtime_epoch'].apply(lambda s : dt.datetime.fromtimestamp(s).strftime('%Y-%m-%dT%H:%M:%S+02:00'))
    normalized.rename(columns={'location.name': 'location', 
    'location.region': 'region',
    'current.temp_c': 'temp_c',
    'current.wind_kph': 'wind_kph'
    }, inplace=True)     

    ex_df = normalized.filter(['location','temp_c','wind_kph'])      

    ex_json = DataFrame.to_json(ex_df)

    return ex_json