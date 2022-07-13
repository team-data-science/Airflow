

# imports important for Airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

# Import Modules for code
import json
import requests
import datetime as dt
import logging

# import custom transformer for API data
from transformer import transform_weatherAPI

def my_extract(**kwargs):
    # TODO: Fetch the data from an API and store it where it can be
    # read later
    payload = {'Key': '5a91e86eedc148059a390511211510', 'q': 'Berlin', 'aqi': 'no'}
    r = requests.get("http://api.weatherapi.com/v1/current.json", params=payload)

    # Get the json
    r_string = r.json()
    
    #dump the json result into a string
    ex_string = json.dumps(r_string)  
    
    # push it into xcom variable api_result
    task_instance = kwargs['ti']
    task_instance.xcom_push(key='api_result', value= ex_string)
    
    # optional return value (also goes into xcom, if you only have one value it's enough)
    return ex_string


def my_transform(**kwargs):
    
    task_instance = kwargs['ti']
    api_data = task_instance.xcom_pull(key='api_result', task_ids='extract')
    
    ex_json = transform_weatherAPI(api_data)
    
    task_instance.xcom_push(key='transformed_weather', value=ex_json)
    

def my_load(**kwargs):
    # TODO: Read the transformed data and save it where it can be analyzed later
    
    task_instance = kwargs['ti']
    weather_json = task_instance.xcom_pull(key='transformed_weather', task_ids='transform')
    
    logger = logging.getLogger("airflow.task")
    logger.info(weather_json)


with DAG('ETLWeatherPrintAirflow2', description='Airflow2.0 DAG', start_date=dt.datetime(2018, 11, 1),schedule_interval = "0 * * * *", catchup=False,tags=['LearnDataEngineering']) as dag:
    ext = PythonOperator(
        task_id='extract',
        python_callable=my_extract,
        provide_context=True,
    )


    trn = PythonOperator(
        task_id='transform',
        python_callable=my_transform,
        provide_context=True,
    )

    lds = PythonOperator(
        task_id='load',
        python_callable=my_load,
        provide_context=True,
    )

    ext >> trn >> lds



