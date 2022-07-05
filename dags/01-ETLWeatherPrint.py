

# imports important for Airflow
import pendulum
from airflow.decorators import dag, task

# Import Modules for code
import json
import requests
import pandas as pd
from pandas import DataFrame, json_normalize
import datetime as dt
import psycopg2 

# import custom transformer for API data
from transformer import transform_weatherAPI

# [START instantiate_dag]
@dag(
    schedule_interval=None,                             # interval how often the dag will run (can be cron expression as string)
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"), # from what point on the dag will run (will only be scheduled after this date)
    catchup=False,                                      # no catchup needed, because we are running an api that returns now values
    tags=['LearnDataEngineering'],                      # tag the DAQ so it's easy to find in AirflowUI
)
def ETLWeatherPrint():
    """
    ### TaskFlow API Tutorial Documentation
    This is a simple ETL data pipeline example which demonstrates the use of
    the TaskFlow API using three simple tasks for Extract, Transform, and Load.
    Documentation that goes along with the Airflow TaskFlow API tutorial is
    located
    [here](https://airflow.apache.org/docs/apache-airflow/stable/tutorial_taskflow_api.html)
    """

    # EXTRACT: Query the data from the Weather API
    @task()
    def extract():
        """
        #### Extract task
        A simple Extract task to get data ready for the rest of the data
        pipeline. In this case, getting data is simulated by reading from a
        hardcoded JSON string.
        """
        payload = {'Key': '5a91e86eedc148059a390511211510', 'q': 'Berlin', 'aqi': 'no'}
        r = requests.get("http://api.weatherapi.com/v1/current.json", params=payload)

        # Get the json
        r_string = r.json()

        #weather_data_dict = json.loads(r_string)
        #print(weather_data_dict)
        return r_string


    # TRANSFORM: Transform the API response into something that is useful for the load
    @task()
    def transform(weather_json: json):
        """
        A simple Transform task which takes in the API data and only extracts the location, wind,
        the temperature and time.
        """
        weather_str = json.dumps(weather_json)
        transformed_str = transform_weatherAPI(weather_str)

        # turn string into dictionary
        ex_dict = json.loads(transformed_str)
        
        #return ex_dict
        return ex_dict     

    # LOAD: Just print the data received from the API
    @task()
    def load(ex_dict: dict):

        print(ex_dict)



    # Define the main flow
    weather_data = extract()
    weather_summary = transform(weather_data)
    load(weather_summary)



# Invocate the DAG
lde_weather_dag = ETLWeatherPrint()
