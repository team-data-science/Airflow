
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




# [START instantiate_dag]
@dag(
    schedule_interval=None,                             #interval how often the dag will run (can be cron expression as string)
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"), # from what point on the dag will run (will only be scheduled after this date)
    catchup=False,                                      # no catchup needed, because we are running an api that returns now values                
    tags=['LearnDataEngineering'],                      # tag the DAQ so it's easy to find in AirflowUI
)
def ETLWeatherPostgres():
    """
    ### TaskFlow API Tutorial Documentation
    This is a simple ETL data pipeline example which demonstrates the use of
    the TaskFlow API using three simple tasks for Extract, Transform, and Load.
    Documentation that goes along with the Airflow TaskFlow API tutorial is
    located
    [here](https://airflow.apache.org/docs/apache-airflow/stable/tutorial_taskflow_api.html)
    """
    # [END instantiate_dag]

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
        print(r_string)
        return r_string


    # TRANSFORM: Transform the API response into something that is useful for the load
    @task()
    def transform(weather_json: json):
        """
        #### Transform task
        A simple Transform task which takes in the collection of order data and
        computes the total order value.
        """
        #weather_json = json.loads(weather_data_dict)

        normalized = json_normalize(weather_json)

        # timestamp format with +2.00 is very important otherwise it will not get shown on the board API returns local board
        normalized['timestamp'] = normalized['location.localtime_epoch'].apply(lambda s : dt.datetime.fromtimestamp(s).strftime('%Y-%m-%dT%H:%M:%S+02:00'))
        normalized.rename(columns={'location.name': 'location', 
        'location.region': 'region',
        'current.temp_c': 'temp_c',
        'current.wind_kph': 'wind_kph'
        }, inplace=True)     


        ex_df = normalized.filter(['location','temp_c','wind_kph','timestamp'])      
        ex_dict = ex_df.to_dict('records')
        print(ex_dict[0]["location"])
        return ex_dict


    # LOAD: Save the data into Postgres database
    @task()
    def load(weather_data: dict):
        """
        #### Load task
        A simple Load task which takes in the result of the Transform task and
        instead of saving it to end user review, just prints it out.
        """

        try:
            connection = psycopg2.connect(user="airflow",
                                        password="airflow",
                                        host="postgres",
                                        port="5432",
                                        database="WeatherData")
            cursor = connection.cursor()

            postgres_insert_query = """INSERT INTO temperature (location, temp_c, wind_kph, time) VALUES ( %s , %s, %s, %s);"""
            record_to_insert = (weather_data[0]["location"], weather_data[0]["temp_c"], weather_data[0]["wind_kph"], weather_data[0]["timestamp"])
            cursor.execute(postgres_insert_query, record_to_insert)

            connection.commit()
            count = cursor.rowcount
            print(count, "Record inserted successfully into table")

        except (Exception, psycopg2.Error) as error:
            
            print("Failed to insert record into mobile table", error)
            
            if connection:
                cursor.close()
                connection.close()
                print("PostgreSQL connection is closed")
            
            raise Exception(error)

        finally:
            # closing database connection.
            if connection:
                cursor.close()
                connection.close()
                print("PostgreSQL connection is closed")

    
    # Define the main flow
    weather_data = extract()
    weather_summary = transform(weather_data)
    load(weather_summary)


# Invocate the DAG
lde_weather_dag_posgres = ETLWeatherPostgres()

