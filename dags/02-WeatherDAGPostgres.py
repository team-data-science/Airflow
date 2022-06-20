#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.


# [START tutorial]
# [START import_module]
import json
import string

import pendulum

from airflow.decorators import dag, task

# my imports
from numpy import float64, int32, string_
from pandas.core.reshape.pivot import pivot
import requests
import pandas as pd
from pandas import DataFrame, json_normalize
import datetime as dt

from airflow.providers.postgres.operators.postgres import PostgresOperator

# [END import_module]


# [START instantiate_dag]
@dag(
    schedule_interval=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=['LearnDataEngineering'],
)
def lde_weather_api_posgres():
    """
    ### TaskFlow API Tutorial Documentation
    This is a simple ETL data pipeline example which demonstrates the use of
    the TaskFlow API using three simple tasks for Extract, Transform, and Load.
    Documentation that goes along with the Airflow TaskFlow API tutorial is
    located
    [here](https://airflow.apache.org/docs/apache-airflow/stable/tutorial_taskflow_api.html)
    """
    # [END instantiate_dag]

    # [START extract]
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

    # [END extract]

    # [START transform]
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

    # [END transform]

    # [START load]
    @task()
    def load(ex_dict: dict):
        """
        #### Load task
        A simple Load task which takes in the result of the Transform task and
        instead of saving it to end user review, just prints it out.
        """
        print(ex_dict)

    # [END load]


    # [START main_flow]
    weather_data = extract()
    weather_summary = transform(weather_data)

    #weather_summary[0]["location"]
    insert_data = PostgresOperator(
        task_id="insert_request_data",
        postgres_conn_id="postgres_default",
        parameters={'location': weather_summary[0]["location"]},
        sql="""INSERT INTO temperature (location, temp_c, wind_kph, time) VALUES ( %(location)s , 20.5, 2.1, '2022-06-15 00:01:00');""",
    )
    #insert_data.set_upstream(weather_summary)
    weather_summary >> insert_data
    
    # [END main_flow]


# [START dag_invocation]
lde_weather_dag_posgres = lde_weather_api_posgres()
# [END dag_invocation]

# [END tutorial]