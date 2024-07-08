from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from airflow.sensors.base import PokeReturnValue
from datetime import datetime
import requests

@dag(
    description='The stock market DAG',
    start_date=datetime(2024, 1, 1),
    schedule='@daily',
    catchup=False,              # Not running all past non-triggered past DAG runs between start date and now
    tags=['stock_market'],
)
def stock_market():
    
    @task.sensor(
        poke_interval=30,       # Sensor checks for true condition every 30 seconds
        timeout=300,            # 5 minute timeout
        mode='poke'
    )
    def is_api_available() -> PokeReturnValue:
        api = BaseHook.get_connection('stock_api')
        url = f"{api.host}/query?function=TIME_SERIES_DAILY&symbol={api.extra_dejson['symbol']}&apikey={api.extra_dejson['api_key']}"
        response = requests.get(url)
        data = response.json()

        # Check if the response contains the expected data
        condition = "Time Series (Daily)" in data and data["Time Series (Daily)"]
        return PokeReturnValue(is_done=condition, xcom_value=data)
    
    # Assuming you want to save the data fetched to Minio or process it further
    @task()
    def process_data(api_data):
        # Logic to process data or save to Minio
        pass

    api_data = is_api_available()
    process_data(api_data)

stock_market()
