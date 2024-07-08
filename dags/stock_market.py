from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from datetime import datetime
import requests
import logging

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
    def is_api_available() -> bool:
        api = BaseHook.get_connection('stock_api')
        url = f"{api.host}/query?function=TIME_SERIES_DAILY&symbol={api.extra_dejson['symbol']}&apikey={api.extra_dejson['api_key']}"
        response = requests.get(url)
        data = response.json()

        # Log the response data for debugging purposes
        # logging.info(f"API response data: {data}")

        # Check if the response contains the expected data
        condition = "Time Series (Daily)" in data and bool(data["Time Series (Daily)"])
        return condition

    is_api_available()

stock_market()
