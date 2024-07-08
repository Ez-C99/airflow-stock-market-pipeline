from airflow.decorators import dag, task
from airflow.sensors.base import PokeReturnValue
from datetime import datetime

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
        pass

stock_market()
