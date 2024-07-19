from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
import requests
import json
import pandas as pd
import os

# Укажите ваш API-ключ OpenWeatherMap
API_KEY = 'YOUR_API_KEY'
CITY = 'London'
API_URL = f'http://api.openweathermap.org/data/2.5/weather?q={CITY}&appid={API_KEY}'

# Функции для задач DAG
def download_weather_data(**kwargs):
    response = requests.get(API_URL)
    data = response.json()
    with open('/tmp/weather_data.json', 'w') as f:
        json.dump(data, f)

def process_weather_data(**kwargs):
    with open('/tmp/weather_data.json', 'r') as f:
        data = json.load(f)
    
    main = data.get('main', {})
    temp_k = main.get('temp')
    temp_c = temp_k - 273.15  # Преобразование из Кельвинов в Цельсии
    
    processed_data = {
        'city': data.get('name'),
        'temperature_celsius': temp_c,
        'pressure': main.get('pressure'),
        'humidity': main.get('humidity')
    }
    
    df = pd.DataFrame([processed_data])
    df.to_csv('/tmp/processed_weather_data.csv', index=False)

def save_to_parquet(**kwargs):
    df = pd.read_csv('/tmp/processed_weather_data.csv')
    df.to_parquet('/tmp/weather.parquet', index=False)

# Создание DAG
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
}

dag = DAG(
    'weather_data_pipeline_dag',
    default_args=default_args,
    description='A simple weather data pipeline',
    schedule_interval='@daily',
)

download_task = PythonOperator(
    task_id='download_weather_data',
    python_callable=download_weather_data,
    dag=dag,
)

process_task = PythonOperator(
    task_id='process_weather_data',
    python_callable=process_weather_data,
    dag=dag,
)

save_task = PythonOperator(
    task_id='save_to_parquet',
    python_callable=save_to_parquet,
    dag=dag,
)

download_task >> process_task >> save_task
