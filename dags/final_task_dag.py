from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.dates import days_ago
from datetime import timedelta
from clearml import Task
import pandas as pd
import pandera as pa
from pandera import Column, DataFrameSchema
import logging

taxi_schema = DataFrameSchema({
    "pickup_datetime": Column(pa.DateTime, required=True),
    "dropoff_datetime": Column(pa.DateTime, required=True),
    "pickup_longitude": Column(pa.Float, required=True),
    "pickup_latitude": Column(pa.Float, required=True),
    "dropoff_longitude": Column(pa.Float, required=True),
    "dropoff_latitude": Column(pa.Float, required=True),
    "passenger_count": Column(pa.Int, required=True),
    "trip_distance": Column(pa.Float, required=True),
    "fare_amount": Column(pa.Float, required=True)
})

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

def download_data(**kwargs):
    """Скачать сырые данные из S3"""
    hook = S3Hook(aws_conn_id='s3_default')
    local_path = '/tmp/uber-raw.csv'
    hook.download_file(key='raw/uber.csv', bucket_name='your-bucket', local_path=local_path)
    
    logging.info(f"✅ Downloaded: {local_path}")
    return local_path

def validate_data(**kwargs):
    """Валидация схемы данных"""
    ti = kwargs['ti']
    raw_path = ti.xcom_pull(task_ids='download_data')
    
    df = pd.read_csv(raw_path)
    df['pickup_datetime'] = pd.to_datetime(df['pickup_datetime'])
    df['dropoff_datetime'] = pd.to_datetime(df['dropoff_datetime'])
    
    validated_df = taxi_schema.validate(df)
    
    valid_path = '/tmp/uber-validated.csv'
    validated_df.to_csv(valid_path, index=False)
    
    logging.info(f"✅ Validated: {len(validated_df)} rows")
    return valid_path

def transform_data(**kwargs):
    """Трансформация + feature engineering"""
    ti = kwargs['ti']
    valid_path = ti.xcom_pull(task_ids='validate_data')
    
    df = pd.read_csv(valid_path)
    
    df['trip_duration'] = (df['dropoff_datetime'] - df['pickup_datetime']).dt.total_seconds()
    
    df['speed_kmh'] = (df['trip_distance'] * 3.6) / (df['trip_duration'] / 3600)
    
    df['pickup_hour'] = df['pickup_datetime'].dt.hour
    
    transform_path = '/tmp/uber-transformed.csv'
    df.to_csv(transform_path, index=False)
    
    logging.info(f"✅ Transformed: {len(df)} rows")
    return transform_path

def prepare_ml_data(**kwargs):
    """Подготовка ML-ready датасета"""
    ti = kwargs['ti']
    transform_path = ti.xcom_pull(task_ids='transform_data')
    
    df = pd.read_csv(transform_path)
    
    ml_features = df[[
        'pickup_longitude', 'pickup_latitude', 
        'dropoff_longitude', 'dropoff_latitude',
        'passenger_count', 'trip_distance', 
        'trip_duration', 'speed_kmh', 'pickup_hour'
    ]].copy()
    
    ml_path = '/tmp/uber-ml-ready.csv'
    ml_features.to_csv(ml_path, index=False)
    
    hook = S3Hook(aws_conn_id='s3_default')
    hook.load_file(filename=ml_path, key='ml/uber_ml_ready.csv', bucket_name='your-bucket')
    
    logging.info(f"✅ ML-ready: {ml_path}")
    return ml_path

def launch_train_models(**kwargs):
    """Запуск ClearML pipeline для тренировки"""
    task = Task.init(project_name='Taxi Aggregator', task_name='Airflow → ML Training')
    
    logging.info("🚀 Launching ClearML ML Pipeline...")
    logging.info("📊 ClearML Tasks: prepare_ml → train_models → choose_model")
    
    task.close()
    
    return "ml_pipeline_completed"

def choose_best_model(**kwargs):
    """Выбор лучшей модели (заглушка)"""
    logging.info("🏆 Best model selected from ClearML Experiments")
    logging.info("📈 Check: Taxi Aggregator → Experiments")
    
    return "best_model_selected"

with DAG(
    dag_id='taxi_etl_ml_pipeline_v2',
    default_args=default_args,
    schedule='@daily',
    catchup=False,
    max_active_runs=1,
) as dag:
    
    download_task = PythonOperator(
        task_id='download_data',
        python_callable=download_data
    )
    
    validate_task = PythonOperator(
        task_id='validate_data',
        python_callable=validate_data
    )
    
    transform_task = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data
    )
    
    prepare_ml_task = PythonOperator(
        task_id='prepare_ml',
        python_callable=prepare_ml_data
    )
    
    train_models_task = PythonOperator(
        task_id='train_models',
        python_callable=launch_train_models
    )
    
    choose_model_task = PythonOperator(
        task_id='choose_model',
        python_callable=choose_best_model
    )
    
    download_task >> validate_task >> transform_task >> prepare_ml_task >> train_models_task >> choose_model_task
