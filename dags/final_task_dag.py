from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.dates import days_ago
from datetime import timedelta
from clearml import Task, PipelineController
import pandas as pd
import pandera as pa
from pandera import Column, DataFrameSchema
import logging
import os

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

def download_and_validate(**kwargs):
    hook = S3Hook(aws_conn_id='s3_default')
    local_path = '/tmp/uber-raw.csv'
    hook.download_file(key='raw/uber.csv', bucket_name='your-bucket', local_path=local_path)
    
    df = pd.read_csv(local_path)
    df['pickup_datetime'] = pd.to_datetime(df['pickup_datetime'])
    
    try:
        df = taxi_schema.validate(df)
    except pa.errors.SchemaError as e:
        logging.error(f"Validation failed: {e}")
        raise
    
    df['trip_duration'] = (df['dropoff_datetime'] - df['pickup_datetime']).dt.total_seconds()
    
    processed_path = '/tmp/processed_uber.csv'
    df.to_csv(processed_path, index=False)
    hook.load_file(filename=processed_path, key='processed/processed_uber.csv', bucket_name='your-bucket')
    
    return processed_path

def launch_clearml_pipeline(**kwargs):
    Task.init(project_name='Taxi Aggregator', task_name='Airflow → ClearML Launcher')
    
    pipe = PipelineController(
        name='Taxi ML Daily Pipeline',
        project='Taxi Aggregator',
        default_execution_queue='default-ml'
    )
    
    pipe.add_function_step('data_prep', prepare_data_for_ml, parents=[], execution_queue='ml-cpu')
    pipe.add_step('train_models', train_models, parents=['data_prep'], 
                     execution_queue='ml-gpu', hyper_parameters=hyper_params)
    pipe.add_step('evaluate', evaluate, parents=['train_models'])
    
    logging.info(f"Pipeline launched: {pipeline_id}")
    task.set_reported_pipeline(pipeline_id)
    
    pipe.start() 
    logging.info(f"Pipeline started: {pipe.id}")
    return pipe.id

with DAG(
    dag_id='taxi_etl_ml_pipeline',
    default_args=default_args,
    schedule='@daily',
    catchup=False,
    max_active_runs=1,
) as dag:
    
    etl = PythonOperator(task_id='etl_s3', python_callable=download_and_validate)
    launch = PythonOperator(task_id='launch_ml_pipeline', python_callable=launch_clearml_pipeline)
    
    etl >> launch