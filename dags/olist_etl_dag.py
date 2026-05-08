"""
E-Commerce ETL DAG with Airflow
================================

This DAG implements the ETL pipeline for Olist e-commerce data:
- Extract: Load raw CSV data
- Transform: Clean and process data
- Load: Insert into PostgreSQL data warehouse

Includes data quality checks with Great Expectations.
"""

import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

# Add project root so `src` package can be imported
sys.path.append('/opt/airflow')

from src.etl_pipeline import DataCleaner, DataExtractor, DataLoader, DataTransformer

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'olist_etl_pipeline',
    default_args=default_args,
    description='ETL pipeline for Olist e-commerce data',
    schedule_interval=timedelta(days=1),
    catchup=False,
)

def extract_task():
    """Extract raw data from CSV files"""
    extractor = DataExtractor()
    datasets = extractor.load_all_datasets()
    # Save to XCom for next tasks
    return datasets

def transform_task(**context):
    """Transform and clean data"""
    datasets = context['ti'].xcom_pull(task_ids='extract')

    cleaner = DataCleaner()
    transformer = DataTransformer()

    cleaned_datasets = {}
    for name, df in datasets.items():
        cleaned_datasets[name] = cleaner.clean_dataset(df, name)

    transformed_datasets = {}
    for name, df in cleaned_datasets.items():
        transformed_datasets[name] = transformer.transform_dataset(df, name)

    return transformed_datasets

def load_task(**context):
    """Load data into PostgreSQL data warehouse"""
    transformed_datasets = context['ti'].xcom_pull(task_ids='transform')

    loader = DataLoader()
    loader.load_all_data(transformed_datasets)

def data_quality_check(**context):
    """Run Great Expectations data quality checks"""
    from tests.data_quality_ge import validate_data

    transformed_datasets = context['ti'].xcom_pull(task_ids='transform')

    for name, df in transformed_datasets.items():
        results = validate_data(df)
        if not results.success:
            raise ValueError(f"Data quality check failed for {name}")

    print("All data quality checks passed!")

# Define tasks
extract = PythonOperator(
    task_id='extract',
    python_callable=extract_task,
    dag=dag,
)

transform = PythonOperator(
    task_id='transform',
    python_callable=transform_task,
    dag=dag,
)

load = PythonOperator(
    task_id='load',
    python_callable=load_task,
    dag=dag,
)

data_quality = PythonOperator(
    task_id='data_quality_check',
    python_callable=data_quality_check,
    dag=dag,
)

# Set dependencies
extract >> transform >> data_quality >> load

