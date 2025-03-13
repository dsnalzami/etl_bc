from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import pandas as pd
import requests
import sqlite3
import os

# Define default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'etl_api_to_sqlite',
    default_args=default_args,
    description='ETL process from API to SQLite',
    schedule_interval=timedelta(days=1),
    catchup=False
)

# Functions for ETL Process
def extract_from_api(**kwargs):
    """Extract data from a public API"""
    # Using JSONPlaceholder API as an example
    url = "https://jsonplaceholder.typicode.com/posts"
    
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()  # Raise exception for HTTP errors
        
        data = response.json()
        df = pd.DataFrame(data)
        
        # Store the DataFrame in XCom
        kwargs['ti'].xcom_push(key='api_data', value=df.to_json(orient='records'))
        return f"Data successfully extracted from API: {url}, {len(df)} records retrieved"
    except requests.exceptions.RequestException as e:
        # Handle API request errors
        error_msg = f"API request failed: {str(e)}"
        print(error_msg)
        raise Exception(error_msg)

def transform_api_data(**kwargs):
    """Transform the API data"""
    # Pull the DataFrame from the previous task
    ti = kwargs['ti']
    data_json = ti.xcom_pull(task_ids='extract_api_task', key='api_data')
    df = pd.DataFrame(pd.read_json(data_json, orient='records'))
    
    # Perform transformations
    # 1. Create a 'title_length' column
    df['title_length'] = df['title'].str.len()
    
    # 2. Create a 'body_words' column counting words in the body
    df['body_words'] = df['body'].str.split().str.len()
    
    # 3. Filter to include only posts with title_length > 30
    original_count = len(df)
    df = df[df['title_length'] > 30]
    filtered_count = len(df)
    
    # Store the transformed DataFrame in XCom
    kwargs['ti'].xcom_push(key='transformed_api_data', value=df.to_json(orient='records'))
    return f"API data transformed successfully. Filtered from {original_count} to {filtered_count} records."

def load_api_data(**kwargs):
    """Load transformed API data to SQLite"""
    # Pull the transformed DataFrame
    ti = kwargs['ti']
    data_json = ti.xcom_pull(task_ids='transform_api_task', key='transformed_api_data')
    df = pd.read_json(data_json, orient='records')
    
    # Ensure data directory exists
    os.makedirs('/opt/airflow/data', exist_ok=True)
    
    # Connect to SQLite
    db_path = '/opt/airflow/data/etl_database.db'
    conn = sqlite3.connect(db_path)
    
    # Create table and load data
    df.to_sql('posts', conn, if_exists='replace', index=False)
    
    # Get record count for logging
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM posts")
    count = cursor.fetchone()[0]
    
    # Close connection
    conn.close()
    return f"API data loaded successfully to SQLite. {count} records inserted in {db_path}"

# Define the tasks
extract_api_task = PythonOperator(
    task_id='extract_api_task',
    python_callable=extract_from_api,
    provide_context=True,
    dag=dag,
)

transform_api_task = PythonOperator(
    task_id='transform_api_task',
    python_callable=transform_api_data,
    provide_context=True,
    dag=dag,
)

load_api_task = PythonOperator(
    task_id='load_api_task',
    python_callable=load_api_data,
    provide_context=True,
    dag=dag,
)

# Set the task dependencies
extract_api_task >> transform_api_task >> load_api_task