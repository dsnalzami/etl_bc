from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import pandas as pd
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
    'etl_csv_to_sqlite',
    default_args=default_args,
    description='ETL process from CSV to SQLite',
    schedule_interval=timedelta(days=1),
    catchup=False
)

# Functions for ETL Process
def extract_data(**kwargs):
    """Extract data from CSV file"""
    # Ensure the data directory exists
    os.makedirs('/opt/airflow/data', exist_ok=True)
    
    # Check if CSV exists, if not create sample data
    csv_path = '/opt/airflow/data/sample_data.csv'
    if not os.path.exists(csv_path):
        # Create a sample file with data
        sample_data = """id,nama,umur,kota
1,Budi,25,Jakarta
2,Ani,30,Bandung
3,Citra,28,Surabaya
4,Dedi,35,Yogyakarta
5,Eka,27,Medan"""
        
        with open(csv_path, 'w') as f:
            f.write(sample_data)
    
    # Now read the CSV
    df = pd.read_csv(csv_path)
    
    # Store the DataFrame in XCom for the next task
    kwargs['ti'].xcom_push(key='extracted_data', value=df.to_json(orient='records'))
    return f"Data extracted successfully from {csv_path}"

def transform_data(**kwargs):
    """Transform the extracted data"""
    # Pull the DataFrame from the previous task
    ti = kwargs['ti']
    data_json = ti.xcom_pull(task_ids='extract_task', key='extracted_data')
    df = pd.read_json(data_json, orient='records')
    
    # Perform transformations
    # 1. Convert 'umur' to integer
    df['umur'] = df['umur'].astype(int)
    
    # 2. Convert 'nama' and 'kota' to uppercase
    df['nama'] = df['nama'].str.upper()
    df['kota'] = df['kota'].str.upper()
    
    # 3. Add a new column 'status' based on age
    df['status'] = df['umur'].apply(lambda x: 'DEWASA' if x >= 30 else 'MUDA')
    
    # Store the transformed DataFrame in XCom
    kwargs['ti'].xcom_push(key='transformed_data', value=df.to_json(orient='records'))
    return f"Data transformed successfully. Added 'status' column: {df['status'].value_counts().to_dict()}"

def load_data(**kwargs):
    """Load data to SQLite database"""
    # Pull the transformed DataFrame
    ti = kwargs['ti']
    data_json = ti.xcom_pull(task_ids='transform_task', key='transformed_data')
    df = pd.read_json(data_json, orient='records')
    
    # Ensure data directory exists
    os.makedirs('/opt/airflow/data', exist_ok=True)
    
    # Connect to SQLite
    db_path = '/opt/airflow/data/etl_database.db'
    conn = sqlite3.connect(db_path)
    
    # Create table and load data
    df.to_sql('pengguna', conn, if_exists='replace', index=False)
    
    # Get record count for logging
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM pengguna")
    count = cursor.fetchone()[0]
    
    # Close connection
    conn.close()
    return f"Data loaded successfully to SQLite. {count} records inserted in {db_path}"

# Define the tasks
extract_task = PythonOperator(
    task_id='extract_task',
    python_callable=extract_data,
    provide_context=True,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_task',
    python_callable=transform_data,
    provide_context=True,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_task',
    python_callable=load_data,
    provide_context=True,
    dag=dag,
)

# Set the task dependencies
extract_task >> transform_task >> load_task