from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
import pandas as pd
import sqlite3
import requests
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
    'etl_multi_source',
    default_args=default_args,
    description='ETL process from multiple sources to SQLite',
    schedule_interval=timedelta(days=1),
    catchup=False
)

# Helper function to ensure directories exist
def ensure_dirs():
    """Ensure necessary directories exist"""
    os.makedirs('/opt/airflow/data', exist_ok=True)

# ETL Functions

# Data Source 1: CSV File
def extract_from_csv(**kwargs):
    """Extract data from CSV file"""
    # Ensure directories exist
    ensure_dirs()
    
    csv_path = '/opt/airflow/data/sample_data.csv'
    if not os.path.exists(csv_path):
        # Create a sample file if it doesn't exist
        sample_data = """id,nama,umur,kota
1,Budi,25,Jakarta
2,Ani,30,Bandung
3,Citra,28,Surabaya
4,Dedi,35,Yogyakarta
5,Eka,27,Medan"""
        
        with open(csv_path, 'w') as f:
            f.write(sample_data)
            
    df = pd.read_csv(csv_path)
    kwargs['ti'].xcom_push(key='csv_data', value=df.to_json(orient='records'))
    return f"CSV data extracted successfully from {csv_path}, {len(df)} records"

# Data Source 2: API
def extract_from_users_api(**kwargs):
    """Extract user data from API"""
    url = "https://jsonplaceholder.typicode.com/users"
    
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        
        data = response.json()
        df = pd.DataFrame(data)
        kwargs['ti'].xcom_push(key='user_api_data', value=df.to_json(orient='records'))
        return f"User API data extracted successfully from {url}, {len(df)} records"
    except requests.exceptions.RequestException as e:
        error_msg = f"User API request failed: {str(e)}"
        print(error_msg)
        raise Exception(error_msg)

# Data Source 3: Another API for TODO items
def extract_from_todos_api(**kwargs):
    """Extract TODO data from API"""
    url = "https://jsonplaceholder.typicode.com/todos"
    
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        
        data = response.json()
        df = pd.DataFrame(data)
        kwargs['ti'].xcom_push(key='todo_api_data', value=df.to_json(orient='records'))
        return f"TODO API data extracted successfully from {url}, {len(df)} records"
    except requests.exceptions.RequestException as e:
        error_msg = f"TODO API request failed: {str(e)}"
        print(error_msg)
        raise Exception(error_msg)

# Transform functions
def transform_csv_data(**kwargs):
    """Transform CSV data"""
    ti = kwargs['ti']
    data_json = ti.xcom_pull(task_ids='extract_csv_task', key='csv_data')
    df = pd.read_json(data_json, orient='records')
    
    # Transformations
    df['nama'] = df['nama'].str.upper()
    df['kota'] = df['kota'].str.upper()
    df['umur_kategori'] = df['umur'].apply(
        lambda x: 'SENIOR' if x > 30 else 'DEWASA' if x > 25 else 'MUDA'
    )
    
    kwargs['ti'].xcom_push(key='transformed_csv_data', value=df.to_json(orient='records'))
    return f"CSV data transformed successfully. Added 'umur_kategori' column with values: {df['umur_kategori'].value_counts().to_dict()}"

def transform_user_data(**kwargs):
    """Transform user API data"""
    ti = kwargs['ti']
    data_json = ti.xcom_pull(task_ids='extract_users_api_task', key='user_api_data')
    df = pd.read_json(data_json, orient='records')
    
    # Extract only needed columns and rename
    selected_columns = ['id', 'name', 'email', 'username']
    df = df[selected_columns]
    df.rename(columns={'name': 'nama_lengkap', 'username': 'nama_pengguna'}, inplace=True)
    
    # Add domain column extracted from email
    df['email_domain'] = df['email'].str.split('@').str[1]
    
    kwargs['ti'].xcom_push(key='transformed_user_data', value=df.to_json(orient='records'))
    return f"User data transformed successfully. Email domains found: {df['email_domain'].unique().tolist()}"

def transform_todo_data(**kwargs):
    """Transform TODO API data"""
    ti = kwargs['ti']
    data_json = ti.xcom_pull(task_ids='extract_todos_api_task', key='todo_api_data')
    df = pd.read_json(data_json, orient='records')
    
    # Filter only completed tasks
    original_count = len(df)
    df = df[df['completed'] == True]
    filtered_count = len(df)
    
    # Add character count column for title
    df['title_length'] = df['title'].str.len()
    
    kwargs['ti'].xcom_push(key='transformed_todo_data', value=df.to_json(orient='records'))
    return f"TODO data transformed successfully. Filtered from {original_count} to {filtered_count} completed tasks."

# Combine and Load function
def combine_and_load_data(**kwargs):
    """Combine transformed data and load to SQLite"""
    ti = kwargs['ti']
    
    # Ensure directories exist
    ensure_dirs()
    
    # Get all transformed data
    csv_data_json = ti.xcom_pull(task_ids='transform_csv_task', key='transformed_csv_data')
    user_data_json = ti.xcom_pull(task_ids='transform_user_task', key='transformed_user_data')
    todo_data_json = ti.xcom_pull(task_ids='transform_todo_task', key='transformed_todo_data')
    
    # Convert to DataFrames
    df_csv = pd.read_json(csv_data_json, orient='records')
    df_user = pd.read_json(user_data_json, orient='records')
    df_todo = pd.read_json(todo_data_json, orient='records')
    
    # Connect to SQLite
    db_path = '/opt/airflow/data/multi_source_db.db'
    conn = sqlite3.connect(db_path)
    
    # Save each dataset to its own table
    df_csv.to_sql('penduduk', conn, if_exists='replace', index=False)
    df_user.to_sql('pengguna', conn, if_exists='replace', index=False)
    df_todo.to_sql('tugas_selesai', conn, if_exists='replace', index=False)
    
    # Create a joined table for users and their todos
    if not df_user.empty and not df_todo.empty:
        query = """
        CREATE TABLE IF NOT EXISTS pengguna_dan_tugas AS
        SELECT p.nama_lengkap, p.email, t.title as judul_tugas, t.title_length as panjang_judul
        FROM pengguna p
        JOIN tugas_selesai t ON p.id = t.userId
        """
        conn.execute(query)
    
    # Get record counts for logging
    cursor = conn.cursor()
    result = {}
    for table in ['penduduk', 'pengguna', 'tugas_selesai', 'pengguna_dan_tugas']:
        try:
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            result[table] = cursor.fetchone()[0]
        except sqlite3.OperationalError:
            result[table] = 'Table not created'
    
    # Close connection
    conn.close()
    
    return f"All data combined and loaded to SQLite successfully. Table counts: {result}"

# Create tasks
start = DummyOperator(task_id='start', dag=dag)
end = DummyOperator(task_id='end', dag=dag)

# Extract tasks
extract_csv_task = PythonOperator(
    task_id='extract_csv_task',
    python_callable=extract_from_csv,
    provide_context=True,
    dag=dag,
)

extract_users_api_task = PythonOperator(
    task_id='extract_users_api_task',
    python_callable=extract_from_users_api,
    provide_context=True,
    dag=dag,
)

extract_todos_api_task = PythonOperator(
    task_id='extract_todos_api_task',
    python_callable=extract_from_todos_api,
    provide_context=True,
    dag=dag,
)

# Transform tasks
transform_csv_task = PythonOperator(
    task_id='transform_csv_task',
    python_callable=transform_csv_data,
    provide_context=True,
    dag=dag,
)

transform_user_task = PythonOperator(
    task_id='transform_user_task',
    python_callable=transform_user_data,
    provide_context=True,
    dag=dag,
)

transform_todo_task = PythonOperator(
    task_id='transform_todo_task',
    python_callable=transform_todo_data,
    provide_context=True,
    dag=dag,
)

# Load task
load_task = PythonOperator(
    task_id='load_task',
    python_callable=combine_and_load_data,
    provide_context=True,
    dag=dag,
)

# Set task dependencies
start >> [extract_csv_task, extract_users_api_task, extract_todos_api_task]
extract_csv_task >> transform_csv_task
extract_users_api_task >> transform_user_task
extract_todos_api_task >> transform_todo_task
[transform_csv_task, transform_user_task, transform_todo_task] >> load_task
load_task >> end