[![ETL Apache Airflow](https://upload.wikimedia.org/wikipedia/commons/thumb/d/de/AirflowLogo.png/1200px-AirflowLogo.png)](#)

# tutorial ETL Airflow
- Mata Kuliah : Business Intelligence - Materi Data Warehouse
- Pengampu: Dr. Eng. Farrikh Alzami, M.Kom
- Universitas Dian Nuswantoro, Semarang, 2025 

## siapkan direktori
Buat folder untuk project Airflow Anda:
```bash
mkdir airflow-etl
cd airflow-etl
```
## instalasi docker-compose.yaml
buat file `docker-compose.yaml` dengan konfigurasi berikut: 

```yaml
version: '3'

services:
  postgres:
    image: postgres:13
    environment:
      POSTGRES_USER: airflow
      POSTGRES_PASSWORD: airflow
      POSTGRES_DB: airflow
    volumes:
      - postgres-db-volume:/var/lib/postgresql/data
    ports:
      - "5432:5432"

  airflow-webserver:
    image: apache/airflow:2.8.1
    depends_on:
      - postgres
    environment:
      AIRFLOW__CORE__EXECUTOR: LocalExecutor
      AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:airflow@postgres/airflow
      AIRFLOW__CORE__LOAD_EXAMPLES: 'false'
      AIRFLOW__CORE__FERNET_KEY: ''
      AIRFLOW__WEBSERVER__SECRET_KEY: 'airflow-secret'
    volumes:
      - ./dags:/opt/airflow/dags
      - ./logs:/opt/airflow/logs
      - ./plugins:/opt/airflow/plugins
      - ./data:/opt/airflow/data
    user: "50000:0"
    ports:
      - 8080:8080
    command: bash -c "airflow db init && airflow users create --username admin --password admin --firstname Admin --lastname User --role Admin --email admin@example.com && airflow webserver"
    healthcheck:
      test: ["CMD", "curl", "--fail", "http://localhost:8080/health"]
      interval: 30s
      timeout: 30s
      retries: 5

  airflow-scheduler:
    image: apache/airflow:2.8.1
    depends_on:
      - airflow-webserver
    environment:
      AIRFLOW__CORE__EXECUTOR: LocalExecutor
      AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:airflow@postgres/airflow
      AIRFLOW__CORE__LOAD_EXAMPLES: 'false'
      AIRFLOW__CORE__FERNET_KEY: ''
      AIRFLOW__WEBSERVER__SECRET_KEY: 'airflow-secret'
    volumes:
      - ./dags:/opt/airflow/dags
      - ./logs:/opt/airflow/logs
      - ./plugins:/opt/airflow/plugins
      - ./data:/opt/airflow/data
    user: "50000:0"
    command: scheduler
    healthcheck:
      test: ["CMD-SHELL", 'airflow jobs check --job-type SchedulerJob --hostname "$${HOSTNAME}"']
      interval: 30s
      timeout: 30s
      retries: 5

volumes:
  postgres-db-volume:
```

## buat struktur direktori
```bash
mkdir -p dags plugins logs data
```
## jalankan airflow
jalankan airflow dengan docker compose
```bash
docker-compose up -d
```

Setelah semua container berjalan, akses Airflow Web UI di `http://localhost:8080` dengan:

- Username: airflow
- Password: airflow

# Tutorial ETL dengan Airflow dan SQLite
Sekarang mari kita buat contoh ETL sederhana menggunakan Airflow dan SQLite.

## Contoh 1: ETL Sederhana - CSV ke SQLite
Buat file data dummy terlebih dahulu. Simpan file CSV berikut di folder `data`:
1. Buat file `data/sample_data.csv` dengan konten:
```csv
id,nama,umur,kota
1,Budi,25,Jakarta
2,Ani,30,Bandung
3,Citra,28,Surabaya
4,Dedi,35,Yogyakarta
5,Eka,27,Medan
```
2. Buat DAG pertama Anda di folder `dags`. Buat file `dags/etl_csv_to_sqlite.py`:

```python
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
```

## Contoh 2: ETL dari API ke SQLite
Mari kita buat contoh kedua yang mengambil data dari API publik dan menyimpannya ke SQLite `etl_api_to_sqlite.py`.
```python
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
```

## Contoh 3: ETL dengan Multiple Data Sources
Mari buat contoh yang lebih kompleks dengan beberapa sumber data:
```python
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
```

## Cara Menjalankan ETL Pipeline
Setelah Anda mengupload file-file DAG di atas ke folder `dags` Anda, pipeline akan muncul di Airflow Web UI.

1. Buka browser dan akses `http://localhost:8080`
2. Login dengan username airflow dan password airflow
3. Di halaman beranda, Anda akan melihat daftar DAG Anda
4. Untuk menjalankan DAG secara manual:
    - Klik pada nama DAG
    - Klik tombol "Trigger DAG" (ikon play)
5. Anda bisa melihat progres dan status setiap task

## Melihat Hasil ETL di SQLite
Setelah DAG berjalan, Anda dapat melihat hasil ETL di database SQLite:

1. Masuk ke container Airflow:
```bash
docker-compose exec airflow-webserver bash
```

2. Gunakan SQLite CLI untuk melihat data:
```bash
cd /opt/airflow/data
sqlite3 etl_database.db
```
3. Di dalam SQLite CLI, Anda dapat melihat tabel dan data:
```sql
.tables
SELECT * FROM pengguna;
SELECT * FROM posts;
```
Untuk database multi-source:
```sql
sqlite3 multi_source_db.db
.tables
SELECT * FROM penduduk;
SELECT * FROM pengguna;
SELECT * FROM tugas_selesai;
SELECT * FROM pengguna_dan_tugas;
```

# versi advanced
misal kita mau tarik data tiap 5 menit
```python
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import pandas as pd
import requests
import sqlite3
import os
import json
from airflow.models import Variable

# Tentukan argumen default
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

# Definisikan DAG
dag = DAG(
    'weather_data_etl',
    default_args=default_args,
    description='Mengambil data cuaca dari API setiap 5 menit',
    schedule_interval=timedelta(minutes=5),  # Jadwal setiap 5 menit
    catchup=False
)

# Kota yang akan dipantau cuacanya
CITIES = [
    {"id": 1642911, "name": "Jakarta", "country": "ID"},
    {"id": 1650357, "name": "Bandung", "country": "ID"},
    {"id": 1625812, "name": "Surabaya", "country": "ID"},
    {"id": 1646170, "name": "Medan", "country": "ID"},
    {"id": 1621177, "name": "Yogyakarta", "country": "ID"}
]

# Fungsi untuk mengekstrak data dari API OpenWeatherMap
def extract_weather_data(**kwargs):
    """Ekstrak data cuaca dari OpenWeatherMap API"""
    # API Key OpenWeatherMap dapat didaftarkan secara gratis di openweathermap.org
    # Dalam lingkungan produksi, gunakan Airflow Variables atau Connections
    # Untuk demo ini, gunakan kunci API gratis atau dummy
    api_key = "YOUR_API_KEY_HERE"  # Ganti dengan API key Anda
    
    # Untuk tujuan demo, jika API key tidak diisi, gunakan data dummy
    use_dummy_data = True if api_key == "YOUR_API_KEY_HERE" else False
    
    all_weather_data = []
    
    # Pastikan direktori data ada
    os.makedirs('/opt/airflow/data', exist_ok=True)
    
    # Timestamp untuk penamaan file dan tracking
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    if use_dummy_data:
        # Gunakan data dummy untuk demo
        for city in CITIES:
            dummy_data = {
                "city_id": city["id"],
                "city_name": city["name"],
                "country": city["country"],
                "timestamp": timestamp,
                "temp": round(25 + (hash(city["name"] + timestamp) % 10) - 5, 1),  # Temp antara 20-30°C
                "feels_like": round(26 + (hash(city["name"] + "feels" + timestamp) % 10) - 5, 1),
                "temp_min": round(23 + (hash(city["name"] + "min" + timestamp) % 5) - 2.5, 1),
                "temp_max": round(28 + (hash(city["name"] + "max" + timestamp) % 5) - 2.5, 1),
                "pressure": 1010 + (hash(city["name"] + timestamp) % 20),
                "humidity": 60 + (hash(city["name"] + "hum" + timestamp) % 30),
                "wind_speed": round(3 + (hash(city["name"] + "wind" + timestamp) % 6), 1),
                "weather_main": "Clouds" if (hash(city["name"] + timestamp) % 3 == 0) else 
                               "Clear" if (hash(city["name"] + timestamp) % 3 == 1) else "Rain",
                "weather_description": "scattered clouds" if (hash(city["name"] + timestamp) % 3 == 0) else 
                                      "clear sky" if (hash(city["name"] + timestamp) % 3 == 1) else "light rain"
            }
            all_weather_data.append(dummy_data)
        
        print("Menggunakan data dummy karena API key tidak diatur.")
    else:
        # Gunakan API sebenarnya
        base_url = "http://api.openweathermap.org/data/2.5/weather"
        
        for city in CITIES:
            params = {
                'id': city["id"],
                'appid': api_key,
                'units': 'metric'  # Suhu dalam Celsius
            }
            
            try:
                response = requests.get(base_url, params=params, timeout=10)
                response.raise_for_status()
                
                data = response.json()
                
                weather_data = {
                    "city_id": city["id"],
                    "city_name": city["name"],
                    "country": city["country"],
                    "timestamp": timestamp,
                    "temp": data["main"]["temp"],
                    "feels_like": data["main"]["feels_like"],
                    "temp_min": data["main"]["temp_min"],
                    "temp_max": data["main"]["temp_max"],
                    "pressure": data["main"]["pressure"],
                    "humidity": data["main"]["humidity"],
                    "wind_speed": data["wind"]["speed"],
                    "weather_main": data["weather"][0]["main"],
                    "weather_description": data["weather"][0]["description"]
                }
                
                all_weather_data.append(weather_data)
                
            except requests.exceptions.RequestException as e:
                print(f"Error mengambil data untuk {city['name']}: {str(e)}")
                # Tambahkan data dummy untuk kota yang gagal diambil
                all_weather_data.append({
                    "city_id": city["id"],
                    "city_name": city["name"],
                    "country": city["country"],
                    "timestamp": timestamp,
                    "temp": 25.0,
                    "feels_like": 26.0,
                    "temp_min": 24.0,
                    "temp_max": 27.0,
                    "pressure": 1010,
                    "humidity": 70,
                    "wind_speed": 3.5,
                    "weather_main": "Unknown",
                    "weather_description": "Data unavailable"
                })
    
    # Simpan data mentah ke JSON sebagai backup
    raw_data_path = f'/opt/airflow/data/weather_raw_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json'
    with open(raw_data_path, 'w') as f:
        json.dump(all_weather_data, f)
    
    # Konversi ke DataFrame dan simpan ke XCom
    df = pd.DataFrame(all_weather_data)
    kwargs['ti'].xcom_push(key='weather_data', value=df.to_json(orient='records'))
    
    return f"Data cuaca berhasil diambil pada {timestamp} untuk {len(all_weather_data)} kota"

def transform_weather_data(**kwargs):
    """Transform data cuaca"""
    ti = kwargs['ti']
    data_json = ti.xcom_pull(task_ids='extract_weather_task', key='weather_data')
    df = pd.read_json(data_json, orient='records')
    
    # Konversi timestamp ke datetime
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    
    # Tambah kolom waktu_lokal (untuk analisis berdasarkan waktu)
    df['waktu_lokal'] = df['timestamp'].dt.strftime('%H:%M')
    
    # Tambah kolom tanggal
    df['tanggal'] = df['timestamp'].dt.date
    
    # Kategorisasi suhu
    df['kategori_suhu'] = pd.cut(
        df['temp'],
        bins=[0, 20, 25, 30, 100],
        labels=['Dingin', 'Normal', 'Hangat', 'Panas']
    )
    
    # Kategorisasi kelembaban
    df['kategori_kelembaban'] = pd.cut(
        df['humidity'],
        bins=[0, 30, 60, 80, 100],
        labels=['Kering', 'Normal', 'Lembab', 'Sangat Lembab']
    )
    
    # Tambah kolom cuaca_sederhana
    weather_mapping = {
        'Clear': 'Cerah',
        'Clouds': 'Berawan',
        'Rain': 'Hujan',
        'Drizzle': 'Gerimis',
        'Thunderstorm': 'Badai',
        'Snow': 'Salju',
        'Mist': 'Berkabut',
        'Smoke': 'Berasap',
        'Haze': 'Berkabut',
        'Dust': 'Berdebu',
        'Fog': 'Berkabut',
        'Sand': 'Berpasir',
        'Ash': 'Abu Vulkanik',
        'Squall': 'Angin Kencang',
        'Tornado': 'Tornado'
    }
    
    df['cuaca_sederhana'] = df['weather_main'].map(weather_mapping).fillna('Lainnya')
    
    # Tambah kolom rekomendasi_aktivitas
    def get_activity_recommendation(row):
        if row['weather_main'] in ['Rain', 'Thunderstorm', 'Drizzle']:
            return 'Aktivitas Dalam Ruangan'
        elif row['temp'] > 30:
            return 'Hindari Aktivitas di Luar Ruangan'
        elif row['weather_main'] == 'Clear' and 20 <= row['temp'] <= 30:
            return 'Ideal untuk Aktivitas Luar Ruangan'
        else:
            return 'Aktivitas Normal'
    
    df['rekomendasi_aktivitas'] = df.apply(get_activity_recommendation, axis=1)
    
    # Simpan hasil transformasi ke XCom
    kwargs['ti'].xcom_push(key='transformed_weather_data', value=df.to_json(orient='records'))
    
    return f"Data cuaca berhasil ditransformasi dengan {len(df)} baris data"

def load_weather_data(**kwargs):
    """Load data cuaca ke SQLite database"""
    ti = kwargs['ti']
    data_json = ti.xcom_pull(task_ids='transform_weather_task', key='transformed_weather_data')
    df = pd.read_json(data_json, orient='records')
    
    # Pastikan direktori ada
    os.makedirs('/opt/airflow/data', exist_ok=True)
    
    # Koneksi ke SQLite
    db_path = '/opt/airflow/data/weather_database.db'
    conn = sqlite3.connect(db_path)
    
    # Simpan data ke tabel saat ini
    df.to_sql('weather_current', conn, if_exists='replace', index=False)
    
    # Tambahkan juga ke tabel historis
    df.to_sql('weather_history', conn, if_exists='append', index=False)
    
    # Hitung ringkasan statistik
    stats_query = """
    SELECT 
        city_name,
        COUNT(*) as jumlah_data,
        AVG(temp) as rata_suhu,
        MAX(temp) as suhu_tertinggi,
        MIN(temp) as suhu_terendah,
        MAX(timestamp) as data_terakhir
    FROM weather_history
    GROUP BY city_name
    """
    
    stats_df = pd.read_sql_query(stats_query, conn)
    stats_df.to_sql('weather_stats', conn, if_exists='replace', index=False)
    
    # Bersihkan data historis yang lebih lama dari 7 hari
    cleanup_query = """
    DELETE FROM weather_history
    WHERE timestamp < datetime('now', '-7 day')
    """
    conn.execute(cleanup_query)
    
    # Dapatkan jumlah data dalam tabel historis
    count_query = "SELECT COUNT(*) FROM weather_history"
    cursor = conn.cursor()
    cursor.execute(count_query)
    history_count = cursor.fetchone()[0]
    
    # Tutup koneksi
    conn.close()
    
    return f"Data cuaca berhasil disimpan. {len(df)} rekaman baru, {history_count} total dalam sejarah"

# Definisikan tasks
extract_weather_task = PythonOperator(
    task_id='extract_weather_task',
    python_callable=extract_weather_data,
    provide_context=True,
    dag=dag,
)

transform_weather_task = PythonOperator(
    task_id='transform_weather_task',
    python_callable=transform_weather_data,
    provide_context=True,
    dag=dag,
)

load_weather_task = PythonOperator(
    task_id='load_weather_task',
    python_callable=load_weather_data,
    provide_context=True,
    dag=dag,
)

# Atur dependensi task
extract_weather_task >> transform_weather_task >> load_weather_task
```

setelah dibuat, anda bisa membuat dashboard (silakan lihat di `weather_dashboard.py`). berikut cara menjalankan:
```bash
python -m streamlit run app.py
```