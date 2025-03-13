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