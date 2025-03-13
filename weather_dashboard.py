import streamlit as st
import pandas as pd
import sqlite3
import matplotlib.pyplot as plt
import seaborn as sns
import plotly.express as px
from datetime import datetime, timedelta
import os

# Konfigurasi halaman
st.set_page_config(page_title="Dashboard Cuaca Realtime", layout="wide")

# Fungsi untuk menghubungkan ke database
@st.cache_resource
def get_connection():
    # conn = sqlite3.connect('/opt/airflow/data/weather_database.db') # jika pakai linux
    conn = sqlite3.connect('data/weather_database.db')
    return conn

# Fungsi untuk memuat data
@st.cache_data(ttl=300)  # Cache data selama 5 menit
def load_data(query):
    conn = get_connection()
    df = pd.read_sql_query(query, conn)
    return df

# Judul Dashboard
st.title("🌦️ Dashboard Monitoring Cuaca Realtime")
st.markdown("Dashboard ini menampilkan data cuaca terkini dan historis dari beberapa kota di Indonesia")

# Sidebar untuk filter
st.sidebar.title("Filter Data")

# Load data untuk filter
all_weather_data = load_data("SELECT * FROM weather_history ORDER BY timestamp DESC")
cities = all_weather_data['city_name'].unique()
weather_types = all_weather_data['cuaca_sederhana'].unique()

# Filter kota
selected_cities = st.sidebar.multiselect(
    "Pilih Kota", 
    options=cities,
    default=cities
)

# Filter rentang waktu
date_range = st.sidebar.date_input(
    "Rentang Tanggal",
    value=[datetime.now().date() - timedelta(days=7), datetime.now().date()]
)

# Filter jenis cuaca
selected_weather = st.sidebar.multiselect(
    "Jenis Cuaca",
    options=weather_types,
    default=weather_types
)

# Konversi rentang tanggal ke string untuk query SQL
if len(date_range) == 2:
    start_date, end_date = date_range
    start_date_str = start_date.strftime('%Y-%m-%d')
    end_date_str = (end_date + timedelta(days=1)).strftime('%Y-%m-%d')  # Include end date
else:
    # Default to last 7 days if date range is not complete
    end_date_str = (datetime.now().date() + timedelta(days=1)).strftime('%Y-%m-%d')
    start_date_str = (datetime.now().date() - timedelta(days=7)).strftime('%Y-%m-%d')

# Buat string daftar kota dan jenis cuaca untuk query
cities_str = ', '.join([f"'{city}'" for city in selected_cities])
weather_str = ', '.join([f"'{w}'" for w in selected_weather])

# Query data berdasarkan filter
if selected_cities and selected_weather:
    filtered_data_query = f"""
    SELECT * FROM weather_history 
    WHERE city_name IN ({cities_str})
    AND cuaca_sederhana IN ({weather_str})
    AND timestamp BETWEEN '{start_date_str}' AND '{end_date_str}'
    ORDER BY timestamp DESC
    """
elif selected_cities:
    filtered_data_query = f"""
    SELECT * FROM weather_history 
    WHERE city_name IN ({cities_str})
    AND timestamp BETWEEN '{start_date_str}' AND '{end_date_str}'
    ORDER BY timestamp DESC
    """
elif selected_weather:
    filtered_data_query = f"""
    SELECT * FROM weather_history 
    WHERE cuaca_sederhana IN ({weather_str})
    AND timestamp BETWEEN '{start_date_str}' AND '{end_date_str}'
    ORDER BY timestamp DESC
    """
else:
    filtered_data_query = f"""
    SELECT * FROM weather_history 
    WHERE timestamp BETWEEN '{start_date_str}' AND '{end_date_str}'
    ORDER BY timestamp DESC
    """

filtered_data = load_data(filtered_data_query)

# Muat data statistik
stats_data = load_data("SELECT * FROM weather_stats")

# Tampilkan statistik secara transparan di bagian atas
col1, col2, col3 = st.columns(3)

with col1:
    st.metric("Total Pengukuran", len(filtered_data))

with col2:
    avg_temp = filtered_data['temp'].mean()
    st.metric("Suhu Rata-rata", f"{avg_temp:.1f}°C")

with col3:
    latest_time = filtered_data['timestamp'].max() if not filtered_data.empty else "No data"
    st.metric("Update Terakhir", latest_time)

# Tampilkan tab untuk berbagai visualisasi
tab1, tab2, tab3, tab4 = st.tabs(["Kondisi Terkini", "Tren Suhu", "Perbandingan Kota", "Data Mentah"])

with tab1:
    # Tampilkan kondisi cuaca terkini untuk setiap kota
    st.subheader("Kondisi Cuaca Terkini")
    
    # Filter untuk mendapatkan data terbaru untuk setiap kota
    current_data_query = """
    SELECT * FROM weather_current
    """
    current_data = load_data(current_data_query)
    
    # Tampilkan kartu cuaca untuk setiap kota
    cols = st.columns(len(current_data))
    
    for i, (_, row) in enumerate(current_data.iterrows()):
        with cols[i]:
            st.markdown(f"### {row['city_name']}")
            
            # Ikon cuaca
            weather_icon = "☀️" if row['cuaca_sederhana'] == 'Cerah' else "☁️" if row['cuaca_sederhana'] == 'Berawan' else "🌧️" if row['cuaca_sederhana'] == 'Hujan' else "⛈️" if row['cuaca_sederhana'] == 'Badai' else "🌫️"
            st.markdown(f"# {weather_icon} {row['temp']:.1f}°C")
            
            st.markdown(f"**Terasa seperti:** {row['feels_like']:.1f}°C")
            st.markdown(f"**Kelembaban:** {row['humidity']}%")
            st.markdown(f"**Angin:** {row['wind_speed']} m/s")
            st.markdown(f"**Kondisi:** {row['cuaca_sederhana']}")
            st.markdown(f"**Rekomendasi:** {row['rekomendasi_aktivitas']}")

with tab2:
    st.subheader("Tren Suhu dalam Periode Waktu yang Dipilih")
    
    if not filtered_data.empty:
        # Konversi timestamp ke datetime jika belum
        if not pd.api.types.is_datetime64_any_dtype(filtered_data['timestamp']):
            filtered_data['timestamp'] = pd.to_datetime(filtered_data['timestamp'])
        
        # Buat pivot table untuk tren suhu
        pivot_data = filtered_data.pivot_table(
            index='timestamp', 
            columns='city_name', 
            values='temp',
            aggfunc='mean'
        ).reset_index()
        
        # Plot dengan Plotly untuk interaktivitas
        fig = px.line(
            pivot_data, 
            x='timestamp',
            y=pivot_data.columns[1:],  # Semua kolom kecuali timestamp
            labels={'value': 'Suhu (°C)', 'timestamp': 'Waktu', 'variable': 'Kota'},
            title='Tren Suhu per Kota'
        )
        st.plotly_chart(fig, use_container_width=True)
        
        # Tampilkan distribusi suhu dengan boxplot
        st.subheader("Distribusi Suhu per Kota")
        fig2 = px.box(
            filtered_data,
            x='city_name',
            y='temp',
            color='city_name',
            labels={'city_name': 'Kota', 'temp': 'Suhu (°C)'},
            title='Sebaran Suhu per Kota'
        )
        st.plotly_chart(fig2, use_container_width=True)
    else:
        st.warning("Tidak ada data untuk ditampilkan dengan filter yang dipilih.")

with tab3:
    st.subheader("Perbandingan Antar Kota")
    
    if not filtered_data.empty:
        # Agregasi data per kota
        city_agg = filtered_data.groupby('city_name').agg({
            'temp': ['mean', 'min', 'max'],
            'humidity': 'mean',
            'wind_speed': 'mean'
        }).reset_index()
        
        city_agg.columns = ['Kota', 'Suhu Rata-rata', 'Suhu Minimum', 'Suhu Maksimum', 'Kelembaban Rata-rata', 'Kecepatan Angin Rata-rata']
        
        # Buat radar chart/spider chart untuk perbandingan kota
        categories = ['Suhu Rata-rata', 'Suhu Minimum', 'Suhu Maksimum', 'Kelembaban Rata-rata', 'Kecepatan Angin Rata-rata']
        
        # Tampilkan dalam bentuk bar chart yang lebih sederhana
        fig3 = px.bar(
            city_agg,
            x='Kota',
            y='Suhu Rata-rata',
            color='Kota',
            title='Suhu Rata-rata per Kota'
        )
        st.plotly_chart(fig3, use_container_width=True)
        
        # Tabel perbandingan
        st.subheader("Tabel Perbandingan Kota")
        st.dataframe(city_agg.style.highlight_max(axis=0, subset=['Suhu Rata-rata', 'Suhu Maksimum', 'Kelembaban Rata-rata', 'Kecepatan Angin Rata-rata']))
        
        # Tampilkan histogram kondisi cuaca
        st.subheader("Distribusi Kondisi Cuaca per Kota")
        fig4 = px.histogram(
            filtered_data,
            x='cuaca_sederhana',
            color='city_name',
            barmode='group',
            labels={'cuaca_sederhana': 'Kondisi Cuaca', 'city_name': 'Kota', 'count': 'Jumlah Pengamatan'},
            title='Frekuensi Kondisi Cuaca per Kota'
        )
        st.plotly_chart(fig4, use_container_width=True)
    else:
        st.warning("Tidak ada data untuk ditampilkan dengan filter yang dipilih.")

with tab4:
    st.subheader("Data Mentah")
    # Tampilkan data mentah yang difilter
    st.dataframe(filtered_data)
    
    # Opsi untuk mengunduh data
    csv = filtered_data.to_csv(index=False).encode('utf-8')
    st.download_button(
        "Unduh Data sebagai CSV",
        csv,
        "data_cuaca.csv",
        "text/csv",
        key='download-csv'
    )

# Footer
st.markdown("---")
st.markdown("Dashboard dibuat dengan Streamlit • Data diperbarui setiap 5 menit oleh Airflow")