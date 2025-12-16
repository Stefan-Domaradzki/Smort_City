import os
import psycopg2
import pandas as pd
from dotenv import load_dotenv

# --- 1. Funkcja połączenia do bazy ---
def connect_to_db():
    load_dotenv()
    
    try:
        connection = psycopg2.connect(
            dbname      = os.getenv("DB_NAME"),
            user        = os.getenv("DB_USER"),
            password    = os.getenv("DB_PASSWORD"),
            host        = os.getenv("DB_HOST", "localhost"),
            port        = os.getenv("DB_PORT", "5432")
        )
        print("DB connected")
        return connection
    
    except Exception as e:
        print(f"DB connection failed: {e}")
        return None


# --- 2. Wczytanie danych CSV ---
# Upewniamy się, że separator kolumn = ';' i dziesiętny = '.'
csv_file = "./processed/pogoda.aggregated5.csv"

pogoda = pd.read_csv(csv_file, sep=';', decimal='.')

# konwersja
pogoda['timestamp'] = pd.to_datetime(pogoda['timestamp'], errors='coerce')

# usuń wiersze z NaT
pogoda = pogoda[pd.notnull(pogoda['timestamp'])]


# --- 3. Połączenie z bazą ---
conn = connect_to_db()
if conn is None:
    exit(1)

cur = conn.cursor()

# --- 4. Utworzenie tabeli jeśli nie istnieje ---
create_table_query = """
CREATE TABLE IF NOT EXISTS rz_weather (
    timestamp TIMESTAMP PRIMARY KEY,
    temperature_2m REAL,
    dew_point_2m REAL,
    precipitation REAL,
    rain REAL,
    snowfall REAL,
    wind_speed_10m REAL,
    wind_direction_10m REAL,
    wind_gusts_10m REAL,
    cloud_cover SMALLINT,
    shortwave_radiation REAL
);
"""
cur.execute(create_table_query)
conn.commit()

# --- 5. Wstawianie danych do tabeli ---
# Używamy execute_batch dla wydajności
from psycopg2.extras import execute_batch

insert_query = """
INSERT INTO rz_weather (
    timestamp, temperature_2m, dew_point_2m, precipitation, rain, snowfall,
    wind_speed_10m, wind_direction_10m, wind_gusts_10m, cloud_cover, shortwave_radiation
) VALUES (
    %(timestamp)s, %(temperature_2m)s, %(dew_point_2m)s, %(precipitation)s, %(rain)s, %(snowfall)s,
    %(wind_speed_10m)s, %(wind_direction_10m)s, %(wind_gusts_10m)s, %(cloud_cover)s, %(shortwave_radiation)s
)
ON CONFLICT (timestamp) DO UPDATE SET
    temperature_2m = EXCLUDED.temperature_2m,
    dew_point_2m = EXCLUDED.dew_point_2m,
    precipitation = EXCLUDED.precipitation,
    rain = EXCLUDED.rain,
    snowfall = EXCLUDED.snowfall,
    wind_speed_10m = EXCLUDED.wind_speed_10m,
    wind_direction_10m = EXCLUDED.wind_direction_10m,
    wind_gusts_10m = EXCLUDED.wind_gusts_10m,
    cloud_cover = EXCLUDED.cloud_cover,
    shortwave_radiation = EXCLUDED.shortwave_radiation;
"""

# Konwersja timestamp na datetime
pogoda['timestamp'] = pd.to_datetime(pogoda['timestamp'])

# Zamiana wartości NaN na None, aby psycopg2 poprawnie wstawił NULL
pogoda = pogoda.where(pd.notnull(pogoda), None)

# Wstawiamy wsadowo
execute_batch(cur, insert_query, pogoda.to_dict(orient='records'), page_size=1000)

conn.commit()
cur.close()
conn.close()
print("Data inserted successfully")
