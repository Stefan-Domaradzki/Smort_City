# consumer_to_db_traffic.py
from kafka import KafkaConsumer
import json
import psycopg2
import os
from dotenv import load_dotenv
from datetime import datetime, timedelta

# ---- Funkcja połączenia z bazą ----
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
        print("✅ DB connected")
        return connection
    except Exception as e:
        print(f"❌ DB connection failed: {e}")
        return None
# -----------------------------------

# Połączenie z bazą
conn = connect_to_db()
if not conn:
    raise SystemExit("Brak połączenia z bazą danych.")
cursor = conn.cursor()

# Tworzenie tabeli, jeśli nie istnieje
cursor.execute("""
CREATE TABLE IF NOT EXISTS RZ_Traffic (
    id SERIAL PRIMARY KEY,
    measuring_point VARCHAR(50),
    location VARCHAR(100),
    approach VARCHAR(50),
    direction VARCHAR(50),
    date_time TIMESTAMP,
    traffic_count INT
);
""")
conn.commit()

# Konsument Kafki
consumer = KafkaConsumer(
    'sroda_test',
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='traffic_group'
)

print("📥 Oczekiwanie na dane z tematu 'traffic_data'...")

# Bufor do agregacji danych godzinowych
hourly_data = {}

for msg in consumer:
    data = msg.value

    # Parsowanie czasu i zaokrąglenie do pełnej godziny
    dt = datetime.strptime(data['date_time'], "%Y-%m-%d %H:%M:%S")
    dt_hour = dt.replace(minute=0, second=0, microsecond=0)

    key = (data['measuring_point'], dt_hour)

    # Sumowanie wartości traffic_count
    if key not in hourly_data:
        hourly_data[key] = {
            'measuring_point': data['measuring_point'],
            'location': data['location'],
            'approach': data['approach'],
            'direction': data['direction'],
            'date_time': dt_hour,
            'traffic_count': data['traffic_count']
        }
    else:
        hourly_data[key]['traffic_count'] += data['traffic_count']

    # Gdy minie godzina od początku danej grupy — zapis do bazy
    current_time = datetime.now()
    for (point, hour), record in list(hourly_data.items()):
        if current_time >= hour + timedelta(hours=1):
            # Sprawdzenie duplikatu
            cursor.execute("""
                SELECT 1 FROM RZ_Traffic 
                WHERE measuring_point = %s AND date_time = %s
            """, (point, hour))
            exists = cursor.fetchone()

            if not exists:
                cursor.execute("""
                    INSERT INTO RZ_Traffic 
                    (measuring_point, location, approach, direction, date_time, traffic_count)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, (
                    record['measuring_point'],
                    record['location'],
                    record['approach'],
                    record['direction'],
                    record['date_time'],
                    record['traffic_count']
                ))
                conn.commit()
                print(f"💾 Zapisano godzinową sumę: {record}")
            else:
                print(f"⚠️ Duplikat pominięty: {point} {hour}")

            # Usunięcie zapisanej grupy z bufora
            del hourly_data[(point, hour)]
