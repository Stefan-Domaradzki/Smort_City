# consumer_to_db_traffic.py
from kafka import KafkaConsumer
import json
import psycopg2
import os
from dotenv import load_dotenv

# ---- Twoja funkcja ----
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
# ------------------------

# Połączenie z bazą
conn = connect_to_db()
if not conn:
    raise SystemExit("Brak połączenia z bazą danych. Zakończono działanie.")

cursor = conn.cursor()

# Upewnij się, że tabela istnieje
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
topic_name = 'sroda_test'

consumer = KafkaConsumer(
    topic_name,
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='traffic_group'
)

print("📥 Oczekiwanie na dane z tematu 'traffic_data'...")

for msg in consumer:
    data = msg.value

    # Sprawdzenie duplikatu po measuring_point + date_time
    cursor.execute("""
        SELECT 1 FROM RZ_Traffic 
        WHERE measuring_point = %s AND date_time = %s
    """, (data['measuring_point'], data['date_time']))
    exists = cursor.fetchone()

    if not exists:
        cursor.execute("""
            INSERT INTO RZ_Traffic 
            (measuring_point, location, approach, direction, date_time, traffic_count)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (
            data['measuring_point'],
            data['location'],
            data['approach'],
            data['direction'],
            data['date_time'],
            data['traffic_count']
        ))
        conn.commit()
        print("💾 Zapisano:", data)
    else:
        print("⚠️ Pominięto duplikat:", data)
