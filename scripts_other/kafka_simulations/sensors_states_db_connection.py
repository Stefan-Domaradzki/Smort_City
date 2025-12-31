import psycopg2 
import os
import json 


from dotenv import load_dotenv
from datetime import datetime
from kafka import KafkaConsumer
from consumer import JunctionConsumer

kafka_consumer = KafkaConsumer(
    'heartbeat',
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    group_id=None,
    auto_offset_reset='earliest',
    enable_auto_commit=False,
    )

status_consumer = JunctionConsumer(kafka_consumer,"test_heartbeat")
sensors_states = status_consumer.read_last_heartbeat_per_sensor()

kafka_consumer.close()

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


connection = connect_to_db()
   
if connection:
    print('CONNECTED!')
    
    try:
        cursor = connection.cursor()

        query = """
        INSERT INTO sensors_states (
            measuring_point,
            sensor_state,
            last_registered_malfunction,
            last_update
        )
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (measuring_point)
        DO UPDATE SET
            sensor_state = EXCLUDED.sensor_state,
            last_registered_malfunction = EXCLUDED.last_registered_malfunction,
            last_update = EXCLUDED.last_update;
        """

        for sensor_id, data in sensors_states.items():
            cursor.execute(
                query,
                (
                    data["measuring_point"],
                    data.get("sensor_state") or data.get("type") or "UNKNOWN",
                    data.get("last_registered_malfunction"),
                    datetime.strptime(data["timestamp"], "%Y-%m-%d %H:%M:%S")
                )
            )

        connection.commit()
        cursor.close()
    except Exception as e:
        print('error: ', e)
        pass
