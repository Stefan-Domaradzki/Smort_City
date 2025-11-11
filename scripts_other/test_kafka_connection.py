from kafka import KafkaConsumer
import json
import os
from dotenv import load_dotenv

def read_kafka_messages():
    topic = os.getenv('SIMULATION_TOPIC')
    bootstrap_servers = 'localhost:9092'

    # Inicjalizacja konsumenta
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        auto_offset_reset='latest',  # lub 'earliest' jeśli chcesz od początku
        enable_auto_commit=True,
        group_id='smartcity_reader',
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )

    print(f"Naszłuchuję temat: {topic}")
    try:
        for message in consumer:
            print(f"Odebrano: {message.value}")
    except KeyboardInterrupt:
        print("Przerwano odbiór wiadomości.")

if __name__ == "__main__":
    read_kafka_messages()
