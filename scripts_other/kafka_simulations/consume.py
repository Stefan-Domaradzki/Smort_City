from kafka import KafkaConsumer
import json
import psycopg2
import os
from dotenv import load_dotenv
from datetime import datetime, timedelta


consumer = KafkaConsumer(
    'MQ_K49',
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='traffic_group'
)

print("📥 Oczekiwanie na dane z tematu 'traffic_data'...")

for msg in consumer:
    data = msg.value

    print(data, '\n')