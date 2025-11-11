# producer_traffic.py
from kafka import KafkaProducer
import json
from datetime import datetime

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Przykładowa wiadomość z danymi ruchu
message = {
    "measuring_point": "TEST_1",
    "location": "ul. Dąbrowskiego - ul. Pola",
    "approach": "Wszystkie wloty",
    "direction": "Wszystkie",
    "date_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    "traffic_count": 300
}

topic_name = 'sroda_test'

producer.send(topic_name, value=message)
producer.flush()
print("Wysłano wiadomość:", message, " na temat: ", topic_name)
