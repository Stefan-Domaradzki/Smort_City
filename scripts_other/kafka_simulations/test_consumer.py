from kafka import KafkaConsumer
import json

from consumer import JunctionConsumer

consumer = KafkaConsumer(
    'heartbeat',
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    group_id=None,
    auto_offset_reset='earliest',
    enable_auto_commit=False,
)

consumer1 = JunctionConsumer(consumer,"test_heartbeat")
#consumer1.read_topic()
x = consumer1.read_last_heartbeat_per_sensor()
print(x)
