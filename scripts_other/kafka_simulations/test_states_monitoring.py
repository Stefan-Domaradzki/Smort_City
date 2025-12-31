from consumer import JunctionConsumer
from kafka import KafkaConsumer
import json

kafka_consumer = KafkaConsumer(
    'heartbeat',
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    group_id=None,
    auto_offset_reset='earliest',
    enable_auto_commit=False,
    )

status_consumer = JunctionConsumer(kafka_consumer,"heartbeat")
#sensors_states = status_consumer.read_last_heartbeat_per_sensor()
#print(sensors_states)
status_consumer.monitor_sensors_state()








kafka_consumer.close()



