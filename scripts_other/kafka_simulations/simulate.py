from kafka import KafkaProducer
import json
import time 
import os
import sys

from kafka import KafkaProducer




BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PARENT_DIR = os.path.dirname(BASE_DIR)
sys.path.append(PARENT_DIR)

from producer import JunctionSimulator
from loggers import create_kafka_logger

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8'))
logger = create_kafka_logger()
    
simulator_1 = JunctionSimulator(logger, producer, "MQ_K1", sim_timeout=600)
simulator_2 = JunctionSimulator(logger, producer, "MQ_K49", sim_timeout=480)

simulator_3 = JunctionSimulator(logger=logger,
                                kafka_producer=producer,
                                measuring_point="newtest",
                                sim_timeout=480)

try:
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    print("End of simulation")

