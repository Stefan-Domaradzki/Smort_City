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
    
simulator_1 = JunctionSimulator(logger, producer, "A_01", sim_timeout=600)
simulator_2 = JunctionSimulator(logger, producer, "A_02", sim_timeout=480)
simulator_3 = JunctionSimulator(logger, producer, "A_03", sim_timeout=60)
simulator_4 = JunctionSimulator(logger, producer, "A_04", sim_timeout=600)
simulator_5 = JunctionSimulator(logger, producer, "A_05", sim_timeout=600)
simulator_6 = JunctionSimulator(logger, producer, "A_06", sim_timeout=600)
simulator_7 = JunctionSimulator(logger, producer, "A_07", sim_timeout=600)
simulator_8 = JunctionSimulator(logger, producer, "A_08", sim_timeout=600)


simulator_1.start()
simulator_2.start()
simulator_3.start()
simulator_4.start()
simulator_5.start()
simulator_6.start()
simulator_7.start()
simulator_8.start()


print('startuje petla')

try:
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    print("End of simulation")

