'''
Script used to make entry in kafka heartbeat topic for every measuring point in the system,

'''

from kafka import KafkaProducer
import json
from datetime import datetime
import os
import sys

import pandas as pd

from kafka import KafkaProducer


producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8'))

junction_list = pd.read_csv("./measuring_points.csv")
print(junction_list)

for index, row in junction_list.iterrows():

    message = {
            'measuring_point': row['measuring_point'],
            'sensor_state': 'STOPPED',
            'last_registered_malfunction': 'none',
            'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            }

    try:
        #producer.send('heartbeat', value=message)
        print(f'heartbeat: {message}')

    except Exception as e:
        print(f'Error occured during sending the message. Error: {e}')

#producer.flush()