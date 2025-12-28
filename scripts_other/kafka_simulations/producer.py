import json
import time 
import threading
import random

from datetime import datetime
from kafka import KafkaProducer

import sys
import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PARENT_DIR = os.path.dirname(BASE_DIR)
sys.path.append(PARENT_DIR)

class JunctionSimulator:
    def __init__(self,
                logger,
                kafka_producer,
                measuring_point,      
                probability=0.7,
                sim_timeout=3660):
            
            self.kafka_producer = kafka_producer
            self.logger = logger
            self.measuring_point = measuring_point
            self.probability = probability
            self.sim_timeout = sim_timeout

            self.alert_malfunction = ['camera_damaged', 'energy_shortage', 'connection_temporarily_lost']
            self.status = 'STARTING'
 
            self.stop_event = threading.Event()
            self.thread = threading.Thread(target=self._run, daemon=True)


            self.logger.info('Simulation starting: measuring point: %s at %s',
                             self.measuring_point,
                             datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
            
            self.heartbeat()
            self.thread.start()
            

    def _run(self):
        start_time = time.time()

        self.status = 'RUNNING'
        self.heartbeat()

        heartbeat_start = start_time

        while not self.stop_event.is_set():
            elapsed = time.time() - start_time

            if elapsed >= self.sim_timeout:
                self.logger.info('Simulation timeout')
                break

            malfuntion_prob = 0.0001
            malfuntion = random.choices(population=[0, 1], weights=[1-malfuntion_prob, malfuntion_prob],k=1)[0]
            
            if malfuntion:
                
                self.status = random.choices(population=self.alert_malfunction,k=1)

                self.logger.info(
                    'Malfunction detected: %s @ %s',
                    self.status[0],
                    datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                )

                self.heartbeat()
                self.stop()

            car = random.choices(population=[0, 1], weights=[1-self.probability, self.probability],k=1)[0]

            if car:
                message = {
                    'measuring_point': self.measuring_point,
                    'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                }

                try:
                    self.kafka_producer.send(self.measuring_point, value=message)
                    self.kafka_producer.flush()

                except Exception as e:
                    self.logger.error(f'Błąd przy wysyłaniu wiadomości: {e}')

            if time.time() - heartbeat_start > 60:
                print('heartbeat!')
                self.heartbeat()
                heartbeat_start = time.time()
                
            time.sleep(1)

    def heartbeat(self):

        message = {
            'measuring_point': self.measuring_point,
            'type': self.status,
            'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            }

        try:
            self.kafka_producer.send('heartbeat', value=message)
            self.kafka_producer.flush()

            print(f'heartbeat: {message}')

        except Exception as e:
            self.logger.error(f'Error occured during sending the message. Error: {e}')

    def stop(self):
        """Zatrzymanie symulatora ręcznie."""
        self.status = 'STOPPED' 
        self.stop_event.set()
        self.logger.info("Symulator zatrzymany ręcznie.")
