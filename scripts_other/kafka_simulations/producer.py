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

            self.alert_malfunction = ["camera_damaged", "energy_shortage", "connection_temporarily_lost"]

            self.stop_event = threading.Event()
            self.thread = threading.Thread(target=self._run, daemon=True)
            self.thread.start()

            self.logger.info("Symulacja startuje")
            print("Hello Smart_city! I'm:", self.measuring_point)

    def _run(self):
        start_time = time.time()

        while not self.stop_event.is_set():
            elapsed = time.time() - start_time

            if elapsed >= self.sim_timeout:
                self.logger.info("Simulation timeout")
                break

            car = random.choices(population=[0, 1], weights=[1-self.probability, self.probability],k=1)[0]

            if car:
                message = {
                    "measuring_point": self.measuring_point,
                    "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                }

                try:
                    self.kafka_producer.send("sim_test_1", value=message)
                    self.kafka_producer.flush()
                    #self.logger.debug(f"Wysłano wiadomość: {message}")
                except Exception as e:
                    self.logger.error(f"Błąd przy wysyłaniu wiadomości: {e}")

            time.sleep(1)

    def stop(self):
        """Zatrzymanie symulatora ręcznie."""
        self.stop_event.set()
        self.logger.info("Symulator zatrzymany ręcznie.")
