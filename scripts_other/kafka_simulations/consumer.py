import sys
import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PARENT_DIR = os.path.dirname(BASE_DIR)
sys.path.append(PARENT_DIR)

class JunctionConsumer:
    def __init__(self,
                 consumer,
                 measuring_point):
        
        self.consumer = consumer
        self.measuring_point = measuring_point
        
        print('___________________________________________________________________')
        print('junction conumer created \nmeasuring_point ', self.measuring_point)
        print('consumer_group ', self.consumer.config['group_id'])
        print('___________________________________________________________________')


    def monitor_sensors_state(self):
        sensors_states = self.read_last_heartbeat_per_sensor()
        print(sensors_states)
        print("monitor_sensors_state")


    def read_topic(self):
        while True:
            data = self.consumer.poll(timeout_ms=1000)

            if not data:
                print("no data")
                continue

            for tp, msgs in data.items():
                for msg in msgs:
                    print(msg.value)
        
    def read_last_heartbeat_per_sensor(self, timeout_ms=1000):
        sensors_last_states = {}

        while True:
            records = self.consumer.poll(timeout_ms=timeout_ms)

            if not records:
                break 

            for tp, msgs in records.items():
                for msg in msgs:
                    data = msg.value
                    measuring_point = data.get("measuring_point")

                    if measuring_point:
                        sensors_last_states[measuring_point] = data

            #for state in sensors_last_states:
            #    print(f'point: {state}, last state: {sensors_last_states[state]}')

        return sensors_last_states