import sys
import os
import psycopg2

from dotenv import load_dotenv

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PARENT_DIR = os.path.dirname(BASE_DIR)
sys.path.append(PARENT_DIR)

class JunctionConsumer:
    def __init__(self,
                 consumer,
                 measuring_point):
        
        self.consumer = consumer
        self.measuring_point = measuring_point
        
        self.consumer = consumer
        self.db_connection = self._connect_to_db()
        self.db_cursor = self.db_connection.cursor()      
        
        self.running = True

        print('___________________________________________________________________')
        print('junction conumer created \nmeasuring_point ', self.measuring_point)
        print('consumer_group ', self.consumer.config['group_id'])
        print('___________________________________________________________________')


    def monitor_sensors_state(self):
        
        def update_states(data):
            for tp, msgs in data.items():
                for msg in msgs:

                    payload = msg.value

                    s_measuring_point = payload['measuring_point']
                    s_state = payload['sensor_state']

                    if sensors_states[s_measuring_point]['sensor_state'] != s_state:

                        sensors_states[s_measuring_point] = payload

                        self.db_cursor.execute(
                            UPSERT_SENSOR_STATE_SQL,
                            (
                                payload['measuring_point'],
                                payload['sensor_state'],
                                payload.get('last_registered_malfunction', 'none'),
                                payload['timestamp']
                            )
                        )
                        self.db_connection.commit()
        
        sensors_states = self.read_last_heartbeat_per_sensor() 
        
        UPSERT_SENSOR_STATE_SQL = """
            INSERT INTO sensors_states (
                measuring_point,
                sensor_state,
                last_registered_malfunction,
                last_update
            )
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (measuring_point)
            DO UPDATE SET
                sensor_state = EXCLUDED.sensor_state,
                last_registered_malfunction = EXCLUDED.last_registered_malfunction,
                last_update = EXCLUDED.last_update;
            """

        try:
            while True:
                data = self.consumer.poll(timeout_ms=5000)

                if not data:
                    print("no data")
                    continue

                else: update_states(data)

        except KeyboardInterrupt:
            print('Keyboard Intteruption!')
        finally:
            self._cleanup()

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

        return sensors_last_states

    def _connect_to_db(self):
        load_dotenv()
        
        try:
            connection = psycopg2.connect(
                dbname      = os.getenv("DB_NAME"),
                user        = os.getenv("DB_USER"),
                password    = os.getenv("DB_PASSWORD"),
                host        = os.getenv("DB_HOST", "localhost"),
                port        = os.getenv("DB_PORT", "5432")
            )
            print("DB connected")
            return connection
        
        except Exception as e:
            print(f"DB connection failed: {e}")
            return None

    def _cleanup(self):
        self.consumer.close()
        self.db_cursor.close()
        self.db_connection.close()