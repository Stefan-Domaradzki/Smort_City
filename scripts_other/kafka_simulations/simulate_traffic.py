import json
import logging
import pandas as pd
import numpy as np
import random

import time
from datetime import datetime



from kafka import KafkaProducer

import os
from dotenv import load_dotenv


def create_kafka_logger():

    base_dir = os.path.dirname(os.path.abspath(__file__))
    logs_path = os.path.join(base_dir, '..', 'logs', 'kafka')

    log_file = os.path.join(logs_path, 'connector.log')

    connector_logger = logging.getLogger(__name__)
    connector_logger.setLevel(logging.DEBUG)

    formatter = logging.Formatter('%(asctime)s:%(levelname)s:%(message)s')

    file_handler = logging.FileHandler(log_file)
    file_handler.setFormatter(formatter)

    connector_logger.addHandler(file_handler)

    return connector_logger


def run_simulation():
    
    logger = create_kafka_logger()

    load_dotenv()

    kafka_bootstrap_servers = os.getenv("KAFKA_BOOSTRAP_SERVERS")
    topic = os.getenv('SIMULATION_TOPIC')
    
    producer = KafkaProducer(
        bootstrap_servers=kafka_bootstrap_servers,
        value_serializer=lambda m: json.dumps(m).encode('utf-8')
    )
    
    # csv containing Random Variable
    csv_path = "./data/processed/ruch_rzeszow_ZL.csv"
    df = pd.read_csv(csv_path, sep =';', decimal =',')


    df.columns = ["index", "hour", "direction", "total_vehicles", "prob", "prob_s"]

    hour_map = {}
    for hour, group in df.groupby("hour"):
        directions = group["direction"].values
        probs = group["prob_s"].astype(str).str.replace(",", ".").astype(float).values
        hour_map[hour] = (directions, probs)


    logger.info("Symulacja rozpoczęta.")

    try:
        while True:
            current_hour = datetime.now().hour
            print(current_hour)
            if current_hour in hour_map:
                directions, probs = hour_map[current_hour]
                chosen_direction = np.random.choice(directions, p=probs)

                message = {
                    "timestamp": datetime.now().isoformat(),
                    "hour": current_hour,
                    "direction": chosen_direction
                }

                producer.send(topic, message)
                producer.flush()
                logger.info(f"Wysłano: {message}")
            else:
                pass
                logger.warning(f"Brak danych dla godziny {current_hour}")

            time.sleep(1)

    except KeyboardInterrupt:
        pass
        logger.info("Symulacja przerwana klawiszem.")
    except Exception as e:
        logger.exception(f"Błąd w symulacji: {str(e)}")
        pass
    finally:
        producer.close()


def run_simulation_test():
    
    logger = create_kafka_logger()

    load_dotenv()

    kafka_bootstrap_servers = os.getenv("KAFKA_BOOSTRAP_SERVERS")
    topic = os.getenv('SIMULATION_TOPIC')
    
    producer = KafkaProducer(
        bootstrap_servers=kafka_bootstrap_servers,
        value_serializer=lambda m: json.dumps(m).encode('utf-8')
    )
    

    directions = [
            "Brak",
            "Lewoskręt",
            "Na wprost",
            "Na wprost i Lewoskręt",
            "Na wprost i Prawoskręt",
            "Prawoskręt",
            "Zawrotka"
        ]

    approaches = ['N', 'S', 'E', 'W']




    logger.info("Symulacja rozpoczęta.")

    try:
        while True:

            message = {
                "measuring_point": "SIM-1",                      
                "location": "Rzeszów, Al. Piłsudskiego",         
                "approach": random.choice(approaches),
                "direction": random.choice(directions),
                "date_time": datetime.now().isoformat(),
                "traffic_count": random.randint(0, 1)
            }
            print(message)
            producer.send(topic, message)
            producer.flush()
            print(f"Wysłano: {message}")
            # np. 1 wiadomość co 1 sekundę:
            time.sleep(1)

    except KeyboardInterrupt:
        pass
        logger.info("Symulacja przerwana klawiszem.") 
    except Exception as e:
        logger.exception(f"Błąd w symulacji: {str(e)}")
        pass
    finally:
        producer.close()



if __name__ == "__main__":
    run_simulation_test()