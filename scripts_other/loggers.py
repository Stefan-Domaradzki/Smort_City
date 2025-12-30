import logging
import os

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