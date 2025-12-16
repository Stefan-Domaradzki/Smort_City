import psycopg2
from datetime import datetime
from dotenv import load_dotenv
import os


def convert_date(date_str):
    return datetime.strptime(date_str, "%d.%m.%Y %H:%M").strftime("%Y-%m-%d %H:%M:%S")

def connect_to_db():

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


def create_table(connection):
    
    cursor = connection.cursor()

    create_table_query = '''
    CREATE TABLE IF NOT EXISTS RZ_Traffic (
        id SERIAL PRIMARY KEY,
        measuring_point VARCHAR(50),
        location VARCHAR(100),
        approach VARCHAR(50),
        direction VARCHAR(50),
        date_time TIMESTAMP,
        traffic_count INT
    );
    '''
    cursor.execute(create_table_query)
    connection.commit()
    print("Table created or already existing")


def insert_sample_data(connection):

    data = {
        "measuring_point": "MQ_K1",
        "location": "ul. Lwowska - ul. Cienista",
        "approach": "Wszyskie wloty",
        "direction": "Wszystkie",
        "date_time": "15.03.2021 01:00",
        "traffic_count": 92
    }

    data["date_time"] = convert_date(data["date_time"])

    insert_query = '''
    INSERT INTO RZ_Traffic (measuring_point, location, approach, direction, date_time, traffic_count)
    VALUES (%s, %s, %s, %s, %s, %s);
    '''
    cursor = connection.cursor()
    cursor.execute(insert_query, (
        data["measuring_point"],
        data["location"],
        data["approach"],
        data["direction"],
        data["date_time"],
        data["traffic_count"]
    ))
    connection.commit()
    print("Data inserted successfully")


def copy_data_from_csv(connection, csv_file_path):
    try:
        cursor = connection.cursor()
        
        with open(csv_file_path, 'r') as f:
            # skip header
            next(f)
            
            lines = []
            for line in f:
                parts = line.strip().split(';')
                
                if len(parts) != 6 or not parts[4].strip(): continue

                parts[4] = convert_date(parts[4])
                
                lines.append(parts)
            
            cursor.executemany(
                "INSERT INTO rz_traffic (measuring_point, location, approach, direction, date_time, traffic_count) VALUES (%s, %s, %s, %s, %s, %s)",
                lines
            )
        
        connection.commit()
        print(f"Data from {csv_file_path} loaded successfully.")

    except Exception as e:
        print(f"Error loading data from {csv_file_path}: {e}")
        connection.rollback()

def process_csv_files(connection):
    
    if connection:
        source_dir = './data/source/'
        for filename in os.listdir(source_dir):
            if filename.endswith('.csv'):
                file_path = os.path.join(source_dir, filename)
                print(f"Processing file: {file_path}")
                copy_data_from_csv(connection, file_path)
        connection.close()
        print("All data processed.")
    else:
        print("Failed to connect to database.")


def main():

    connection = connect_to_db()
    
    if connection:

        create_table(connection)
        process_csv_files(connection)
        
        connection.close()

if __name__ == "__main__":
    main()
