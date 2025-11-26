import psycopg2
import os
from dotenv import load_dotenv

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

def get_unique_locations():
    conn = connect_to_db()
    if not conn:
        raise SystemExit("Brak połączenia z bazą danych.")
    
    cursor = conn.cursor()
    cursor.execute("SELECT DISTINCT location FROM RZ_Traffic WHERE location IS NOT NULL;")
    results = [row[0] for row in cursor.fetchall()]
    
    conn.close()
    return results

if __name__ == "__main__":
    locations = get_unique_locations()
    print("Znalezione lokalizacje w bazie:")
    for loc in locations:
        print(" -", loc)
