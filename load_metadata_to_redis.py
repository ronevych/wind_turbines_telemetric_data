import os
import redis
import pyodbc
from dotenv import load_dotenv

load_dotenv()

# SQL конфігурація
SQL_SERVER = os.getenv("SQL_SERVER")
SQL_DATABASE = os.getenv("SQL_DATABASE")
SQL_USERNAME = os.getenv("SQL_USERNAME")
SQL_PASSWORD = os.getenv("SQL_PASSWORD")

# Redis конфігурація
REDIS_HOST = os.getenv("REDIS_HOST")
REDIS_PORT = int(os.getenv("REDIS_PORT"))
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD")

# Підключення до Redis
r = redis.StrictRedis(
    host=REDIS_HOST,
    port=REDIS_PORT,
    password=REDIS_PASSWORD,
    ssl=True
)

# Підключення до SQL Server через pyodbc
conn_str = (
    f"DRIVER={{ODBC Driver 18 for SQL Server}};"
    f"SERVER={SQL_SERVER};DATABASE={SQL_DATABASE};"
    f"UID={SQL_USERNAME};PWD={SQL_PASSWORD};"
    f"TrustServerCertificate=yes;"
)


try:
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()

    def load_and_store(query, redis_key):
        cursor.execute(query)
        columns = [col[0] for col in cursor.description]
        rows = [dict(zip(columns, row)) for row in cursor.fetchall()]
        r.set(redis_key, str(rows).replace("'", '"'))
        print(f"💾 Redis key '{redis_key}' loaded with {len(rows)} rows.")

    # Зчитування таблиць
    load_and_store("SELECT * FROM Turbines", "metadata:turbines")
    load_and_store("SELECT * FROM Sensors", "metadata:sensors")
    load_and_store("SELECT * FROM Measurements", "metadata:measurements")

    conn.close()
    print("✅ Метадані успішно збережені в Redis.")

except Exception as e:
    print("❌ Помилка:", e)
