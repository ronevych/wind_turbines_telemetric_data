import redis
import os
from dotenv import load_dotenv

load_dotenv()

REDIS_HOST = os.getenv("REDIS_HOST")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6380))
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD")

print("📡 Підключення до Redis...")

try:
    r = redis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        password=REDIS_PASSWORD,
        ssl=True 
    )

    r.set("test_key", "test_value")
    value = r.get("test_key")
    print("✅ Підключення успішне! test_key =", value.decode())

except Exception as e:
    print("❌ Помилка підключення або запису в Redis:", e)
