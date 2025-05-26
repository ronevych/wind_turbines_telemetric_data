import os
import redis
from dotenv import load_dotenv

load_dotenv()

REDIS_HOST = os.getenv("REDIS_HOST")
REDIS_PORT = int(os.getenv("REDIS_PORT"))
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD")

redis_client = redis.StrictRedis(
    host=REDIS_HOST,
    port=REDIS_PORT,
    password=REDIS_PASSWORD,
    ssl=True
)

device_ids = ["turbine001", "turbine002", "turbine003", "turbine004", "turbine005"]

for device_id in device_ids:
    key = f"telemetry:{device_id}"
    value = redis_client.get(key)
    if value:
        print(f"[{device_id}] 📥 {value.decode()}")
    else:
        print(f"[{device_id}] ❌ Нічого не знайдено")
