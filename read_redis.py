import os
import redis
from dotenv import load_dotenv

load_dotenv()

r = redis.StrictRedis(
    host=os.getenv("REDIS_HOST"),
    port=int(os.getenv("REDIS_PORT")),
    password=os.getenv("REDIS_PASSWORD"),
    ssl=True
)

keys = r.keys("telemetry:*")
print("🔑 Ключі в Redis:")
for key in keys:
    value = r.get(key)
    print(key.decode(), "→", value.decode())
