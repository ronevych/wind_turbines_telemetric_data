import os
import redis
from dotenv import load_dotenv

load_dotenv()
REDIS_HOST = os.getenv("REDIS_HOST")
REDIS_PORT = int(os.getenv("REDIS_PORT"))
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD")

print("📡 Підключення до Redis...")

try:
    r = redis.StrictRedis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        password=REDIS_PASSWORD,
        ssl=True
    )

    pong = r.ping()
    print("✅ Redis підключення успішне!")

    r.set("test_key", "hello_redis")
    value = r.get("test_key")
    print("📥 Прочитано з Redis: test_key =", value.decode())

except Exception as e:
    print("❌ Помилка підключення до Redis:", e)
