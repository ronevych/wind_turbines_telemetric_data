import threading
import time
import random
import datetime
import os
import redis
from dotenv import load_dotenv
from azure.iot.device import IoTHubDeviceClient

# Завантаження змінних з .env
load_dotenv()

REDIS_HOST = os.getenv("REDIS_HOST")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6380))
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD")

# Redis клієнт
redis_client = redis.StrictRedis(
    host=REDIS_HOST,
    port=REDIS_PORT,
    password=REDIS_PASSWORD,
    ssl=True
)

# Встановити кількість турбін
LIMIT_TURBINES = 5

def send_telemetry(device_id, connection_string):
    client = IoTHubDeviceClient.create_from_connection_string(connection_string)
    print(f"[{device_id}] 🔗 Підключення...")

    try:
        client.connect()
        last_redis_time = 0

        while True:
            telemetry = {
                "turbine_id": device_id,
                "timestamp": datetime.datetime.utcnow().isoformat() + "Z",
                "wind_speed": round(random.uniform(3.0, 15.0), 2),
            }
            msg = str(telemetry).replace("'", '"')
            print(f"[{device_id}] → Надсилає: {msg}")
            client.send_message(msg)

            # Перевірити, чи минуло 60 сек
            if time.time() - last_redis_time >= 60:
                redis_client.set(device_id, msg)
                print(f"[{device_id}] 💾 Збережено в Redis")
                last_redis_time = time.time()

            time.sleep(15)

    except Exception as e:
        print(f"[{device_id}] ❌ Помилка: {e}")
    finally:
        client.shutdown()


def load_devices(filename="devices.txt"):
    devices = {}
    with open(filename, "r") as f:
        for line in f:
            parts = line.strip().split(";")
            dev_id = [x for x in parts if x.startswith("DeviceId=")][0].split("=")[1]
            devices[dev_id] = line.strip()
    return devices


def main():
    devices = load_devices()
    limited_devices = dict(list(devices.items())[:LIMIT_TURBINES])

    for device_id, conn_str in limited_devices.items():
        thread = threading.Thread(target=send_telemetry, args=(device_id, conn_str), daemon=True)
        thread.start()

    while True:
        time.sleep(60)


if __name__ == "__main__":
    main()
