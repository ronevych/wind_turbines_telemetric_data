import os
import time
import random
import datetime
import threading
from azure.iot.device import IoTHubDeviceClient
import redis
from dotenv import load_dotenv

# Завантаження .env
load_dotenv()

REDIS_HOST = os.getenv("REDIS_HOST")
REDIS_PORT = int(os.getenv("REDIS_PORT"))
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD")

# Redis клієнт
redis_client = redis.StrictRedis(
    host=REDIS_HOST,
    port=REDIS_PORT,
    password=REDIS_PASSWORD,
    ssl=True
)

def generate_new_value(prev_value, min_val, max_val, fluctuation=0.1):
    change = prev_value * random.uniform(-fluctuation, fluctuation)
    new_value = max(min_val, min(max_val, prev_value + change))
    return round(new_value, 2)

def load_devices(filename="devices.txt"):
    devices = {}
    with open(filename, "r") as f:
        for line in f:
            parts = line.strip().split(";")
            dev_id = [x for x in parts if x.startswith("DeviceId=")][0].split("=")[1]
            devices[dev_id] = line.strip()
    return devices

def send_telemetry(device_id, connection_string):
    client = IoTHubDeviceClient.create_from_connection_string(connection_string)
    print(f"[{device_id}] 🔗 Підключення...")

    try:
        client.connect()

        # Початкові значення
        wind_speed = random.uniform(4.0, 8.0)
        wind_direction = random.randint(0, 360)
        temperature = random.uniform(-5.0, 25.0)
        humidity = random.uniform(30.0, 90.0)
        last_redis_time = 0

        while True:
            # Генерація нових значень
            wind_speed = generate_new_value(wind_speed, 3.0, 15.0)
            wind_direction = (wind_direction + random.randint(-5, 5)) % 360
            temperature = generate_new_value(temperature, -10.0, 40.0)
            humidity = generate_new_value(humidity, 20.0, 100.0)

            telemetry = {
                "turbine_id": device_id,
                "timestamp": datetime.datetime.utcnow().isoformat() + "Z",
                "wind_speed": wind_speed,
                "wind_direction": wind_direction,
                "temperature": temperature,
                "humidity": humidity
            }

            msg = str(telemetry).replace("'", '"')
            print(f"[{device_id}] → Надсилає: {msg}")
            client.send_message(msg)

            # Зберігаємо в Redis раз на 60 секунд
            now = time.time()
            if now - last_redis_time >= 60:
                redis_key = f"telemetry:{device_id}"
                try:
                    redis_client.set(redis_key, msg)
                    print(f"[{device_id}] 💾 Збережено в Redis")
                    last_redis_time = now
                except Exception as e:
                    print(f"[{device_id}] ❌ Redis помилка: {e}")

            delay = random.randint(20, 56)
            time.sleep(delay)

    except Exception as e:
        print(f"[{device_id}] ❌ Помилка: {e}")
    finally:
        client.shutdown()

def main():
    devices = load_devices()
    for device_id, conn_str in devices.items():
        thread = threading.Thread(target=send_telemetry, args=(device_id, conn_str), daemon=True)
        thread.start()

    while True:
        time.sleep(60)

if __name__ == "__main__":
    main()
