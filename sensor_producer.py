"""Крок 1. Генерація потоку даних датчика.

Кожен запуск скрипту — один датчик зі сталим у межах запуску ID.
Щоб імітувати декілька датчиків, запустіть скрипт декілька разів.

Значення змінюються «випадковим блуканням» навколо поточного рівня, тому
середні за хвилину поступово проходять через різні діапазони
(холодно / нормально / спекотно, сухо / нормально / волого) і спрацьовують
різні алерти з alerts_conditions.csv.
"""

import random
import time
import uuid
from datetime import datetime, timezone

from kafka import KafkaProducer

from configs import BUILDING_SENSORS_TOPIC, connection_kwargs
from serializers import JsonSerializer, StrSerializer

SEND_INTERVAL_SEC = 2

TEMPERATURE_RANGE = (15.0, 60.0)
HUMIDITY_RANGE = (10.0, 95.0)

SENSOR_ID = random.randint(1000, 9999)


def drift(value: float, step: float, bounds: tuple[float, float]) -> float:
    low, high = bounds
    return min(high, max(low, value + random.uniform(-step, step)))


def main() -> None:
    producer = KafkaProducer(
        **connection_kwargs(),
        value_serializer=JsonSerializer(),
        key_serializer=StrSerializer(),
    )
    temperature = random.uniform(*TEMPERATURE_RANGE)
    humidity = random.uniform(*HUMIDITY_RANGE)
    print(f"Sensor {SENSOR_ID} started. Sending to '{BUILDING_SENSORS_TOPIC}' (Ctrl+C to stop)")

    try:
        while True:
            temperature = drift(temperature, 3, TEMPERATURE_RANGE)
            humidity = drift(humidity, 5, HUMIDITY_RANGE)
            data = {
                "id": SENSOR_ID,
                "temperature": round(temperature, 2),
                "humidity": round(humidity, 2),
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }
            producer.send(BUILDING_SENSORS_TOPIC, key=str(uuid.uuid4()), value=data)
            producer.flush()
            print(f"Sent: {data}")
            time.sleep(SEND_INTERVAL_SEC)
    except KeyboardInterrupt:
        print("\nSensor stopped.")
    finally:
        producer.close()


if __name__ == "__main__":
    main()
