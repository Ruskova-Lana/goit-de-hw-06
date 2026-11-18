"""Читання алертів з вихідного топіку та вивід на екран (для перевірки результату)."""

import json

from kafka import KafkaConsumer

from configs import ALERTS_TOPIC, MY_NAME, connection_kwargs
from serializers import JsonDeserializer, StrDeserializer


def main() -> None:
    consumer = KafkaConsumer(
        ALERTS_TOPIC,
        **connection_kwargs(),
        value_deserializer=JsonDeserializer(),
        key_deserializer=StrDeserializer(),
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id=f"{MY_NAME}_avg_alerts_consumer",
    )
    print(f"Listening to '{ALERTS_TOPIC}' (Ctrl+C to stop)")

    try:
        for msg in consumer:
            print(json.dumps(msg.value, indent=2, ensure_ascii=False))
    except KeyboardInterrupt:
        print("\nConsumer stopped.")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
