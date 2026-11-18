"""Створення вхідного (дані датчиків) та вихідного (алерти) топіків у Kafka."""

from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

from configs import ALERTS_TOPIC, BUILDING_SENSORS_TOPIC, MY_NAME, connection_kwargs

NUM_PARTITIONS = 2
REPLICATION_FACTOR = 1


def main() -> None:
    admin_client = KafkaAdminClient(**connection_kwargs())
    try:
        for name in (BUILDING_SENSORS_TOPIC, ALERTS_TOPIC):
            topic = NewTopic(name=name, num_partitions=NUM_PARTITIONS, replication_factor=REPLICATION_FACTOR)
            try:
                admin_client.create_topics(new_topics=[topic], validate_only=False)
                print(f"Topic '{name}' created successfully.")
            except TopicAlreadyExistsError:
                print(f"Topic '{name}' already exists.")

        print("\nMy topics:")
        for topic in sorted(admin_client.list_topics()):
            if MY_NAME in topic:
                print(f"  {topic}")
    finally:
        admin_client.close()


if __name__ == "__main__":
    main()
