"""Спільна конфігурація Kafka та імена топіків.

Параметри підключення можна задати через змінні середовища:
    KAFKA_BOOTSTRAP_SERVERS   (за замовчуванням localhost:9092)
    KAFKA_USERNAME / KAFKA_PASSWORD
    KAFKA_SECURITY_PROTOCOL   (PLAINTEXT або SASL_PLAINTEXT)
    KAFKA_SASL_MECHANISM      (за замовчуванням PLAIN)
    TOPIC_PREFIX              (ідентифікатор у назвах топіків)
"""

import os

kafka_config = {
    "bootstrap_servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092").split(","),
    "username": os.getenv("KAFKA_USERNAME", ""),
    "password": os.getenv("KAFKA_PASSWORD", ""),
    "security_protocol": os.getenv("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT"),
    "sasl_mechanism": os.getenv("KAFKA_SASL_MECHANISM", "PLAIN"),
}

# Ідентифікатор, який додається до імен топіків, щоб вони не дублювалися
MY_NAME = os.getenv("TOPIC_PREFIX", "ruslana_uskova")

BUILDING_SENSORS_TOPIC = f"{MY_NAME}_building_sensors"
ALERTS_TOPIC = f"{MY_NAME}_avg_alerts"

ALERTS_CONDITIONS_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "alerts_conditions.csv")


def connection_kwargs() -> dict:
    """Аргументи підключення для KafkaProducer / KafkaConsumer / KafkaAdminClient."""
    kwargs = {
        "bootstrap_servers": kafka_config["bootstrap_servers"],
        "security_protocol": kafka_config["security_protocol"],
    }
    if kafka_config["security_protocol"].startswith("SASL"):
        kwargs.update(
            sasl_mechanism=kafka_config["sasl_mechanism"],
            sasl_plain_username=kafka_config["username"],
            sasl_plain_password=kafka_config["password"],
        )
    return kwargs


def spark_kafka_options() -> dict:
    """Ті самі параметри підключення у форматі опцій Spark Kafka source/sink."""
    options = {
        "kafka.bootstrap.servers": ",".join(kafka_config["bootstrap_servers"]),
        "kafka.security.protocol": kafka_config["security_protocol"],
    }
    if kafka_config["security_protocol"].startswith("SASL"):
        options.update(
            {
                "kafka.sasl.mechanism": kafka_config["sasl_mechanism"],
                "kafka.sasl.jaas.config": (
                    "org.apache.kafka.common.security.plain.PlainLoginModule required "
                    f'username="{kafka_config["username"]}" password="{kafka_config["password"]}";'
                ),
            }
        )
    return options
