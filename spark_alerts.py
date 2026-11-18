"""Кроки 2–5. Spark Structured Streaming: агрегація та алерти.

1. Читає потік з топіку building_sensors.
2. Sliding window 1 хв, крок 30 с, watermark 10 с -> середні t_avg, h_avg.
3. Зчитує критерії алертів з alerts_conditions.csv
   (-999 означає, що межа для цього алерту не використовується).
4. Cross join агрегатів з умовами + фільтрація.
5. Записує алерти у вихідний Kafka-топік.
"""

import glob
import os

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

from configs import ALERTS_CONDITIONS_PATH, ALERTS_TOPIC, BUILDING_SENSORS_TOPIC, MY_NAME, spark_kafka_options

WINDOW_DURATION = "1 minute"
SLIDING_INTERVAL = "30 seconds"
WATERMARK_DURATION = "10 seconds"
UNUSED = -999

CHECKPOINT_DIR = os.getenv("CHECKPOINT_DIR", "/tmp/checkpoints/" + MY_NAME + "_avg_alerts")

# Конектор Kafka має відповідати версіям Spark і Scala
_spark_version = pyspark.__version__
_scala_version = "2.13" if int(_spark_version.split(".")[0]) >= 4 else "2.12"
KAFKA_PACKAGE = f"org.apache.spark:spark-sql-kafka-0-10_{_scala_version}:{_spark_version}"

# Якщо jars/ заповнено скриптом download_jars.py — інтернет для Spark не потрібен
JARS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "jars")
LOCAL_JARS = sorted(glob.glob(os.path.join(JARS_DIR, "*.jar")))

SENSOR_SCHEMA = StructType(
    [
        StructField("id", IntegerType()),
        StructField("temperature", DoubleType()),
        StructField("humidity", DoubleType()),
        StructField("timestamp", StringType()),
    ]
)


def build_spark() -> SparkSession:
    builder = SparkSession.builder.appName("SensorAvgAlerts").master(os.getenv("SPARK_MASTER", "local[*]"))
    if LOCAL_JARS:
        print(f"Using local Kafka connector jars from {JARS_DIR}")
        builder = builder.config("spark.jars", ",".join(LOCAL_JARS))
    else:
        builder = builder.config("spark.jars.packages", KAFKA_PACKAGE).config(
            "spark.jars.ivy", os.getenv("SPARK_IVY_DIR", "/tmp/.ivy2")
        )
    spark = (
        builder.config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def read_sensor_stream(spark: SparkSession):
    raw = (
        spark.readStream.format("kafka")
        .options(**spark_kafka_options())
        .option("subscribe", BUILDING_SENSORS_TOPIC)
        .option("startingOffsets", "latest")
        .option("maxOffsetsPerTrigger", "500")
        .option("failOnDataLoss", "false")
        .load()
    )
    return (
        raw.select(F.from_json(F.col("value").cast("string"), SENSOR_SCHEMA).alias("data"))
        .select("data.*")
        .withColumn("timestamp", F.col("timestamp").cast("timestamp"))
        .dropna(subset=["timestamp", "temperature", "humidity"])
    )


def aggregate(sensors):
    return (
        sensors.withWatermark("timestamp", WATERMARK_DURATION)
        .groupBy(F.window("timestamp", WINDOW_DURATION, SLIDING_INTERVAL))
        .agg(
            F.avg("temperature").alias("t_avg"),
            F.avg("humidity").alias("h_avg"),
        )
    )


def read_conditions(spark: SparkSession):
    return spark.read.csv(ALERTS_CONDITIONS_PATH, header=True, inferSchema=True)


def in_range(value_col, min_col, max_col):
    """True, якщо межа не використовується (-999) або значення в [min, max]."""
    min_ok = (F.col(min_col) == UNUSED) | (value_col >= F.col(min_col))
    max_ok = (F.col(max_col) == UNUSED) | (value_col <= F.col(max_col))
    return min_ok & max_ok


def detect_alerts(averages, conditions):
    return (
        averages.crossJoin(conditions)
        .where(in_range(F.col("t_avg"), "temperature_min", "temperature_max"))
        .where(in_range(F.col("h_avg"), "humidity_min", "humidity_max"))
        .select(
            "window",
            "t_avg",
            "h_avg",
            F.col("code").cast("string").alias("code"),
            "message",
            F.date_format(F.current_timestamp(), "yyyy-MM-dd HH:mm:ss.SSSSSS").alias("timestamp"),
        )
    )


def main() -> None:
    spark = build_spark()

    averages = aggregate(read_sensor_stream(spark))
    alerts = detect_alerts(averages, read_conditions(spark))

    kafka_out = alerts.select(
        F.expr("uuid()").alias("key"),
        F.to_json(F.struct("window", "t_avg", "h_avg", "code", "message", "timestamp")).alias("value"),
    )

    query = (
        kafka_out.writeStream.trigger(processingTime="10 seconds")
        .outputMode("update")
        .format("kafka")
        .options(**spark_kafka_options())
        .option("topic", ALERTS_TOPIC)
        .option("checkpointLocation", CHECKPOINT_DIR)
        .start()
    )

    # Дублюємо алерти в консоль для наочності
    console = (
        alerts.writeStream.trigger(processingTime="10 seconds")
        .outputMode("update")
        .format("console")
        .option("truncate", "false")
        .start()
    )

    print(f"Streaming '{BUILDING_SENSORS_TOPIC}' -> '{ALERTS_TOPIC}' (Ctrl+C to stop)")
    try:
        spark.streams.awaitAnyTermination()
    except KeyboardInterrupt:
        print("\nStopping...")
    finally:
        query.stop()
        console.stop()
        spark.stop()


if __name__ == "__main__":
    main()
