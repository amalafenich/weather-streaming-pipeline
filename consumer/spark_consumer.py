"""Consume Kafka weather events with Spark and write micro-batches to InfluxDB."""

import os
from pathlib import Path

from dotenv import load_dotenv
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import DoubleType, LongType, StringType, StructType


load_dotenv()

KAFKA_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "weather-data")
INFLUXDB_URL = os.getenv("INFLUXDB_URL", "http://localhost:8086")
INFLUXDB_TOKEN = os.getenv("INFLUXDB_TOKEN")
INFLUXDB_ORG = os.getenv("INFLUXDB_ORG", "weather-org")
INFLUXDB_BUCKET = os.getenv("INFLUXDB_BUCKET", "weather-data")

SCHEMA = (
    StructType()
    .add("city", StringType())
    .add("openweather_temperature", DoubleType())
    .add("openweather_humidity", DoubleType())
    .add("weatherapi_temperature", DoubleType())
    .add("weatherapi_humidity", DoubleType())
    .add("timestamp", LongType())
)


def validate_configuration() -> None:
    if not INFLUXDB_TOKEN:
        raise RuntimeError("Missing required environment variable: INFLUXDB_TOKEN")


def write_batch(batch: DataFrame, batch_id: int) -> None:
    rows = batch.collect()
    if not rows:
        return

    points = [
        (
            Point("weather")
            .tag("city", row.city)
            .field("openweather_temperature", row.openweather_temperature)
            .field("openweather_humidity", row.openweather_humidity)
            .field("weatherapi_temperature", row.weatherapi_temperature)
            .field("weatherapi_humidity", row.weatherapi_humidity)
            .time(row.timestamp, WritePrecision.S)
        )
        for row in rows
    ]

    with InfluxDBClient(
        url=INFLUXDB_URL,
        token=INFLUXDB_TOKEN,
        org=INFLUXDB_ORG,
    ) as client:
        client.write_api(write_options=SYNCHRONOUS).write(
            bucket=INFLUXDB_BUCKET,
            org=INFLUXDB_ORG,
            record=points,
        )

    print(f"Batch {batch_id}: wrote {len(points)} point(s) to InfluxDB.")


def main() -> None:
    validate_configuration()

    spark = (
        SparkSession.builder
        .appName("WeatherKafkaToInfluxDB")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    kafka_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .load()
    )

    weather_stream = (
        kafka_stream
        .selectExpr("CAST(value AS STRING) AS json_value")
        .select(from_json(col("json_value"), SCHEMA).alias("data"))
        .select("data.*")
        .filter(col("city").isNotNull())
    )

    checkpoint = str(Path("checkpoints") / "weather-to-influx")

    query = (
        weather_stream.writeStream
        .foreachBatch(write_batch)
        .option("checkpointLocation", checkpoint)
        .outputMode("append")
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()

