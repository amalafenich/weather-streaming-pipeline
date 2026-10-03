"""Fetch live weather data from two providers and publish it to Kafka."""

import json
import logging
import os
import signal
import time
from typing import Any

import requests
from dotenv import load_dotenv
from kafka import KafkaProducer


load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
LOGGER = logging.getLogger("weather-producer")

OPENWEATHER_API_KEY = os.getenv("OPENWEATHER_API_KEY")
WEATHERAPI_API_KEY = os.getenv("WEATHERAPI_API_KEY")
CITY = os.getenv("WEATHER_CITY", "Tetouan")
INTERVAL_SECONDS = int(os.getenv("FETCH_INTERVAL_SECONDS", "30"))
KAFKA_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "weather-data")

RUNNING = True


def validate_configuration() -> None:
    missing = [
        name
        for name, value in {
            "OPENWEATHER_API_KEY": OPENWEATHER_API_KEY,
            "WEATHERAPI_API_KEY": WEATHERAPI_API_KEY,
        }.items()
        if not value
    ]
    if missing:
        raise RuntimeError(
            "Missing required environment variable(s): " + ", ".join(missing)
        )


def fetch_weather(session: requests.Session) -> dict[str, Any]:
    openweather_response = session.get(
        "https://api.openweathermap.org/data/2.5/weather",
        params={
            "q": CITY,
            "appid": OPENWEATHER_API_KEY,
            "units": "metric",
        },
        timeout=15,
    )
    openweather_response.raise_for_status()

    weatherapi_response = session.get(
        "https://api.weatherapi.com/v1/current.json",
        params={"key": WEATHERAPI_API_KEY, "q": CITY},
        timeout=15,
    )
    weatherapi_response.raise_for_status()

    openweather = openweather_response.json()
    weatherapi = weatherapi_response.json()

    return {
        "city": CITY,
        "openweather_temperature": float(openweather["main"]["temp"]),
        "openweather_humidity": float(openweather["main"]["humidity"]),
        "weatherapi_temperature": float(weatherapi["current"]["temp_c"]),
        "weatherapi_humidity": float(weatherapi["current"]["humidity"]),
        "timestamp": int(time.time()),
    }


def stop(_signal: int, _frame: object) -> None:
    global RUNNING
    RUNNING = False
    LOGGER.info("Shutdown requested.")


def main() -> None:
    validate_configuration()
    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_SERVERS,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        retries=5,
        acks="all",
    )

    with requests.Session() as session:
        try:
            while RUNNING:
                try:
                    payload = fetch_weather(session)
                    producer.send(KAFKA_TOPIC, value=payload).get(timeout=10)
                    LOGGER.info("Published weather data for %s: %s", CITY, payload)
                except (requests.RequestException, KeyError, ValueError) as error:
                    LOGGER.error("Weather retrieval failed: %s", error)
                except Exception as error:  # Kafka/runtime errors
                    LOGGER.exception("Publishing failed: %s", error)

                if RUNNING:
                    time.sleep(INTERVAL_SECONDS)
        finally:
            producer.flush(timeout=10)
            producer.close(timeout=10)


if __name__ == "__main__":
    main()

