import json
import logging
import os
import time

import requests
from kafka import KafkaProducer
from kafka.errors import KafkaError


# ---------------------------------------------------------
# Logging
# ---------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

logger = logging.getLogger("wikimedia-producer")


# ---------------------------------------------------------
# Environment variables
# ---------------------------------------------------------

KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "kafka:9092",
)

KAFKA_TOPIC = os.getenv(
    "KAFKA_TOPIC",
    "wikimedia.recentchange",
)

WIKIMEDIA_SSE_URL = os.getenv(
    "WIKIMEDIA_SSE_URL",
    "https://stream.wikimedia.org/v2/stream/recentchange",
)


# ---------------------------------------------------------
# Kafka Producer
# ---------------------------------------------------------

def create_kafka_producer():
    logger.info(
        "Connecting to Kafka: %s",
        KAFKA_BOOTSTRAP_SERVERS,
    )

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda value: json.dumps(
            value,
            ensure_ascii=False,
        ).encode("utf-8"),
        acks="all",
        retries=10,
        linger_ms=10,
    )

    logger.info("Kafka producer created successfully")

    return producer


# ---------------------------------------------------------
# SSE Event Parser
# ---------------------------------------------------------

def parse_sse_event(response):

    event_data = []

    for raw_line in response.iter_lines(
        decode_unicode=True
    ):

        if raw_line is None:
            continue

        line = raw_line.strip()

        # Empty line means the current SSE event is complete
        if line == "":
            if event_data:
                yield "\n".join(event_data)
                event_data = []

            continue

        # Wikimedia sends JSON in data: lines
        if line.startswith("data:"):
            data = line[5:].strip()

            if data:
                event_data.append(data)

    # Handle a final event if the connection closes
    if event_data:
        yield "\n".join(event_data)


# ---------------------------------------------------------
# Wikimedia SSE Consumer
# ---------------------------------------------------------

def consume_wikimedia_stream(producer):

    logger.info(
        "Connecting to Wikimedia SSE stream: %s",
        WIKIMEDIA_SSE_URL,
    )

    headers = {
        "Accept": "text/event-stream",
        "Cache-Control": "no-cache",
        "User-Agent": "wikimedia-realtime-pipeline/1.0",
    }

    with requests.get(
        WIKIMEDIA_SSE_URL,
        headers=headers,
        stream=True,
        timeout=(30, 120),
    ) as response:

        response.raise_for_status()

        logger.info(
            "Connected to Wikimedia SSE stream"
        )

        for event_data in parse_sse_event(response):

            try:

                event = json.loads(event_data)

            except json.JSONDecodeError:
                logger.warning(
                    "Received invalid JSON event"
                )
                continue

            # -------------------------------------------------
            # Requirement:
            # Only process events where type == "edit"
            # -------------------------------------------------

            if event.get("type") != "edit":
                continue

            wiki = event.get("wiki", "unknown")

            namespace = event.get(
                "namespace",
                "unknown",
            )

            logger.info(
                "Edit event received | wiki=%s | namespace=%s",
                wiki,
                namespace,
            )

            # -------------------------------------------------
            # Publish the ORIGINAL JSON event
            # -------------------------------------------------

            future = producer.send(
                KAFKA_TOPIC,
                value=event,
            )

            try:

                metadata = future.get(
                    timeout=30
                )

                logger.info(
                    "Published edit event | topic=%s | partition=%s | offset=%s",
                    metadata.topic,
                    metadata.partition,
                    metadata.offset,
                )

            except KafkaError as error:

                logger.error(
                    "Kafka publish failed: %s",
                    error,
                )

                raise


# ---------------------------------------------------------
# Main application with automatic reconnection
# ---------------------------------------------------------

def main():

    reconnect_delay = 5

    producer = None

    while True:

        try:

            if producer is None:
                producer = create_kafka_producer()

            consume_wikimedia_stream(producer)

            logger.warning(
                "Wikimedia SSE connection closed."
            )

        except requests.RequestException as error:

            logger.error(
                "Wikimedia SSE connection error: %s",
                error,
            )

        except KafkaError as error:

            logger.error(
                "Kafka error: %s",
                error,
            )

            try:
                producer.close()
            except Exception:
                pass

            producer = None

        except Exception as error:

            logger.exception(
                "Unexpected producer error: %s",
                error,
            )

        finally:

            logger.info(
                "Reconnecting in %s seconds...",
                reconnect_delay,
            )

            time.sleep(reconnect_delay)


if __name__ == "__main__":
    main()