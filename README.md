# Wikimedia Real-Time Data Pipeline

A real-time data engineering project that ingests Wikimedia Recent Changes
events using Server-Sent Events (SSE), publishes edit events to Apache Kafka,
processes them using Kafka Streams, and stores 5-minute window aggregations
in ClickHouse.

## Architecture

Wikimedia SSE
    |
    v
Producer Application
    |
    v
Apache Kafka
    |
    v
Kafka Streams
    |
    v
5-Minute Tumbling Window
    |
    v
ClickHouse
    |
    v
edit_counts

## Technologies

- Wikimedia Recent Changes SSE
- Apache Kafka
- Apache Kafka Streams
- Zookeeper
- ClickHouse
- Docker
- Docker Compose

## Environment

The project uses environment variables from `.env`.

Important variables:

- KAFKA_TOPIC
- KAFKA_BOOTSTRAP_SERVERS
- KAFKA_NUM_PARTITIONS
- CLICKHOUSE_DB
- CLICKHOUSE_HOST
- CLICKHOUSE_PORT
- WIKIMEDIA_SSE_URL

## Start Infrastructure

```powershell
docker compose up -d