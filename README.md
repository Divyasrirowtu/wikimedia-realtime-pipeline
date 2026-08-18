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

## Current Implementation Status

### Step 1 — Project Infrastructure

Completed:

- Project structure
- Docker Compose
- Zookeeper
- Kafka
- ClickHouse
- Environment configuration
- Docker volumes
- Health checks

### Step 2 — Kafka Topic

Completed:

- Kafka topic `wikimedia.recentchange`
- Exactly 3 partitions
- Replication factor of 1 for local development
- Topic verification
- Reusable PowerShell topic creation script

### Future Steps

- Wikimedia SSE producer
- Edit event filtering
- Custom `wiki` Kafka partitioner
- ClickHouse `edit_counts` table
- Kafka Streams 5-minute tumbling windows
- ClickHouse persistence
- SSE reconnection handling
- Idempotent ClickHouse writes
- End-to-end testing

### Step 3 — Wikimedia SSE Producer

Completed:

- Wikimedia Recent Changes SSE connection
- SSE event parsing
- JSON parsing
- Filtering for `type == "edit"`
- Publishing original edit-event JSON to Kafka
- Kafka producer configuration
- Producer Docker container
- Environment-based configuration
- Automatic SSE reconnection
- Producer restart verification

Producer source:

`producer/producer.py`

Kafka topic:

`wikimedia.recentchange`