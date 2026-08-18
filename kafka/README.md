# Kafka Configuration

## Topic

The Wikimedia recent-change events are published to:

`wikimedia.recentchange`

## Topic Configuration

- Partitions: 3
- Replication factor: 1
- Bootstrap server inside Docker: `kafka:9092`

## Partitions

The topic contains exactly three partitions:

- Partition 0
- Partition 1
- Partition 2

## Create Topic

From the project root:

```powershell
.\kafka\create-topic.ps1