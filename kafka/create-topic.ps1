$ErrorActionPreference = "Stop"

$KafkaContainer = "wikimedia-kafka"
$BootstrapServer = "kafka:9092"
$Topic = "wikimedia.recentchange"
$Partitions = 3
$ReplicationFactor = 1

Write-Host "Checking Kafka..." -ForegroundColor Cyan

docker exec $KafkaContainer kafka-broker-api-versions `
    --bootstrap-server $BootstrapServer | Out-Null

if ($LASTEXITCODE -ne 0) {
    throw "Kafka is not available."
}

Write-Host "Checking whether topic '$Topic' already exists..." -ForegroundColor Cyan

$topics = docker exec $KafkaContainer kafka-topics `
    --bootstrap-server $BootstrapServer `
    --list

if ($topics -contains $Topic) {
    Write-Host "Topic '$Topic' already exists." -ForegroundColor Yellow
}
else {
    Write-Host "Creating topic '$Topic' with $Partitions partitions..." -ForegroundColor Cyan

    docker exec $KafkaContainer kafka-topics `
        --bootstrap-server $BootstrapServer `
        --create `
        --topic $Topic `
        --partitions $Partitions `
        --replication-factor $ReplicationFactor

    if ($LASTEXITCODE -ne 0) {
        throw "Failed to create Kafka topic."
    }

    Write-Host "Topic created successfully." -ForegroundColor Green
}

Write-Host ""
Write-Host "Topic configuration:" -ForegroundColor Cyan

docker exec $KafkaContainer kafka-topics `
    --bootstrap-server $BootstrapServer `
    --describe `
    --topic $Topic