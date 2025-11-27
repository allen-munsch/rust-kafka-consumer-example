# Kafka Consumer

Rust service that consumes messages from Kafka and stores them in DynamoDB.

## Stack

- Rust + rdkafka + aws-sdk-dynamodb
- Apache Kafka (KRaft mode)
- DynamoDB Local

## Quick Start
```bash
docker compose up --build
```

Wait ~30s for Kafka to initialize.

## Usage

**Send a message:**
```bash
./post_message_to_kafka.sh
```

**Check stored messages:**
```bash
AWS_ACCESS_KEY_ID=local AWS_SECRET_ACCESS_KEY=local \
  aws dynamodb scan --table-name KafkaMessages --endpoint-url http://localhost:8000 --region us-east-1
```

## Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| KAFKA_BROKER | kafka:9092 | Kafka bootstrap server |
| KAFKA_TOPIC | test-topic | Topic to consume |
| DYNAMODB_ENDPOINT | http://dynamodb:8000 | DynamoDB endpoint |
| DYNAMODB_TABLE | KafkaMessages | Target table name |
