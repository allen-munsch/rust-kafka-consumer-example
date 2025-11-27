#!/bin/bash

TOPIC="test-topic"
MESSAGE="Hello from script!"

docker exec -it $(docker ps --filter "ancestor=bitnami/kafka" -q) \
  kafka-console-producer.sh \
  --broker-list kafka:9092 \
  --topic "$TOPIC" <<< "$MESSAGE"

echo "Message sent."
