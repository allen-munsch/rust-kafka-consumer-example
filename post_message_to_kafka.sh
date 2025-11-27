#!/bin/bash
TOPIC=${KAFKA_TOPIC:-test-topic}

docker exec -i kafka \
  /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 \
  --topic "$TOPIC" <<EOF
Hello from script!
EOF
