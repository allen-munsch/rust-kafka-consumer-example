#!/bin/bash
AWS_ACCESS_KEY_ID=local AWS_SECRET_ACCESS_KEY=local aws dynamodb scan --table-name KafkaMessages --endpoint-url http://localhost:8000 --region us-east-1