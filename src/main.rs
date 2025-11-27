use aws_sdk_dynamodb::{Client, types::AttributeValue};
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::config::ClientConfig;
use rdkafka::Message;
use tokio::time::{sleep, Duration};

#[tokio::main]
async fn main() {
    let kafka_broker = std::env::var("KAFKA_BROKER").unwrap_or("kafka:9092".into());
    let kafka_topic = std::env::var("KAFKA_TOPIC").unwrap_or("test-topic".into());
    let dynamodb_endpoint =
        std::env::var("DYNAMODB_ENDPOINT").unwrap_or("http://dynamodb:8000".into());
    let table = std::env::var("DYNAMODB_TABLE").unwrap_or("KafkaMessages".into());

    // Kafka Consumer
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", &kafka_broker)
        .set("group.id", "rust-consumer-group")
        .set("enable.partition.eof", "false")
        .set("session.timeout.ms", "6000")
        .set("enable.auto.commit", "true")
        .create()
        .expect("consumer creation failed");

    consumer
        .subscribe(&[&kafka_topic])
        .expect("subscription failed");

    // DynamoDB client
    let config = aws_config::from_env()
        .endpoint_url(dynamodb_endpoint)
        .load()
        .await;
    let dynamodb = Client::new(&config);

    // Make table if not exists
    ensure_table(&dynamodb, &table).await;

    println!("Rust Kafka consumer started.");

    loop {
        if let Some(result) = consumer.recv().await {
            match result {
                Ok(m) => {
                    if let Some(payload) = m.payload_view::<str>() {
                        if let Ok(text) = payload {
                            println!("Received: {}", text);

                            // Write to DynamoDB
                            let pk = uuid::Uuid::new_v4().to_string();
                            let _ = dynamodb
                                .put_item()
                                .table_name(&table)
                                .item("id", AttributeValue::S(pk))
                                .item("message", AttributeValue::S(text.into()))
                                .send()
                                .await;
                        }
                    }
                }
                Err(e) => eprintln!("Kafka error: {e:?}"),
            }
        }

        sleep(Duration::from_secs(1)).await;
    }
}

async fn ensure_table(client: &Client, table: &str) {
    let tables = client.list_tables().send().await.unwrap();
    if tables.table_names().unwrap_or_default().contains(&table.to_string()) {
        return;
    }

    println!("Creating DynamoDB table: {}", table);

    client
        .create_table()
        .table_name(table)
        .attribute_definitions(
            aws_sdk_dynamodb::types::AttributeDefinition::builder()
                .attribute_name("id")
                .attribute_type(aws_sdk_dynamodb::types::ScalarAttributeType::S)
                .build(),
        )
        .key_schema(
            aws_sdk_dynamodb::types::KeySchemaElement::builder()
                .attribute_name("id")
                .key_type(aws_sdk_dynamodb::types::KeyType::Hash)
                .build(),
        )
        .provisioned_throughput(
            aws_sdk_dynamodb::types::ProvisionedThroughput::builder()
                .read_capacity_units(5)
                .write_capacity_units(5)
                .build(),
        )
        .send()
        .await
        .unwrap();
}
