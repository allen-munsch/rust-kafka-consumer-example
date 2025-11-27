use aws_sdk_dynamodb::{types::AttributeValue, Client};
use aws_sdk_dynamodb::types::{
    AttributeDefinition, KeySchemaElement, ProvisionedThroughput,
    ScalarAttributeType, KeyType,
};
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::Message;
use tokio::time::{sleep, Duration};
use uuid::Uuid;

#[tokio::main]
async fn main() {
    let kafka_broker = std::env::var("KAFKA_BROKER").unwrap_or("kafka:9092".into());
    let kafka_topic = std::env::var("KAFKA_TOPIC").unwrap_or("test-topic".into());
    let dynamodb_endpoint =
        std::env::var("DYNAMODB_ENDPOINT").unwrap_or("http://dynamodb:8000".into());
    let table = std::env::var("DYNAMODB_TABLE").unwrap_or("KafkaMessages".into());

    // --- Kafka consumer ------------------------------------------------------
    let consumer: StreamConsumer = rdkafka::ClientConfig::new()
        .set("bootstrap.servers", &kafka_broker)
        .set("group.id", "rust-consumer-group")
        .set("auto.offset.reset", "earliest")
        .create()
        .expect("Failed to create Kafka consumer");

    consumer.subscribe(&[&kafka_topic]).expect("Failed to subscribe");

    // --- DynamoDB client -----------------------------------------------------
    let config = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .endpoint_url(dynamodb_endpoint)
        .load()
        .await;

    let dynamodb = Client::new(&config);

    ensure_table(&dynamodb, &table).await;

    println!("Rust Kafka consumer started...");

    // --- Poll loop -----------------------------------------------------------
    loop {
        match consumer.recv().await {
            Ok(msg) => {
                if let Some(Ok(text)) = msg.payload_view::<str>() {
                    println!("Received: {text}");

                    let _ = dynamodb
                        .put_item()
                        .table_name(&table)
                        .item("id", AttributeValue::S(Uuid::new_v4().to_string()))
                        .item("message", AttributeValue::S(text.into()))
                        .send()
                        .await;
                }
            }
            Err(err) => {
                eprintln!("Kafka error: {err}");
            }
        }

        sleep(Duration::from_secs(1)).await;
        println!("Rust Kafka consumer polling.");
    }
}

async fn ensure_table(client: &Client, table: &str) {
    let resp = client.list_tables().send().await.expect("list tables failed");
    let names = resp.table_names.unwrap_or_default();

    if names.contains(&table.to_string()) {
        return;
    }

    println!("Creating DynamoDB table: {table}");

    let attr_def = AttributeDefinition::builder()
        .attribute_name("id")
        .attribute_type(ScalarAttributeType::S)
        .build()
        .expect("attr_def");

    let key_schema = KeySchemaElement::builder()
        .attribute_name("id")
        .key_type(KeyType::Hash)
        .build()
        .expect("key_schema");

    let throughput = ProvisionedThroughput::builder()
        .read_capacity_units(5)
        .write_capacity_units(5)
        .build()
        .expect("throughput");

    client
        .create_table()
        .table_name(table)
        .attribute_definitions(attr_def)
        .key_schema(key_schema)
        .provisioned_throughput(throughput)
        .send()
        .await
        .expect("create table failed");
}
