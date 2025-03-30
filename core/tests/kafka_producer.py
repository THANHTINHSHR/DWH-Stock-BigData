from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers="localhost:9092")
producer.send("my-topic", b"Hello Apache Kafka!")
producer.flush()
print("✅ Sent message to Kafka!")
