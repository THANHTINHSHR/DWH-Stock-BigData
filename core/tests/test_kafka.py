from confluent_kafka import Producer

# Cấu hình Kafka Producer
producer_conf = {"bootstrap.servers": "localhost:9093"}  # Địa chỉ Kafka Broker

producer = Producer(producer_conf)


# Callback khi gửi thành công hoặc lỗi
def delivery_report(err, msg):
    if err:
        print(f"❌ Message failed: {err}")
    else:
        print(f"✅ Message delivered to {msg.topic()} [{msg.partition()}]")


# Gửi message
topic = "test_topic"
message = "Hello Kafka!"

producer.produce(topic, key="key1", value=message, callback=delivery_report)
producer.flush()  # Đảm bảo tất cả message được gửi đi
