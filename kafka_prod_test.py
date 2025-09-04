from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers="localhost:9092")
producer.send("test-topic", b"Hello Kafka from Python")
producer.send("test-topic", b"Whatsup")
producer.flush()



