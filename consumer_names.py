from kafka import KafkaConsumer

consumer = KafkaConsumer(
    'names',
    bootstrap_servers=['localhost:9092']
)

for message in consumer:
    print(message)
