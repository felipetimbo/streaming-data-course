import time

from kafka import KafkaProducer
from faker import Faker

fake = Faker()

producer = KafkaProducer(bootstrap_servers=['localhost:9092'])
for _ in range(100):
    name = fake.name()
    producer.send('names', name.encode('utf-8'))
    print(name)
    
time.sleep(20)
