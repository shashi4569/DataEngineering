
from kafka import KafkaProducer
from kafka.errors import KafkaError
import time
import json
producer = KafkaProducer(bootstrap_servers=['quickstart.cloudera:9092'])

topic = "test1"

with open('ddos_data') as f:
    f = f.readlines()
    cnt = 0
    for line in f:
        cnt = cnt + 1
        print(cnt, line)
        fut = producer.send(topic, line.encode('utf-8'))
        #producer = KafkaProducer(value_serializer=lambda v: json.dumps(v).encode('utf-8'))

