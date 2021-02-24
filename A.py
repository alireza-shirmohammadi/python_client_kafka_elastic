from time import sleep


from confluent_kafka import Producer
import socket

conf = {
    "bootstrap.servers": "0.0.0.0:9092,0.0.0.0:9092",
    "client.id": socket.gethostname(),
}
while True:
    producer = Producer(conf)
    for i in range(10000):
        producer.produce("test", value=str(i))
        i += 1
        producer.flush()
        print("yeah")
        sleep(5)
