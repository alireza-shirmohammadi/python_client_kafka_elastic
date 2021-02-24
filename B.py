from confluent_kafka import Consumer, KafkaError, KafkaException
import sys
from confluent_kafka import Producer
import socket

conf = {
    "bootstrap.servers": "0.0.0.0:9092,0.0.0.0:9092",
    "group.id": "foo",
    "auto.offset.reset": "smallest",
    "enable.auto.commit": True,
}

consumer = Consumer(conf)
msg_count = 0
try:
    consumer.subscribe(["test"])
    while True:

        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue

        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                # End of partition event
                sys.stderr.write(
                    "%% %s [%d] reached end at offset %d\n"
                    % (msg.topic(), msg.partition(), msg.offset())
                )
            elif msg.error():
                raise KafkaException(msg.error())
        else:
            print(msg.value())
            message = int(msg.value())
            conf_p = {
                "bootstrap.servers": "0.0.0.0:9092,0.0.0.0:9092",
                "client.id": socket.gethostname(),
            }
            producer = Producer(conf_p)
            message *= 2
            b = str(message)
            producer.produce("test2", value=b)

            producer.flush()


finally:
    # Close down consumer to commit final offsets.
    consumer.close()

# msg = consumer.poll(timeout=1.0)

# print (msg.value())


import socket

# conf = {'bootstrap.servers': "0.0.0.0:9092,0.0.0.0:9092",
#        'client.id': socket.gethostname()}
