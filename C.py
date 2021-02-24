from confluent_kafka import Consumer, KafkaError, KafkaException
import sys

conf = {
    "bootstrap.servers": "0.0.0.0:9092,0.0.0.0:9092",
    "group.id": "foo",
    "auto.offset.reset": "smallest",
    "enable.auto.commit": True,
}

consumer = Consumer(conf)

try:
    consumer.subscribe(["test2"])
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
            message = msg.value()


finally:
    # Close down consumer to commit final offsets.
    consumer.close()
