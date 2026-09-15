import sys
import uuid

from confluent_kafka import Consumer, KafkaException, Producer
from confluent_kafka.admin import AdminClient

BOOTSTRAP = "localhost:9092"
TOPIC = "smoke_test"
MESSAGES = 9
RUN_ID = uuid.uuid4().hex[:8]

delivered = []


def on_delivery(err, msg):
    if err is not None:
        raise KafkaException(err)
    delivered.append((msg.partition(), msg.offset()))


def produce():
    producer = Producer({"bootstrap.servers": BOOTSTRAP})
    for i in range(MESSAGES):
        producer.produce(
            TOPIC,
            key=f"visitor-{i % 3}".encode(),
            value=f"{RUN_ID}:smoke message {i}".encode(),
            on_delivery=on_delivery,
        )
    producer.flush(30)
    return delivered


def consume(expected):
    consumer = Consumer(
        {
            "bootstrap.servers": BOOTSTRAP,
            "group.id": f"smoke-{RUN_ID}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([TOPIC])
    received = []
    empty_polls = 0
    while len(received) < expected and empty_polls < 20:
        msg = consumer.poll(1.0)
        if msg is None:
            empty_polls += 1
            continue
        if msg.error():
            raise KafkaException(msg.error())
        value = msg.value().decode()
        if not value.startswith(f"{RUN_ID}:"):
            continue
        received.append((msg.key().decode(), value, msg.partition()))
    consumer.close()
    return received


def main():
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP})
    meta = admin.list_topics(timeout=15)
    if TOPIC not in meta.topics:
        print(f"FAIL topic {TOPIC} does not exist")
        return 1
    print(f"run {RUN_ID}: broker reachable, {len(meta.brokers)} broker(s), topic {TOPIC} present")

    acks = produce()
    partitions_written = sorted({p for p, _ in acks})
    print(f"produced {len(acks)} messages across partitions {partitions_written}")

    received = consume(MESSAGES)
    partitions_read = sorted({p for _, _, p in received})
    print(f"consumed {len(received)} messages from this run, partitions {partitions_read}")

    keys_by_partition = {}
    for key, _, partition in received:
        keys_by_partition.setdefault(key, set()).add(partition)
    stable = all(len(v) == 1 for v in keys_by_partition.values())
    print(f"each key landed on exactly one partition: {stable}")

    checks = {
        "all produced messages consumed": len(received) == MESSAGES,
        "key to partition mapping stable": stable,
        "read partitions match written": partitions_read == partitions_written,
    }
    for name, passed in checks.items():
        print(f"  {'ok  ' if passed else 'fail'} {name}")

    if not all(checks.values()):
        print("FAIL")
        return 1
    print("PASS")
    return 0


if __name__ == "__main__":
    sys.exit(main())
