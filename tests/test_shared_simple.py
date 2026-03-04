#!/usr/bin/env python3
"""
Simple test for Shared subscription mode
"""

import pulsar
import time


def test_simple_shared():
    """Test basic Shared mode functionality"""
    client = pulsar.Client("pulsar://localhost:6650")

    topic = "test-simple-shared"
    subscription = "test-sub"

    print("Creating consumer...")
    consumer = client.subscribe(
        topic,
        subscription,
        consumer_name="consumer-1",
        consumer_type=pulsar.ConsumerType.Shared,
        initial_position=pulsar.InitialPosition.Earliest
    )

    print("Creating producer...")
    producer = client.create_producer(topic)

    print("Sending message...")
    msg_id = producer.send(b"hello-world")
    print(f"✅ Sent message: {msg_id}")

    print("Waiting for message (timeout=5s)...")
    try:
        msg = consumer.receive(timeout_millis=5000)
        print(f"✅ Received message: {msg.data()}")
        consumer.acknowledge(msg)
        print("✅ Test passed!")
    except Exception as e:
        print(f"❌ Failed to receive message: {e}")
        import traceback
        traceback.print_exc()

    consumer.close()
    producer.close()
    client.close()


if __name__ == "__main__":
    print("=" * 60)
    print("Simple Shared Mode Test")
    print("=" * 60)
    test_simple_shared()
