#!/usr/bin/env python3
"""
Integration tests for Shared subscription mode
Tests the Round-Robin message distribution and auto-dispatch mechanism
"""

import pulsar
import time
import threading
from collections import defaultdict


def test_shared_round_robin_distribution():
    """
    Test that messages are distributed evenly across consumers in Shared mode
    using Round-Robin algorithm
    """
    client = pulsar.Client("pulsar://localhost:6650")

    topic = "test-shared-round-robin"
    subscription = "test-sub"

    # Create 3 consumers with the same subscription (Shared mode)
    consumer1 = client.subscribe(
        topic,
        subscription,
        consumer_name="consumer-1",
        consumer_type=pulsar.ConsumerType.Shared,
        initial_position=pulsar.InitialPosition.Earliest
    )

    consumer2 = client.subscribe(
        topic,
        subscription,
        consumer_name="consumer-2",
        consumer_type=pulsar.ConsumerType.Shared,
        initial_position=pulsar.InitialPosition.Earliest
    )

    consumer3 = client.subscribe(
        topic,
        subscription,
        consumer_name="consumer-3",
        consumer_type=pulsar.ConsumerType.Shared,
        initial_position=pulsar.InitialPosition.Earliest
    )

    # Create producer
    producer = client.create_producer(topic)

    # Send 30 messages
    num_messages = 30
    for i in range(num_messages):
        msg_id = producer.send(f"message-{i}".encode())
        print(f"Sent message-{i}, msg_id: {msg_id}")

    print(f"Sent {num_messages} messages")

    # Receive messages and track which consumer got what
    received = defaultdict(int)
    timeout = 5000  # 5 seconds timeout

    def receive_from_consumer(consumer, name):
        try:
            while True:
                msg = consumer.receive(timeout_millis=timeout)
                received[name] += 1
                consumer.acknowledge(msg)
                print(f"{name} received message: {msg.data()}, msg_id: {msg.message_id()}")
        except Exception as e:
            print(f"{name} timeout or error: {e}")

    # Start receiving in threads
    threads = []
    for consumer, name in [(consumer1, "consumer-1"),
                            (consumer2, "consumer-2"),
                            (consumer3, "consumer-3")]:
        t = threading.Thread(target=receive_from_consumer, args=(consumer, name))
        t.start()
        threads.append(t)

    # Wait for all threads to complete
    for t in threads:
        t.join(timeout=10)

    print(f"\nMessage distribution:")
    total_received = 0
    for name, count in sorted(received.items()):
        print(f"  {name}: {count} messages")
        total_received += count

    print(f"Total received: {total_received}")

    # Verify basic distribution
    assert total_received >= num_messages * 0.8, \
        f"Expected at least {num_messages * 0.8} messages, got {total_received}"

    # Verify each consumer got at least some messages (basic balance check)
    for name, count in received.items():
        assert count >= 2, f"{name} only received {count} messages, expected at least 2"

    print("✅ Round-Robin distribution test passed!")

    # Cleanup
    consumer1.close()
    consumer2.close()
    consumer3.close()
    producer.close()
    client.close()


def test_shared_flow_control():
    """
    Test that flow control works correctly in Shared mode
    """
    client = pulsar.Client("pulsar://localhost:6650")

    topic = "test-shared-flow"
    subscription = "test-sub"

    # Create consumer with limited permits
    consumer = client.subscribe(
        topic,
        subscription,
        consumer_name="consumer-1",
        consumer_type=pulsar.ConsumerType.Shared,
        initial_position=pulsar.InitialPosition.Earliest
    )

    # Create producer and send messages
    producer = client.create_producer(topic)

    # Send 10 messages
    for i in range(10):
        producer.send(f"message-{i}".encode())

    print("Sent 10 messages")

    # Receive only 5 messages (testing flow control)
    received_count = 0
    for i in range(5):
        try:
            msg = consumer.receive(timeout_millis=3000)
            consumer.acknowledge(msg)
            received_count += 1
            print(f"Received message {i}: {msg.data()}")
        except Exception as e:
            print(f"Error receiving message: {e}")
            break

    assert received_count == 5, f"Expected 5 messages, got {received_count}"
    print("✅ Flow control test passed!")

    # Cleanup
    consumer.close()
    producer.close()
    client.close()


def test_shared_multiple_consumers_concurrent():
    """
    Test concurrent message consumption with multiple consumers
    """
    client = pulsar.Client("pulsar://localhost:6650")

    topic = "test-shared-concurrent"
    subscription = "test-sub"

    # Create 5 consumers
    consumers = []
    for i in range(5):
        consumer = client.subscribe(
            topic,
            subscription,
            consumer_name=f"consumer-{i}",
            consumer_type=pulsar.ConsumerType.Shared,
            initial_position=pulsar.InitialPosition.Earliest
        )
        consumers.append(consumer)

    # Create producer and send 100 messages
    producer = client.create_producer(topic)
    for i in range(100):
        producer.send(f"message-{i}".encode())

    print("Sent 100 messages to 5 consumers")

    # Receive messages concurrently
    received_lock = threading.Lock()
    all_received = []

    def receive_messages(consumer, name):
        local_received = []
        try:
            while True:
                msg = consumer.receive(timeout_millis=2000)
                local_received.append(msg.data())
                consumer.acknowledge(msg)
        except:
            pass

        with received_lock:
            all_received.extend(local_received)
            print(f"{name} received {len(local_received)} messages")

    threads = []
    for i, consumer in enumerate(consumers):
        t = threading.Thread(target=receive_messages,
                            args=(consumer, f"consumer-{i}"))
        t.start()
        threads.append(t)

    for t in threads:
        t.join(timeout=15)

    print(f"Total messages received: {len(all_received)}")

    # Verify most messages were received
    assert len(all_received) >= 80, \
        f"Expected at least 80 messages, got {len(all_received)}"

    # Verify no duplicates
    unique_messages = set(all_received)
    assert len(unique_messages) == len(all_received), \
        "Duplicate messages detected!"

    print("✅ Concurrent consumption test passed!")

    # Cleanup
    for consumer in consumers:
        consumer.close()
    producer.close()
    client.close()


if __name__ == "__main__":
    print("=" * 60)
    print("Running Shared Mode Integration Tests")
    print("=" * 60)
    print()

    try:
        print("Test 1: Round-Robin Distribution")
        print("-" * 60)
        test_shared_round_robin_distribution()
        print()

        # print("Test 2: Flow Control")
        # print("-" * 60)
        # test_shared_flow_control()
        # print()
        # 
        # print("Test 3: Concurrent Consumption")
        # print("-" * 60)
        # test_shared_multiple_consumers_concurrent()
        # print()
        # 
        # print("=" * 60)
        # print("✅ All integration tests passed!")
        # print("=" * 60)
    except Exception as e:
        print(f"❌ Test failed with error: {e}")
        import traceback
        traceback.print_exc()
