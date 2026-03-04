#!/usr/bin/env python3
"""
Test Pulsar binary protocol server with actual Pulsar client
"""

import pulsar
import time
import sys

def test_producer():
    """Test producer functionality with binary protocol server"""

    print("Connecting to Pulsar Lite binary server...")
    client = pulsar.Client("pulsar://localhost:6650")

    try:
        print("Creating producer...")
        producer = client.create_producer("persistent://public/default/test-topic")

        print("Sending messages...")
        for i in range(5):
            message = f"Message {i}".encode('utf-8')
            msg_id = producer.send(message)
            print(f"Sent message {i}: {msg_id}")
            time.sleep(0.1)

        print("\nAll messages sent successfully!")
        return True

    except Exception as e:
        print(f"\nError: {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        client.close()

if __name__ == "__main__":
    success = test_producer()
    sys.exit(0 if success else 1)
