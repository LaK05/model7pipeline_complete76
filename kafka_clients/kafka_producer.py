import json, random, time, argparse
from kafka import KafkaProducer
from datetime import datetime

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--broker", default="host.docker.internal:9092")
    parser.add_argument("--topic", default="sensor-data")
    args = parser.parse_args()

    producer = KafkaProducer(
        bootstrap_servers=args.broker,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

    print(f"Connected to Kafka broker at {args.broker}")

    while True:
        record = {
            "sensor_id": random.randint(1, 10),
            "temperature": round(random.uniform(20.0, 40.0), 2),
            "humidity": round(random.uniform(30.0, 90.0), 2),
            "timestamp": datetime.now().isoformat()
        }
        producer.send(args.topic, value=record)
        print("Sent:", record)
        time.sleep(2)

if __name__ == "__main__":
    main()
