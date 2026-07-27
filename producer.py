from kafka import KafkaProducer
import pandas as pd
import json
import time

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

orders = pd.read_csv("orders.csv")

for _, row in orders.iterrows():
    message = row.to_dict()

    producer.send("orders", message)

    print(f"Sent: {message}")

    time.sleep(2)

producer.flush()

print("All messages sent successfully.")