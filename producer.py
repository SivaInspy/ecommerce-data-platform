import json
import time

from kafka import KafkaProducer


KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "orders"


producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda value: json.dumps(value).encode("utf-8")
)


orders = [

    {
        "order_id": 1,
        "customer_id": 101,
        "customer_name": "Arun",
        "city": "Chennai",
        "product_id": "P001",
        "product_name": "iPhone 16",
        "category": "Mobile",
        "price": 79999,
        "payment_method": "UPI",
        "order_time": "2026-09-05 22:55:00"
    },

    {
        "order_id": 2,
        "customer_id": 102,
        "customer_name": "Priya",
        "city": "Bangalore",
        "product_id": "P002",
        "product_name": "MacBook Air",
        "category": "Laptop",
        "price": 115000,
        "payment_method": "Credit Card",
        "order_time": "2026-09-05 22:56:00"
    },

    {
        "order_id": 3,
        "customer_id": 103,
        "customer_name": "Rahul",
        "city": "Mumbai",
        "product_id": "P003",
        "product_name": "Samsung TV",
        "category": "Electronics",
        "price": 65000,
        "payment_method": "UPI",
        "order_time": "2026-09-05 22:57:00"
    },

    {
        "order_id": 4,
        "customer_id": 104,
        "customer_name": "Meena",
        "city": "Delhi",
        "product_id": "P004",
        "product_name": "AirPods Pro",
        "category": "Accessories",
        "price": 24999,
        "payment_method": "Debit Card",
        "order_time": "2026-09-05 22:58:00"
    },

    {
        "order_id": 5,
        "customer_id": 105,
        "customer_name": "Vikram",
        "city": "Hyderabad",
        "product_id": "P005",
        "product_name": "Sony Camera",
        "category": "Camera",
        "price": 55000,
        "payment_method": "Credit Card",
        "order_time": "2026-09-05 22:59:00"
    }

]


for order in orders:

    producer.send(
        KAFKA_TOPIC,
        value=order
    )

    print(f"Sent: {order}")

    time.sleep(2)


producer.flush()

print("All messages sent successfully.")