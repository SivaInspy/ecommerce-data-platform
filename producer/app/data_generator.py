import random
from faker import Faker
import pandas as pd
from datetime import datetime

fake = Faker("en_IN")

products = pd.read_csv("data/products.csv")


def generate_order():

    product = products.sample(1).iloc[0]

    customer = fake.name()

    city = fake.city()

    payment = random.choice([
        "UPI",
        "Credit Card",
        "Debit Card",
        "Net Banking"
    ])

    return {

        "order_id": fake.uuid4(),

        "customer_name": customer,

        "city": city,

        "product_id": product["product_id"],

        "product_name": product["product_name"],

        "category": product["category"],

        "price": int(product["price"]),

        "payment_method": payment,

        "order_time": datetime.now().isoformat()

    }