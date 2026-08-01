import time

from config.settings import MESSAGE_INTERVAL_SECONDS
from config.settings import KAFKA_TOPIC

from app.kafka_client import send_message

from app.data_generator import generate_order

from app.logger import logger

def start_producer():

    logger.info("Producer Started")

    while True:

        order = generate_order()

        send_message(KAFKA_TOPIC, order)

        print(order)

        logger.info(order)

        time.sleep(MESSAGE_INTERVAL_SECONDS)