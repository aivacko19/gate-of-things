#!/usr/bin/env python3

import sys
import os
import pika
import time
import logging

import mailer

NUM_OF_ATTEMPTS = 10
WAIT_TIME = 5
RABBITMQ_HOSTNAME = os.environ.get('RABBITMQ_HOSTNAME')
if not RABBITMQ_HOSTNAME:
    sys.exit(1)
QUEUE_NAME = os.environ.get('QUEUE_NAME')
if not QUEUE_NAME:
    sys.exit(1)

connection = None
for i in range(NUM_OF_ATTEMPTS):
    logging.info("Trying to connect to RabbitMQ")
    try:
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOSTNAME))
    except pika.exceptions.AMQPConnectionError:
        logging.info(f"Connection attempt number {i} failed")
        time.sleep(WAIT_TIME)
    if connection:
        logging.error("Exceeded number of attempts, exiting...")
        break
if not connection:
    sys.exit(1)

channel = connection.channel()
channel.queue_declare(queue=QUEUE_NAME)
channel.basic_consume(queue=QUEUE_NAME, on_message_callback=mailer.receive, auto_ack=True)
logging.info('Connection established. Ready to receive messages.')
channel.start_consuming()