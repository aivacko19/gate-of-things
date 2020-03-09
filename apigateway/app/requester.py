#!/usr/bin/env python3

import sys
import time
import pika
import logging
import json

import bytescoder

def request(hostname, queue_name, response_queue_name, request_queue):

    connection = None
    for i in range(10):
        try:
            connection = pika.BlockingConnection(pika.ConnectionParameters(host=hostname))
        except pika.exceptions.AMQPConnectionError:
            print(f"Connection attempt number {i} failed")
            time.sleep(5)
        if connection:
            break
    if not connection:
        sys.exit(1)
    channel = connection.channel()
    channel.queue_declare(queue_name)

    try:
        while True:
            packet = request_queue.get()
            packet['response_queue_name'] = response_queue_name
            body = json.dumps(packet, cls=bytescoder.BytesEncoder).encode('utf-8')
            channel.basic_publish(exchange='', routing_key=queue_name, body=body)
    except Exception as e:
        logging.error(f"Exception while executing request loop: {e}")



