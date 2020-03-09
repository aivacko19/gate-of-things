#!/usr/bin/env python3

import sys
import time
import pika
import logging
import json

import bytescoder

def receive(hostname, queue_name, response_queue):
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
    channel.queue_declare(queue=queue_name)

    def callback(ch, method, properties, body):
        packet = json.loads(body.decode('utf-8'), object_hook=bytescoder.as_bytes)
        response_queue.put(packet)

    channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=True)
    channel.start_consuming()
