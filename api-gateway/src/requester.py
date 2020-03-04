#!/usr/bin/env python3

import sys
import socket
import selectors
import traceback
import pika
import queue
import logging
import json
import bytescoder

def request(hostname, queue_name, response_queue_name, request_queue):

    connection = pika.BlockingConnection(pika.ConnectionParameters(hostname))
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



