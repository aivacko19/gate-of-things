#!/usr/bin/env python3

import sys
import socket
import selectors
import traceback
import pika
import queue
import threading
import logging
import json
import bytescoder

def receive(hostname, queue_name, response_queue):
    connection = pika.BlockingConnection(pika.ConnectionParameters(hostname))
    channel = connection.channel()
    channel.queue_declare(queue=queue_name)

    def callback(ch, method, properties, body):
        packet = json.loads(body.decode('utf-8'), object_hook=bytescoder.as_bytes)
        queue_name.put(packet)

    channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=True)
    channel.start_consuming()
