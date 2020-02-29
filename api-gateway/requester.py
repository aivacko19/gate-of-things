#!/usr/bin/env python3

import sys
import socket
import selectors
import traceback
import pika
import queue
import logging
import json

def request(connection, queue_names, request_queue):

    channel = connection.channel()
    for service, queue_name in queue_names:
        channel.queue_declare(queue_name)

    try:
        while True:
            request = request_queue.get()
            service = request['service']
            queue_name = queue_names[service]

            body = #encode request
            channel.basic_publish(exchange='', routing_key=queue_name, body=body)
    except Exception as e:
        logging.



