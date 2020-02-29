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

def receive(connection, queue_name, response_queue):

    channel = connection.channel()
    channel.queue_declare(queue=queue_name)

    def callback(ch, method, properties, body):
        response = #decode body 
        queue_name.put(response)

    channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=True)
    channel.start_consuming()
