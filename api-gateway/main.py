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

import gateway
import requester
import receiver
from properties.mqtt.message import MessageFactory

RABBITMQ_HOSTNAME = 'localhost'
MY_HOSTNAME = 'localhost'
RECEIVER_NAME = 'gateway_receive'
SERVICE_NAMES = {
    'connect': 'connect_service'
}

protocol = MessageFactory()

with pika.BlockingConnection(pika.ConnectionParameters(RABBIT_MQ_HOST)) as connection
    request_queue = queue.Queue()
    receive_queue = queue.Queue()

    request_thread = threading.Thread(
        target=requester.request,
        args=(connection, SERVICE_NAMES, request_queue,),
        daemon=True)

    receive_thread = threading.Thread(
        target=receiver.receive,
        args=(connection, RECEIVER_NAME, receive_queue,),
        daemon=True)

    request_thread.start()
    receive_thread.start()

    gateway.start_listening(MY_HOSTNAME, protocol, request_queue, receive_queue)



# if len(sys.argv) != 3:
#     print("usage:", sys.argv[0], "<host> <port>")
#     sys.exit(1)

# host, port = sys.argv[1], int(sys.argv[2])