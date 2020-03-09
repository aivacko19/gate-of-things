#!/usr/bin/env python3

import sys
import pika
import queue
import threading
import logging
import time
import os

import gateway
import requester
import receiver
from protocols import mqtt as protocol


# if len(sys.argv) != 2:
#     print("usage:", sys.argv[0], "<rabbitmq_host>")
#     sys.exit(1)

RABBITMQ_HOSTNAME = os.environ.get('RABBITMQ_HOSTNAME')
if not RABBITMQ_HOSTNAME:
    sys.exit(1)
MY_HOSTNAME = os.getenv('HOST', 'localhost')
RECEIVER_NAME = 'api_gateway_receiver:' + str(int(time.time()*1000))
SERVICE_NAME = 'connection_service'


request_queue = queue.Queue()
receive_queue = queue.Queue()

request_thread = threading.Thread(
    target=requester.request,
    args=(RABBITMQ_HOSTNAME, SERVICE_NAME, RECEIVER_NAME, request_queue,),
    daemon=True)

receive_thread = threading.Thread(
    target=receiver.receive,
    args=(RABBITMQ_HOSTNAME, RECEIVER_NAME, receive_queue,),
    daemon=True)

request_thread.start()
receive_thread.start()

api_gateway = gateway.Gateway(MY_HOSTNAME, protocol, request_queue, receive_queue)
api_gateway.start_listening()
api_gateway.close()
