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
from protocols import mqtt as protocol

RABBITMQ_HOSTNAME = 'localhost'
MY_HOSTNAME = 'localhost'
RECEIVER_NAME = 'gateway_receive'
SERVICE_NAME = 'connection_service'

# if len(sys.argv) != 3:
#     print("usage:", sys.argv[0], "<host> <port>")
#     sys.exit(1)

# host, port = sys.argv[1], int(sys.argv[2])

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
