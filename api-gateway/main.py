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

from gateway import Gateway
import requester
import receiver
from protocols.mqtt.protocol import Protocol

RABBITMQ_HOSTNAME = 'localhost'
MY_HOSTNAME = 'localhost'
RECEIVER_NAME = 'gateway_receive'
SERVICE_NAME = 'connection_service'

# with pika.BlockingConnection(pika.ConnectionParameters(RABBITMQ_HOSTNAME)) as connection:
    
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

gateway = Gateway(MY_HOSTNAME, Protocol(), request_queue, receive_queue)
gateway.start_listening()
gateway.close()


# if len(sys.argv) != 3:
#     print("usage:", sys.argv[0], "<host> <port>")
#     sys.exit(1)

# host, port = sys.argv[1], int(sys.argv[2])