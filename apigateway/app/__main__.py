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
import mailer


# if len(sys.argv) != 2:
#     print("usage:", sys.argv[0], "<rabbitmq_host>")
#     sys.exit(1)

RABBITMQ = os.environ.get('RABBITMQ')
if not RABBITMQ:
    sys.exit(1)
MY_HOSTNAME = os.getenv('HOST', 'localhost')
SERVICE_NAME = 'connection_service'

mail = mailer.Mail(RABBITMQ, SERVICE_NAME)
mail.start()

api_gateway = gateway.Gateway(MY_HOSTNAME, protocol, mail)
api_gateway.start_listening()
api_gateway.close()
