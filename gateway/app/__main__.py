#!/usr/bin/env python3

import os
import logging
import traceback

import gateway_server
from inbox_service import InboxService
from protocols import mqtt as protocol


# if len(sys.argv) != 2:
#     print("usage:", sys.argv[0], "<rabbitmq_host>")
#     sys.exit(1)

LOGGER = logging.getLogger(__name__)
LOG_FORMAT = ('%(levelname) -10s %(asctime)s %(name)s %(funcName)s %(lineno)d: %(message)s')
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)

MY_HOSTNAME = os.getenv('HOST', 'localhost')

inbox = InboxService()
api_gateway = gateway_server.GatewayServer(MY_HOSTNAME, protocol, inbox)
try:
    inbox.start()
    api_gateway.start()
except Exception as e:
    LOGGER.error(traceback.format_exc())
    api_gateway.close()
    inbox.close()
