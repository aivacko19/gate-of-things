import sys
import os
import logging

from request_uri_server import RequestUriRpcServer
from ssl_listener import app
import auth_publisher


logging.basicConfig(level=logging.INFO)

RABBITMQ = os.environ.get('RABBITMQ', "localhost")
if not RABBITMQ:
    sys.exit(1)
QUEUE = os.environ.get('QUEUE', "auth-service")
if not QUEUE:
    sys.exit(1)
REDIRECT_URI = os.environ.get("REDIRECT_URI", "https://127.0.0.1:5000/")
HOST = os.environ.get("HOST", "localhost")
RECEPIENT = os.environ.get('RECEPIENT')
if not RECEPIENT:
    sys.exit(1)

auth_publisher.AuthenticationPublisher.initInstance(RABBITMQ, RECEPIENT)
uri_server = RequestUriRpcServer(RABBITMQ, QUEUE, REDIRECT_URI)
uri_server.run()
app.run(host=HOST, ssl_context="adhoc")