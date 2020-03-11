import sys
import os

from request_uri_server import RequestUriRpcServer
from ssl_listener import app

RABBITMQ = os.environ.get('RABBITMQ', "localhost")
if not RABBITMQ:
    sys.exit(1)
REQUEST_URI = os.environ.get('REQUEST_URI', "auth-service")
if not REQUEST_URI:
    sys.exit(1)
REDIRECT_URI = os.environ.get("REDIRECT_URI", "https://127.0.0.1:5000/")
HOST = os.environ.get("HOST", "localhost")

uri_server = RequestUriRpcServer(RABBITMQ, REQUEST_URI, REDIRECT_URI)
uri_server.run()
app.run(host=HOST, ssl_context="adhoc")