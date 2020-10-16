#!/usr/bin/env python3

import os
import logging

from service import Service
from callback import app 

LOGGER = logging.getLogger(__name__)
LOG_FORMAT = ('%(levelname) -10s %(asctime)s %(name)s %(funcName)s %(lineno)d: %(message)s')
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)

key = 'QUEUE'
queue = os.environ.get(key, None)
if not queue:
    raise Exception('Environment variable %s not defined', key)

key = 'REDIRECT_URI'
redirect_uri = os.environ.get(key, None)
if not redirect_uri:
    raise Exception('Environment variable %s not defined', key)

key = 'HOST'
host = os.environ.get(key, 'localhost')

service = Service(queue, redirect_uri)
service.start()
app.run(host=host, ssl_context=('cert.pem', 'key.pem'))

try:
    service.join()
except KeyboardInterrupt:
    LOGGER.error('Caught Keyboard Interrupt, exiting...')
    service.close()
    service.join()