#!/usr/bin/env python3

import os
import logging

from db import Database
from service import Service

LOGGER = logging.getLogger(__name__)
LOG_FORMAT = ('%(levelname) -10s %(asctime)s %(name)s %(funcName)s %(lineno)d: %(message)s')
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)

key = 'QUEUE'
queue = os.environ.get(key, None)
if not queue:
    raise Exception('Environment variable %s not defined', key)

key = 'DSN'
dsn = os.environ.get(key, None)
if not dsn:
    raise Exception('Environment variable %s not defined', key)

db = Database(dsn)
service = Service(queue, db)
service.start()

try:
    service.join()
except KeyboardInterrupt:
    LOGGER.error('Caught Keyboard Interrupt, exiting...')
    service.close()
    service.join()