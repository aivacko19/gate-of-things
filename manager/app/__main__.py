#!/usr/bin/env python3

import os
import logging

from web_app import app as manager_service

LOGGER = logging.getLogger(__name__)
LOG_FORMAT = ('%(levelname) -10s %(asctime)s %(name)s %(funcName)s %(lineno)d: %(message)s')
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)

HOST = os.environ.get("HOST", "localhost")

manager_service.run(host=HOST, ssl_context=('cert.pem', 'key.pem'))