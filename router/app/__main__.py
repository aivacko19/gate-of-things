#!/usr/bin/env python3

import os
import logging

import db
from routing_service import RoutingService

LOGGER = logging.getLogger(__name__)
LOG_FORMAT = ('%(levelname) -10s %(asctime)s %(name)s %(funcName)s %(lineno)d: %(message)s')
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)

env = {
    'ROUTING_SERVICE': None,
    'VERIFICATION_SERVICE': None
}

for key in env:
    service = os.environ.get(key)
    if not service:
        raise Exception('Environment variable %s not defined', key)
    env[key] = service

DB_NAME = os.environ.get('DB_NAME', 'mydb')
DB_USER = os.environ.get('DB_USER', 'root')
DB_PASS = os.environ.get('DB_PASS', 'root')
DB_HOST = os.environ.get('DB_HOST', '192.168.99.100')
mydb = db.ConnectionDB(DB_NAME, DB_USER, DB_PASS, DB_HOST)

router = RoutingService(env['ROUTING_SERVICE'], mydb)

router.start()

try:
    router.join()
except KeyboardInterrupt:
    LOGGER.error('Caught Keyboard Interrupt, exiting...')
    router.close()
    router.join()