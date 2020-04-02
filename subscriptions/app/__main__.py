#!/usr/bin/env python3

import os
import logging

import db
from subscription_service import SubscriptionService

LOGGER = logging.getLogger(__name__)
LOG_FORMAT = ('%(levelname) -10s %(asctime)s %(name)s %(funcName)s %(lineno)d: %(message)s')
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)

env = {
    'SUBSCRIPTION_SERVICE': None,
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
mydb = db.SessionDB(DB_NAME, DB_USER, DB_PASS, DB_HOST)

subscription_service = SubscriptionService(env['SUBSCRIPTION_SERVICE'], mydb)

subscription_service.start()

try:
    subscription_service.join()
except KeyboardInterrupt:
    LOGGER.error('Caught Keyboard Interrupt, exiting...')
    subscription_service.close()
    subscription_service.join()