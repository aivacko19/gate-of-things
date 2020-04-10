#!/usr/bin/env python3

import os
import logging

import db
import service 

LOGGER = logging.getLogger(__name__)
LOG_FORMAT = ('%(levelname) -10s %(asctime)s %(name)s %(funcName)s %(lineno)d: %(message)s')
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)

env = {
    'DEVICE_SERVICE': None,
}

for key in env:
    myservice = os.environ.get(key)
    if not myservice:
        raise Exception('Environment variable %s not defined', key)
    env[key] = myservice

DB_NAME = os.environ.get('DB_NAME', 'mydb')
DB_USER = os.environ.get('DB_USER', 'root')
DB_PASS = os.environ.get('DB_PASS', 'root')
DB_HOST = os.environ.get('DB_HOST', '192.168.99.100')
mydb = db.SessionDB(DB_NAME, DB_USER, DB_PASS, DB_HOST)

service_agent = service.DeviceService(env['DEVICE_SERVICE'], mydb)
service_agent.start()

try:
    service_agent.join()
except KeyboardInterrupt:
    LOGGER.error('Caught Keyboard Interrupt, exiting...')
    service_agent.close()
    service_agent.join()