#!/usr/bin/env python3

import os
import logging

from web_app import app
import web_app
import service
import db

LOGGER = logging.getLogger(__name__)
LOG_FORMAT = ('%(levelname) -10s %(asctime)s %(name)s %(funcName)s %(lineno)d: %(message)s')
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)

HOST = os.environ.get("HOST", "localhost")

env = {
    'MANAGER_SERVICE': None,
}

for key in env:
    env_var = os.environ.get(key)
    if not env_var:
        raise Exception('Environment variable %s not defined', key)
    env[key] = env_var

DB_NAME = os.environ.get('DB_NAME', 'mydb')
DB_USER = os.environ.get('DB_USER', 'root')
DB_PASS = os.environ.get('DB_PASS', 'root')
DB_HOST = os.environ.get('DB_HOST', '192.168.99.100')
mydb = db.DB(DB_NAME, DB_USER, DB_PASS, DB_HOST)

service_agent = service.Service(env['MANAGER_SERVICE'], mydb)
service_agent.start()

web_app.var['mydb'] = mydb
app.run(host=HOST, ssl_context=('cert.pem', 'key.pem'))

try:
    service_agent.join()
except KeyboardInterrupt:
    LOGGER.error('Caught Keyboard Interrupt, exiting...')
    service_agent.close()
    service_agent.join()