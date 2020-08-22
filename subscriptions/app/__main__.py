#!/usr/bin/env python3

import os
import logging
import threading

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

class ExpiryTable(threading.Thread):

    def __init__(self, db):
        self.db = db
        self._lock = threading.Lock()
        self._table = {}
        self._closing = False

    def add_entry(self, key, value):
        with self._lock:
            self._table[key] = value

    def run(self):
        while True:
            if self._closing:
                break
            time.sleep(5)
            with self._lock:
                curr_time = int(time.time())
                for key, value in self._table.items():
                    if value < curr_time:
                        self.db.delete_sub_by_id(key)
                        del sel._table[key]

    def close(self):
        self._closing = True


db = Database(dsn)
expiry_table = ExpiryTable(db)
service = Service(queue, db, expiry_table)
expiry_table.start()
service.start()

try:
    expiry_table.join()
    service.join()
except KeyboardInterrupt:
    LOGGER.error('Caught Keyboard Interrupt, exiting...')
    expiry_table.close()
    service.close()
    expiry_table.join()
    service.join()