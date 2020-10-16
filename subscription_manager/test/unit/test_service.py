import json
import unittest
import queue
import socket
import threading
import time
import os

LOGGER = 'LOGGER'
MESSAGE_SERVICE = 'MESSAGE'
ACCESS_CONTROL_SERVICE = 'ACCESS_CONTROL'
AMQP_URL = 'AMQP_URL'

os.environ['LOGGER_SERVICE'] = LOGGER
os.environ['MESSAGE_SERVICE'] = MESSAGE_SERVICE
os.environ['ACCESS_CONTROL_SERVICE'] = ACCESS_CONTROL_SERVICE
os.environ['AMQP_URL'] = AMQP_URL


from app import service
from . import dummy_db
from . import dummy_messenger

class TestService(unittest.TestCase):

    def init(self):
        self.db = dummy_db.Database()
        self.messenger = dummy_messenger.Messenger()
        self.service = service.Service('access_control', self.db, self.messenger)


if __name__ == '__main__':
    unittest.main()