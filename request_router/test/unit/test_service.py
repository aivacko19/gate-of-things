import json
import unittest
import queue
import socket
import threading
import time
import os


AMQP_URL = 'AMQP_URL'
QUEUE = 'ROUTER'
OAUTH_SERVICE = 'OAUTH'
SUBSCRIPTION_SERVICE = 'SUBSCRIPTION'
MESSAGE_SERVICE = 'MESSAGE'
DEVICE_SERVICE = 'DEVICE'

os.environ['QUEUE'] = QUEUE
os.environ['OAUTH_SERVICE'] = OAUTH_SERVICE
os.environ['SUBSCRIPTION_SERVICE'] = SUBSCRIPTION_SERVICE
os.environ['MESSAGE_SERVICE'] = MESSAGE_SERVICE
os.environ['DEVICE_SERVICE'] = DEVICE_SERVICE
os.environ['AMQP_URL'] = AMQP_URL

from app import service
from . import dummy_db
from . import dummy_messenger

class TestService(unittest.TestCase):

    def init(self):
        self.db = dummy_db.Database()
        self.messenger = dummy_messenger.Messenger()
        self.service = service.Service('access_control', self.db, self.messenger)

    def test_process(self):
        self.init()
        cid = 'client_id'
        props = Properties(cid)
        self.service.prepare_action(None, props)

class Properties:
    def __init__(self, correlation_id):
        self.correlation_id = correlation_id

if __name__ == '__main__':
    unittest.main()