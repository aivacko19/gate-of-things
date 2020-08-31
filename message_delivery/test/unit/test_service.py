import json
import unittest
import queue
import socket
import threading
import time
import os

ROUTER = 'ROUTER'
AMQP_URL = 'AMQP_URL'

os.environ['ROUTING_SERVICE'] = ROUTER
os.environ['AMQP_URL'] = AMQP_URL

from app import service
from . import dummy_db
from . import dummy_messenger

class TestService(unittest.TestCase):

    def init(self):
        self.db = dummy_db.Database()
        self.messenger = dummy_messenger.Messenger()
        self.service = service.Service('message_delivery', self.db, self.messenger)

    def test_publish(self):
        self.init()

        cid = 'client_id'
        props = Properties(cid)

        self.db.set_quota(cid, 2)

        delay = 5
        expiry = 60
        qos = 1

        request = get_request(delay, expiry, qos)
        request = self.service._publish(request, props)
        self.assertIsNotNone(request)
        self.assertTrue(request.get('properties').get('message_expiry_interval') <= expiry - delay)
        db_request, time_received = self.db.get(cid, request.get('id'))
        self.assertIsNotNone(db_request)

        delay = 20
        expiry = 10
        qos = 2

        wos = None

        request = get_request(delay, expiry, qos)
        request = self.service._publish(request, props)
        self.assertIsNone(request)

        delay = 5
        expiry = 60
        qos = 2

        request = get_request(delay, expiry, qos)
        request = self.service._publish(request, props)
        self.assertIsNotNone(request)
        self.assertTrue(request.get('properties').get('message_expiry_interval') <= expiry - delay)
        db_request, time_received = self.db.get(cid, request.get('id'))
        self.assertIsNotNone(db_request)

        delay = 5
        expiry = 60
        qos = 2

        request = get_request(delay, expiry, qos)
        request = self.service._publish(request, props)
        self.assertIsNone(request)


class Properties:
    def __init__(self, correlation_id):
        self.correlation_id = correlation_id

def get_request(delay, expiry, qos):
    return {'time_received': int((time.time() - delay)*10**4),
            'qos': 1,
            'properties': {'message_expiry_interval': expiry},
            'payload': 'My Message'}

if __name__ == '__main__':
    unittest.main()