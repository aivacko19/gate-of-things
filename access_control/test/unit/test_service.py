import json
import unittest
import queue
import socket
import threading
import time
import os

LOGGER = 'LOGGER'
AMQP_URL = 'AMQP_URL'

os.environ['LOGGER_SERVICE'] = LOGGER
os.environ['AMQP_URL'] = AMQP_URL

from app import service
from . import dummy_db
from . import dummy_messenger

class TestService(unittest.TestCase):

    def init(self):
        self.db = dummy_db.Database()
        self.messenger = dummy_messenger.Messenger()
        self.service = service.Service('access_control', self.db, self.messenger)

    def test_add_policy(self):
        self.init()

        resource = 'resource'
        user = 'user'
        policy = {'read': True, 'write': True, 'own': False, 'access_time': 20}
        self.service.add_policy(get_request(resource, user, policy), None)
        self.assertIsNone(self.messenger.get(LOGGER))
        self.assertTrue(bool(self.db.get_resource(resource)))
        self.assertEqual(self.db.get_resource(resource).get(user), policy)

        policy['own'] = True
        self.service.add_policy(get_request(resource, user, policy), None)
        logger_message = self.messenger.get(LOGGER)
        self.assertIsNotNone(logger_message)
        self.assertEqual(logger_message.get('request'), 
            {'command': 'add_ownership', 'owner': user, 'resource': resource})
        self.assertEqual(self.db.get_resource(resource).get(user), policy)

        policy['own'] = False
        self.service.add_policy(get_request(resource, user, policy), None)
        logger_message = self.messenger.get(LOGGER)
        self.assertIsNotNone(logger_message)
        self.assertEqual(logger_message.get('request'), 
            {'command': 'remove_ownership', 'owner': user, 'resource': resource})
        self.assertEqual(self.db.get_resource(resource).get(user), policy)



    def test_delete_resource(self):
        self.init()

        resource = 'resource'
        user1 = 'user1'
        user2 = 'user2'
        user3 = 'user3'
        policy = {'read': True, 'write': True, 'own': False, 'access_time': 20}
        self.db.add(user1, resource, True, True, True, 20)
        self.db.add(user2, resource, False, True, False, 0)
        self.db.add(user3, resource, True, False, True, 13)
        self.service.delete_resource({'resource': resource}, None)
        self.assertEqual(self.db.get_resource(resource), {})
        logger_message = self.messenger.get(LOGGER)
        self.assertIsNotNone(logger_message)
        self.assertIsNotNone(logger_message.get('request'))
        self.assertEqual(logger_message.get('request'), 
            {'command': 'remove_ownership', 'owner': user1, 'resource': resource})
        logger_message = self.messenger.get(LOGGER)
        self.assertIsNotNone(logger_message)
        self.assertIsNotNone(logger_message.get('request'))
        self.assertEqual(logger_message.get('request'), 
            {'command': 'remove_ownership', 'owner': user3, 'resource': resource})
        logger_message = self.messenger.get(LOGGER)
        self.assertIsNone(logger_message)



        



def get_request(resource, user, policy):
    return {'resource': resource, 
            'user': user, 
            'read': policy.get('read'),
            'write': policy.get('write'),
            'own': policy.get('own'),
            'access_time': policy.get('access_time'),}


if __name__ == '__main__':
    unittest.main()