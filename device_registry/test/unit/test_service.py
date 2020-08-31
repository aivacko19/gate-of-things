import json
import unittest
import queue
import socket
import threading
import time
import os
from cryptography.hazmat.primitives.asymmetric import ed25519
from urllib import parse
from base64 import b64decode
from base64 import b64encode

AMQP_URL = 'AMQP_URL'

os.environ['AMQP_URL'] = AMQP_URL

from app import service
from . import dummy_db
from . import dummy_messenger

key_pairs = [
    {
        'public': 'tYMB5x0zra0lRqPrm8HNSgcgcjYS3HHC6jnH61bx68E=',
        'private': 'V1LX8FavkzyVU6fpp7lG+2SCu3rxiS+BcVW5zOlPeRS1gwHnHTOtrSVGo+ubwc1KByByNhLcccLqOcfrVvHrwQ==',
    },
    {
        'public': 'KMG0R6af/nLhJ8LWZ8dF7pgN35bN6/pa7/Dyc1bn/6o=',
        'private': 'Xiv3QukWJYnoXy9tMjitILFZoLN+qdvaTuc/T+1GSfYowbRHpp/+cuEnwtZnx0XumA3fls3r+lrv8PJzVuf/qg==',
    },
    {
        'public': 'TvGEH7P4BIT8LxQLpV1O6aOhhwX3u2fSPXflks0ClRU=',
        'private': 'duK4jwSH7UY8+5NGjT+TQjFUqlRy8Oh23ZN+BL3tGHxO8YQfs/gEhPwvFAulXU7po6GHBfe7Z9I9d+WSzQKVFQ==',
    },
]

PUBLIC_KEY = 'WuDgBzOCXYkuaXnhGnHIQsZxAJxkzjQ4LxCGxD8+wKg='
PRIVATE_KEY = 'MnYXkElOsDHKJW5rqGw5otuqfZxBoWSC/ZtzKbl7rMha4OAHM4JdiS5peeEacchCxnEAnGTONDgvEIbEPz7AqA=='

TOKEN_FORMAT = "SharedAccessSignature sig=%s&se=%s&skn=%s&sr=%s"
POLICY = 'ed25519'

class TestService(unittest.TestCase):

    def init(self):
        self.db = dummy_db.Database()
        self.messenger = dummy_messenger.Messenger()
        self.service = service.Service('device_registry', self.db, self.messenger)

    def test_authorize(self):
        self.init()

        device_name = 'my-device'
        props = Properties(device_name)
        resource_uri = 'home_uri.com/devices/' + device_name

        self.db.insert(device_name, 'owner', PUBLIC_KEY)

        token = form_token(resource_uri, PRIVATE_KEY)
        request = {'resource_uri': resource_uri, 'token': token}
        response = self.service.authenticate(request, props)
        self.assertEqual(response.get('command'), 'verify')
        self.assertEqual(response.get('email'), device_name + '@device')

        token = form_token(resource_uri, PRIVATE_KEY, 200)
        request = {'resource_uri': resource_uri, 'token': token}
        response = self.service.authenticate(request, props)
        self.assertEqual(response.get('command'), 'verify')
        self.assertIsNone(response.get('email'))

        for pair in key_pairs:

            self.db.update_key(device_name, pair['public'])

            token = form_token(resource_uri, pair['private'])
            request = {'resource_uri': resource_uri, 'token': token}
            response = self.service.authenticate(request, props)
            self.assertEqual(response.get('command'), 'verify')
            self.assertEqual(response.get('email'), device_name + '@device')


        self.db.update_key(device_name, PUBLIC_KEY)
        response = self.service.authenticate(request, props)
        self.assertEqual(response.get('command'), 'verify')
        self.assertIsNone(response.get('email'))


class Properties:
    def __init__(self, correlation_id):
        self.correlation_id = correlation_id

def form_token(resource_uri, private_key, expiry_delay=0):
    expiry = str(int(time.time()) + 60 - expiry_delay)
    url_encoded_resource_uri = parse.quote_plus(resource_uri)
    message = url_encoded_resource_uri + '\n' + expiry
    message_bin = message.encode('utf-8')
    private_key = ed25519.Ed25519PrivateKey.from_private_bytes(b64decode(private_key)[:32])
    signature_bin = private_key.sign(message_bin)
    signature = b64encode(signature_bin).decode('utf-8')
    return (TOKEN_FORMAT % (signature, expiry, POLICY, url_encoded_resource_uri)).encode('utf-8')

if __name__ == '__main__':
    unittest.main()