
import os
import time
import logging
from urllib import parse
from base64 import b64decode
import nacl.encoding
import nacl.signing
import nacl.exceptions

import abstract_service

LOGGER = logging.getLogger(__name__)

SUCCESS = 0x00
NO_MATCHING_SUBSCRIPTIONS = 0x10
NO_SUBSCRIPTION_EXISTED = 0x11
UNSPECIFIED_ERROR = 0x80
IMPLEMENTATION_SPECIFIC_ERROR = 0x83
NOT_AUTHORIZED = 0x87
TOPIC_FILTER_INVALID = 0x8F
PACKET_IDENTIFIER_IN_USE = 0x91
PACKET_IDENTIFIER_NOT_FOUND = 0x93
QUOTA_EXCEEDED = 0x97
PAYLOAD_FORMAT_INVALID = 0x99
SHARED_SUBSCRIPTION_NOT_AVAILABLE = 0x9E
SUBSCRIPTION_IDENTIFIERS_NOT_AVAILABLE = 0xA1
WILDCARD_SUBSCRIPTIONS_NOT_AVAILABLE = 0xA2

UTF8_FORMAT = 1

DEVICE_PREFIX = 'device/'
DEVICE_EMAIL_SUFFIX = '@device'
SHARED_ACCESS_SIGNATURE = 'SharedAccessSignature'
TOKEN_FIELDS = ['sig', 'se', 'skn', 'sr']

def parse_token(token_string):
    LOGGER.info(token_string)
    token = token_string.split()
    LOGGER.info(token)
    if token[0] != SHARED_ACCESS_SIGNATURE or len(token) != 2:
        return None

    rawtoken = {}
    for token_attr in token[1].split('&'):
        LOGGER.info(token_attr)
        token_attr_spl = token_attr.split('=', 1)
        LOGGER.info(token_attr_spl)
        if len(token_attr_spl) != 2:
            return None

        key = token_attr_spl[0]
        value = token_attr_spl[1]

        if key in rawtoken or key not in TOKEN_FIELDS:
            return None

        rawtoken[key] = value

    return rawtoken

class Service(abstract_service.AbstractService):

    def __init__(self, queue, db, dummy_messenger=None):
        self.db = db
        self.dummy_messenger = dummy_messenger
        abstract_service.AbstractService.__init__(self, queue)
        self.actions = {'authenticate': self.authenticate,
                        'get_devices': self.get_devices,
                        'get_devices_by_name': self.get_devices_by_name,
                        'delete_device': self.delete_device,
                        'change_key': self.change_key,
                        'disable_device': self.disable_device,
                        'enable_device': self.enable_device,
                        'get': self.get,
                        'add': self.add,}

    # Authenticate client
    def authenticate(self, request, props):
        device_id = props.correlation_id
        resource_uri = request.get('resource_uri')
        token = request.get('token')
        device = self.db.select(device_id)

        response = {'command': 'verify', 'email': None}
        if device is None:
            LOGGER.info("No device found")
            return response

        b64_public_key = device.get('key')
        if b64_public_key is None:
            LOGGER.info("No public key")
            return response

        if not resource_uri.endswith(device_id):
            LOGGER.info("Wrong resource uri")
            return response
        
        rawtoken = parse_token(token.decode('utf-8'))
        if rawtoken is None:
            LOGGER.info("Wrong token format")
            return response

        # Extracting fields from token           // service.py - Device Registry
        signature = rawtoken.get('sig')
        expiry = rawtoken.get('se')
        policy = rawtoken.get('skn')
        url_encoded_resource_uri = rawtoken.get('sr')

        # Check token fields for validity
        if float(expiry) < time.time():
            LOGGER.info("Expired")
            return response

        if parse.quote_plus(resource_uri) != url_encoded_resource_uri:
            LOGGER.info("Wrong url encoded resource format")
            return response

        # Message that is signed by private key
        message = url_encoded_resource_uri + '\n' + expiry

        # Ed25519 algorithm
        if policy == 'ed25519':
            try:
                # Exctract public key from Base64 encoding for Ed25519 algorithm
                public_key = nacl.signing.VerifyKey(b64_public_key, 
                    encoder=nacl.encoding.URLSafeBase64Encoder)
                # Verify signature using transformed public key and message
                public_key.verify(message.encode('utf-8'), b64decode(signature))
            except nacl.exceptions.BadSignatureError:
                LOGGER.info('Bad Signature')
                return response
        else:
            LOGGER.info("Algorithm not supported")
            return response

        response['email'] = device_id + DEVICE_EMAIL_SUFFIX
        return response

    # Get a list of devices by owner
    def get_devices(self, request, props):
        owner = request.get('owner')
        devices = self.db.select_by_owner(owner)
        return {'devices': devices}

    # Get a list of devices by name
    def get_devices_by_name(self, request, props):
        names = request.get('names')
        devices = {}
        for name in names:
            device = self.db.select(name)
            if device:
                devices[device['name']] = device
        return {'devices': devices}

    # Delete a device
    def delete_device(self, request, props):
        name = request.get('name')
        result = self.db.delete(name)
        return {'result': result}

    # Change public key for a device
    def change_key(self, request, props):
        name = request.get('name')
        key = request.get('key')
        result = self.db.update_key(name, key)
        return {'result': result}

    # Disable device
    def disable_device(self, request, props):
        name = request.get('name')
        result = self.db.update_disabled(name, True)
        return {'result': result}

    # Enable device
    def enable_device(self, request, props):
        name = request.get('name')
        result = self.db.update_disabled(name, False)
        return {'result': result}

    # Get device by name
    def get(self, request, props):
        name = request.get('name')
        device = self.db.select(name)
        return {'device': device}

    # Add a new device to the registry
    def add(self, request, props):
        device = request.get('device')
        name = device.get('name')
        owner = device.get('owner', 'owner')
        key = device.get('key')
        result = self.db.insert(name, owner, key)
        return {'id': result}


















