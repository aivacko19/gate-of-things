
import os
import time
import logging
from urllib import parse
from base64 import b64decode
import nacl.encoding
import nacl.signing
import nacl.exceptions

import amqp_helper

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

class Service(amqp_helper.AmqpAgent):

    def __init__(self, queue, db):
        self.db = db
        amqp_helper.AmqpAgent.__init__(self, queue)
        self.actions = {
            'authenticate': self.authenticate,
            'get_devices': self.get_devices,
            'get_devices_by_name': self.get_devices_by_name,
            'delete_device': self.delete_device,
            'change_key': self.change_key,
            'disable_device': disable_device,
            'enable_device': enable_device,
            'get': get,
            'add': add,}

    # Authenticate client
    def authenticate(self, request, props):
        device_id = props.correlation_id
        device = self.db.select(device_id)
        if device is None:
            LOGGER.info("No device found")
            return None

        resource_uri = request.get('username')
        if not resource_uri.endswith(device_id):
            LOGGER.info("Wrong resource uri")
            return None
        
        token = request.get('password')
        rawtoken = parse_token(token.decode('utf-8'))
        if rawtoken is None:
            LOGGER.info("Wrong token format")
            return None

        signature = rawtoken.get('sig')
        expiry = rawtoken.get('se')
        policy = rawtoken.get('skn', 'ed25519')
        url_encoded_resource_uri = rawtoken.get('sr')

        if float(expiry) < time.time():
            LOGGER.info("Expired")
            return None

        if parse.quote_plus(resource_uri) != url_encoded_resource_uri:
            LOGGER.info("Wrong url encoded resource format")
            return None

        message = '%s\n%s' % (url_encoded_resource_uri, expiry)
        b64key = device.get('key')
        if b64key is None:
            LOGGER.info("No public key")
            return None

        if policy == 'ed25519':
            try:
                verify_key = nacl.signing.VerifyKey(b64key, encoder=nacl.encoding.URLSafeBase64Encoder)
                verify_key.verify(message.encode('utf-8'), b64decode(signature))
            except nacl.exceptions.BadSignatureError:
                LOGGER.info('Bad Signature')
                return None
        else:
            LOGGER.info("Algorithm not supported")
            return None

        email = device_id + DEVICE_EMAIL_SUFFIX

        return {'command': verify, 'email': email}

    # Get a list of devices by owner
    def get_devices(self, request, props):
        owner = request.get('owner')
        devices = self.db.select_by_owner(owner)
        return {'devices': devices}

    # Get a list of devices by name
    def get_devices_by_name(self, request, props):
        names = request.get('names')
        devices = []
        for name in names:
            device = self.db.select(name)
            devices.append(device)
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
        name = request.get('name')
        owner = request.get('owner')
        key = request.get('key')
        result = self.db.insert(name, owner, key)
        return {'id': result}


















