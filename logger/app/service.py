
import os
import time
import logging
import urllib
import base64
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
SHARED_ACCESS_SIGNATURE = 'SharedAccessSignature'
TOKEN_FIELDS = ['sig', 'se', 'skn', 'sr']

class Service(amqp_helper.AmqpAgent):

    def __init__(self, queue, db):
        self.db = db
        amqp_helper.AmqpAgent.__init__(self, queue)

    def main(self, request, props):

        command = request.get('command')
        del request['command']

        if command == 'log':

            if ('user' not in request
                or 'resource' not in request
                or 'action' not in request
                or 'owner' not in request):
                return

            user = request.get('user')
            resource = request.get('resource')
            action = request.get('action')
            owner = request.get('owner')

            self.db.insert(user, resource, action, owner)

        elif command == 'grant':

            if ('owner' not in request):
                return

            owner = request.get('owner')
            
            self.db.grant(owner)



















