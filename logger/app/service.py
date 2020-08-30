
import os
import time
import logging
import urllib
import base64
import nacl.encoding
import nacl.signing
import nacl.exceptions

import abstract_service

LOGGER = logging.getLogger(__name__)

class Service(abstract_service.AbstractService):

    def __init__(self, queue, db, dummy_messenger=None):
        self.db = db
        self.dummy_messenger = dummy_messenger
        abstract_service.AbstractService.__init__(self, queue)
        self.actions = {'log': self.log,
                        'add_ownership': self.add_ownership,
                        'remove_ownership': self.remove_ownership,}

    # Log an access attempt
    def log(self, request, props):
        user = request.get('user')
        resource = request.get('resource')
        action = request.get('action')
        success_bool = request.get('success', True)
        success = 'successfull' if success_bool else 'unsuccessful'
        self.db.insert(user, resource, action, success)

    # Add log access
    def add_ownership(self, request, props):
        owner = request.get('owner')
        resource = request.get('resource')
        self.db.add_ownership(owner, resource)

    # Remove log access
    def remove_ownership(self, request, props):
        owner = request.get('owner')
        resource = request.get('resource')
        self.db.remove_ownership(owner, resource)



















