#!/usr/bin/env python3
import os
import queue

import abstract_service

env = {
    'ROUTING_SERVICE': None
}

for key in env:
    service = os.environ.get(key)
    if not service:
        raise Exception('Environment variable %s not defined', key)
    env[key] = service

class InboxService(abstract_service.AbstractService):

    def __init__(self):
        self.inbox = queue.Queue()
        abstract_service.AbstractService.__init__(self)
        self.actions = {
            'receive_package': self.receive_package}
        
    def receive_package(self, request, props):
        state = request['state']
        del request['state']
        self.inbox.put((request, int(props.correlation_id), state))

    def get(self):
        if self.inbox.empty():
            return None, None, None
        return self.inbox.get()

    def publish_request(self, pakcet, file_descriptor):
        packet['command'] = 'process'
        self.publish(obj=packet,
                     queue=env['ROUTING_SERVICE'],
                     correlation_id=str(file_descriptor))

    def publish_disconnect(self, file_descriptor):
        self.publish(obj={'command': 'disconnect'},
                     queue=env['ROUTING_SERVICE'],
                     correlation_id=str(file_descriptor))
