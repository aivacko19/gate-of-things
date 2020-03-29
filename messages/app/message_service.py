
import os
import time
import logging

import amqp_helper

LOGGER = logging.getLogger(__name__)

SUCCESS = 0x00
NO_MATCHING_SUBSCRIPTIONS = 0x10
NOT_AUTHORIZED = 0x87
PACKET_IDENTIFIER_IN_USE = 0x91
PACKET_IDENTIFIER_NOT_FOUND = 0x93
PAYLOAD_FORMAT_INVALID = 0x99

UTF8_FORMAT = 1

env = {
    'ROUTING_SERVICE': None
}

for key in env:
    service = os.environ.get(key)
    if not service:
        raise Exception('Environment variable %s not defined', key)
    env[key] = service

class MessageService(amqp_helper.AmqpAgent):

    def __init__(self, queue, db):
        self.db = db
        amqp_helper.AmqpAgent.__init__(self, queue)

    def main(self, request, props):
        command = request.get('type')

        if command == 'publish':
            self.publish(request, props)
        elif command == 'puback':
            self.puback(request, props)
        elif command == 'pubrec':
            self.pubrec(request, props)
        elif command == 'pubcomp':
            self.puback(request, props)
        elif command == 'connect':
            self.connect(request, props)
        else:
            LOGGER.error('Unknown command received')

    def publish(self, request, props):
        response = request.copy()

        received = float(response.get('received'))/10**4
        del response['received']
        time_diff = time.time() - received
        message_expiry = response.get('properties').get('message_expiry_interval')
        if time_diff >= message_expiry:
            return

        if response.get('qos') > 0:
            response['id'] = db.add(props.correlation_id, response)
            if response['id'] <= 0:
                return

        response['properties']['message_expiry_interval'] = int(message_expiry - time_diff)
        response['service'] = True
        response['commands'] = {'write': True, 'read': True}

        self.publish(
            obj=response,
            queue=env['ROUTING_SERVICE'],
            correlation_id=props.correlation_id)

    def puback(self, request, props):
        pid = request.get('id')
        message = db.get(props.correlation_id, pid)

        if not message:
            # The MQTT Protocol does not define how to reply in case of wrong packet id
            return

        db.delete(props.correlation_id, pid)

    def pubrec(self, request, props):
        pid = request.get('id')
        message = db.get(props.correlation_id, pid)
        code = SUCCESS

        if not message:
            code = PACKET_IDENTIFIER_NOT_FOUND
        else:
            if request.get('code') < 0x80:
                db.set_received(props.correlation_id, pid)
            else:
                db.delete(props.correlation_id, pid)

        if request.get('code') < 0x80:
            response = {
                    'type': 'pubrel',
                    'id': pid,
                    'code': code,
                    'service': True,
                    'commands': {
                        'write': True,
                        'read': True}}
            self.publish(
                obj=response,
                queue=env['ROUTING_SERVICE'],
                correlation_id=props.correlation_id)

    def connect(self, request, props):
        receive_max = request.get('properties').get('receive_maximum', 65535)
        db.set_quota(props.correlation_id, receive_max)