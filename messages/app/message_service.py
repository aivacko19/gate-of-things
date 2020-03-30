
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
        LOGGER.info('Client ID: %s, Action: %s', props.correlation_id, command)

        if command == 'publish':
            self._publish(request, props)
        elif command == 'puback':
            self.puback(request, props)
        elif command == 'pubrec':
            self.pubrec(request, props)
        elif command == 'pubcomp':
            self.puback(request, props)
        elif command == 'connect':
            self._connect(request, props)
        else:
            LOGGER.error('Unknown command received')

    def _publish(self, request, props):

        time_received = float(request.get('time_received'))/10**4
        del request['time_received']
        time_diff = time.time() - time_received
        message_expiry = request.get('properties').get('message_expiry_interval')
        if message_expiry:
            if time_diff >= message_expiry:
                return

        if request.get('qos') > 0:
            request['id'] = self.db.add(props.correlation_id, time_received, request)
            if request['id'] <= 0:
                return

        if message_expiry:
            request['properties']['message_expiry_interval'] = int(message_expiry - time_diff)
        request['service'] = True
        request['commands'] = {'write': True, 'read': True}

        self.publish(
            obj=request,
            queue=env['ROUTING_SERVICE'],
            correlation_id=props.correlation_id)

    def puback(self, request, props):
        pid = request.get('id')
        message, time_received = self.db.get(props.correlation_id, pid)

        if not message:
            # The MQTT Protocol does not define how to reply in case of wrong packet id
            return

        self.db.delete(props.correlation_id, pid)

    def pubrec(self, request, props):
        pid = request.get('id')
        message, time_received = self.db.get(props.correlation_id, pid)
        code = SUCCESS

        if not message:
            code = PACKET_IDENTIFIER_NOT_FOUND
        else:
            if request.get('code') < 0x80:
                self.db.set_received(props.correlation_id, pid)
            else:
                self.db.delete(props.correlation_id, pid)

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

    def _connect(self, request, props):
        receive_max = request.get('properties').get('receive_maximum', 65535)
        self.db.set_quota(props.correlation_id, receive_max)