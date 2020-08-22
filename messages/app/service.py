
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

class Service(amqp_helper.AmqpAgent):

    def __init__(self, queue, db):
        self.db = db
        amqp_helper.AmqpAgent.__init__(self, queue)
        self.actions = {
            'publish': self._publish,
            'puback': self.puback,
            'pubrec': self.pubrec,
            'pubcomp': self.pubcomp,
            'connect': self._connect,}

    # Process message, save it if QoS > 0 and send it
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
        request['command'] = 'forward'

        self.publish(
            obj=request,
            queue=env['ROUTING_SERVICE'],
            correlation_id=props.correlation_id)

    # Process Publish Acknowledge package (QoS 1)
    def puback(self, request, props):
        pid = request.get('id')
        message, time_received = self.db.get(props.correlation_id, pid)

        if not message:
            # The MQTT Protocol does not define how to reply in case of wrong packet id
            return

        self.db.delete(props.correlation_id, pid)

    # Process Publish Received package (QoS 2)
    def pubrec(self, request, props):
        cid = props.correlation_id
        pid = request.get('id')
        message, time_received = self.db.get(cid, pid)
        
        if request.get('code') >= 0x80:
            self.db.delete(cid, pid)
            return None

        if message:
            self.db.set_received(cid, pid)

        return {
            'command': 'forward',
            'type': 'pubrel',
            'id': pid,
            'code': SUCCESS if message else PACKET_IDENTIFIER_NOT_FOUND,}

    # Process Publish Complete package (QoS 2)
    def pubcomp(self, request, props):
        cid = props.correlation_id
        pid = request.get('id')
        message, time_received = self.db.get(cid, pid)

        if not message:
            # The MQTT Protocol does not define how to reply in case of wrong packet id
            return

        self.db.delete(cid, pid)

    def _connect(self, request, props):
        cid = props.correlation_id
        receive_max = request.get('properties').get('receive_maximum', 65535)
        self.db.set_quota(cid, receive_max)