
import os
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
    'MESSAGE_SERVICE': None
}

for key in env:
    service = os.environ.get(key)
    if not service:
        raise Exception('Environment variable %s not defined', key)
    env[key] = service

class PublishService(amqp_helper.AmqpAgent):

    def __init__(self, queue, db):
        self.db = db
        amqp_helper.AmqpAgent.__init__(self, queue)

    def main(self, request, props):
        LOGGER.info('Fetching session with id %s', props.correlation_id)
        session = self.db.get(props.correlation_id)
        command = request.get('type')
        ids = self.db.get_packet_ids(session)

        if command == 'pubrel':
            code = SUCCESS if request.get('id') in ids else PACKET_IDENTIFIER_NOT_FOUND
            if request.get('code') < 0x80:
                response = {
                    'type': 'pubcomp',
                    'id': request.get('id'),
                    'code': code,
                    'service': True,
                    'commands': {'write': True, 'read': True}}
                self.publish(
                    obj=response,
                    queue=props.reply_to,
                    correlation_id=props.correlation_id)
            if code == SUCCESS:
                self.db.delete_packet_id(props.correlation_id, request.get('id'))
            return

        if command != 'publish': return

        if request.get('qos') > 0:
            if request.get('id') in ids:
                LOGGER.info('ERROR: %s: Packet identifier in use', props.correlation_id)
                self.reply(request, props, PACKET_IDENTIFIER_IN_USE)
                return

        topic = request.get('topic')
        email = session.get_email()

        authorized = True
        # Authorize publish

        if not authorized:
            LOGGER.info('ERROR: %s: Not authorized to publish to the topic %s',
                        props.correlation_id, topic)
            self.reply(request, props, NOT_AUTHORIZED)
            return

        payload = request.get('payload')

        payload_format = request.get('properties').get('payload_format_indicator')
        if payload_format == UTF8_FORMAT:
            try:
                payload.decode('utf-8')
            except UnicodeDecodeError as e:
                LOGGER.info('ERROR: %s: Payload format is invalid', props.correlation_id)
                self.reply(request, props, PAYLOAD_FORMAT_INVALID)
                return

        subs = self.db.get_matching_subs(topic)

        if not subs:
            LOGGER.info('%s: No matching subscriptions', props.correlation_id)
            self.reply(request, props, NO_MATCHING_SUBSCRIPTIONS)
        else:
            LOGGER.info('%s: Publishing to subscriptions...', props.correlation_id)
            self.reply(request, props, SUCCESS)

        client_map = {}

        for sub in subs:
            cid = sub.get_session_id()

            if sub.get_no_local() and cid == session.get_id():
                continue

            if client_map.get(cid):
                if sub.get_sub_id():
                    client_map[cid]['properties']['subscription_identifier'].append(sub.get_sub_id())
            else:
                response = request.copy()
                response['topic'] = sub.get_topic_filter()
                response['qos'] = min(request.get('qos'), sub.get_max_qos())
                response['properties']['subscription_identifier'] = list()
                if sub.get_sub_id():
                    response['properties']['subscription_identifier'].append(sub.get_sub_id())
                client_map[cid] = response

        for response in client_map.values():
            self.publish(
                obj=response,
                queue=env['MESSAGE_SERVICE'],
                correlation_id=sub.get_session_id())

    def reply(self, request, props, code):
        qos = request.get('qos')
        if qos == 0: return
        response = {
            'type': 'puback' if qos == 1 else 'pubrec',
            'id': request.get('id'),
            'code': code,
            'service': True,
            'commands': {'write': True, 'read': True}}
        self.publish(
            obj=response,
            queue=props.reply_to,
            correlation_id=props.correlation_id)
        if qos == 2 and code == SUCCESS:
            self.db.add_packet_id(props.correlation_id, request.get('id'))









