
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
    'SESSION_STATE_SERVICE': None
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
        ids = db.get_packet_ids(session)

        if command == 'pubrel':
            code = SUCCESS if request.get('id') in ids else PACKET_IDENTIFIER_NOT_FOUND
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
                db.delete_packet_id(props.correlation_id, request.get('id'))
            return

        if command != 'publish': return

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

        subs = db.get_matching_subs(topic)

        if not subs:
            LOGGER.info('%s: No matching subscriptions', props.correlation_id)
            self.reply(request, props, NO_MATCHING_SUBSCRIPTIONS)
        else:
            LOGGER.info('%s: Publishing to topics...', props.correlation_id)
            self.reply(request, props, SUCCESS)

        for sub in subs:
            response = {
                'type': 'publish',
                'topic': sub.get_topic_filter(),
                'qos': min(request.get('qos'), sub.get_max_qos())
                'payload': payload
            }

            if sub.get_no_local() and sub.get_session_id() == session.get_id():
                continue

            response['properties'] = request.get('properties')
            del response['properties']['subscription_id']
            if sub.get_sub_id():
                response['properties']['subscription_id'] = sub.get_sub_id()

            self.publish(
                obj=response,
                queue=SESSION_STATE_SERVICE,
                correlation_id=sub.get_session_id())

    def reply(self, request, props, code):
        qos = request.get('qos')
        if qos == 0: return
        response = {
            'type': 'puback' if qos == 1 else 'pubrec',
            'id': request.get('id')
            'code': code,
            'service': True,
            'commands': {'write': True, 'read': True}}
        self.publish(
            obj=response,
            queue=props.reply_to,
            correlation_id=props.correlation_id)
        if qos == 2 and code == SUCCESS:
            db.add_packet_id(props.correlation_id, request.get('id'))









