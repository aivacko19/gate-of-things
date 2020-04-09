
import os
import logging

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

env = {
    'MESSAGE_SERVICE': None,
    'ACCESS_CONTROL_SERVICE': None
}

for key in env:
    service = os.environ.get(key)
    if not service:
        raise Exception('Environment variable %s not defined', key)
    env[key] = service

class SubscriptionService(amqp_helper.AmqpAgent):

    def __init__(self, queue, db):
        self.db = db
        amqp_helper.AmqpAgent.__init__(self, queue)

    def main(self, request, props):
        LOGGER.info('Fetching session with id %s', props.correlation_id)
        self.session = self.db.get(props.correlation_id)
        pid_in_use = (self.db.is_pid_in_use(self.session.get_id(), request.get('id')) 
                      if self.session else False)
        command = request.get('command')
        del request['command']
        response = {}

        if command == 'publish':
            code = SUCCESS
            qos = request.get('qos')
            if qos > 0 and pid_in_use:
                LOGGER.info('ERROR: %s: Packet identifier in use', props.correlation_id)
                code = PACKET_IDENTIFIER_IN_USE
            else:
                code = self._publish(request, props)
            if qos > 0:
                response = {
                    'command': 'forward',
                    'type': 'puback' if qos == 1 else 'pubrec',
                    'id': request.get('id'),
                    'code': code,}
                if qos == 2 and code == SUCCESS:
                    self.db.add_packet_id(props.correlation_id, request.get('id'))

        elif command == 'pubrel':
            code = SUCCESS if pid_in_use else PACKET_IDENTIFIER_NOT_FOUND
            if request.get('code') < 0x80:
                response = {
                    'command': 'forward',
                    'type': 'pubcomp',
                    'id': request.get('id'),
                    'code': code,}
            if code == SUCCESS:
                self.db.delete_packet_id(props.correlation_id, request.get('id'))

        elif command in ['subscribe', 'unsubscribe']:
            codes = list()

            for topic in request.get('topics'):
                if pid_in_use:
                    LOGGER.info('ERROR: %s: Packet identifier in use', props.correlation_id)
                    code = PACKET_IDENTIFIER_IN_USE
                elif command == 'subscribe':
                    code = self.subscribe(request, props, topic)
                elif command == 'unsubscribe':
                    code = self.unsubscribe(request, props, topic)

                codes.append({'code': code})

            if codes:
                response = {
                    'command': 'forward',
                    'type': 'suback' if command == 'subscribe' else 'unsuback',
                    'id': request.get('id'),
                    'topics': codes,}

        elif command == 'fetch_session':
            LOGGER.info('Received command get')
            response = {
                'email': self.session.get_email() if self.session else None}

        elif command == 'create_session':
            if self.session:
                self.db.delete(self.session)
            self.db.add(props.correlation_id, request.get('email'))

        elif command == 'reauthenticate':
            email = request.get('email')
            for sub in self.session.get_subs():
                if not self.authorize_subscribe(email, sub.get_topic_filter()):
                    self.db.delete_sub(sub)
            self.session.set_email(email)
            self.db.update(self.session)

        if response:
            self.publish(
                obj=response,
                queue=props.reply_to,
                correlation_id=props.correlation_id)

    def _publish(self, request, props):

        topic = request.get('topic')

        if not self.authorize_publish(self.session.get_email(), topic):
            LOGGER.info('ERROR: %s: Not authorized to publish to the topic %s',
                        props.correlation_id, topic)
            return NOT_AUTHORIZED

        payload = request.get('payload')
        payload_format = request.get('properties').get('payload_format_indicator')
        if payload_format == UTF8_FORMAT:
            try:
                payload.decode('utf-8')
            except UnicodeDecodeError as e:
                LOGGER.info('ERROR: %s: Payload format is invalid', props.correlation_id)
                return PAYLOAD_FORMAT_INVALID

        subs = self.db.get_matching_subs(topic)

        if not subs:
            LOGGER.info('%s: No matching subscriptions', props.correlation_id)
            return NO_MATCHING_SUBSCRIPTIONS

        client_map = {}
        for sub in subs:
            cid = sub.get_session_id()

            if sub.get_no_local() and cid == self.session.get_id():
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

        return SUCCESS
        

    def subscribe(self, request, props, topic):

        topic_filter = topic.get('filter')
        LOGGER.info('%s: Subscribing on topic filter %s',
                    props.correlation_id, topic.get('filter'))

        if not self.authorize_subscribe(self.session.get_email(), topic.get('filter')):
            return NOT_AUTHORIZED

        topic['sub_id'] = request.get('properties').get('subscription_identifier') or 0
        topic['session_id'] = self.session.get_id()
        self.db.add_sub(self.session, topic)
        return topic.get('max_qos')

    def unsubscribe(self, request, props, topic):

        LOGGER.info('%s: Unsubscribing from topic filter %s',
                    props.correlation_id, topic.get('filter'))
        sub = self.db.get_sub(self.session, topic.get('filter'))

        if not sub:
            return NO_SUBSCRIPTION_EXISTED

        self.db.delete_sub(sub)
        return SUCCESS

    def authorize_subscribe(self, email, topic):
        return self.authorize(email, topic, True)

    def authorize_publish(self, email, topic):
        return self.authorize(email, topic, False)

    def authorize(self, email, topic, read):
        if not topic.startswith(DEVICE_PREFIX):
            return True
        request = {
            'command': 'get_read_access' if read else 'get_write_access',
            'user': email,
            'resource': topic,}
        response = self.rpc(
            obj=request,
            queue=env['ACCESS_CONTROL_SERVICE'])
        return response.get('read_access' if read else 'write_access')



   








