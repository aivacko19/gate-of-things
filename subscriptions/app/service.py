
import os
import time
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
CONTROL_SUFFIX = '/ctrl'
DEVICE_EMAIL_SUFFIX = '@device'

env = {
    'MESSAGE_SERVICE': None,
    'ACCESS_CONTROL_SERVICE': None,
    'LOGGER_SERVICE': None,
}

for key in env:
    service = os.environ.get(key)
    if not service:
        raise Exception('Environment variable %s not defined', key)
    env[key] = service

class Service(amqp_helper.AmqpAgent):

    def __init__(self, queue, db, expiry_table):
        self.db = db
        self.expiry_table = expiry_table
        amqp_helper.AmqpAgent.__init__(self, queue)
        self.actions = {
            'publish': self._publish,
            'pubrel': self.pubrel,
            'subscribe': self.subscribe,
            'unsubscribe': self.unsubscribe,
            'fetch_session': self.fetch_session,
            'create_session': self.create_session,
            'reauthenticate': self.reauthenticate,
            'get_subscriptions': self.get_subscriptions,
            'delete_subscription': self.delete_subscription,}

    def prepare_action(self, request, props):
        cid = props.correlation_id
        self.session = self.db.get(cid)

    # Process Publish package (QoS 0-2)
    def _publish(self, request, props):
        cid = props.correlation_id
        pid = request.get('id')
        qos = request.get('qos')
        
        response = {
            'command': 'forward' if qos > 0 else 'stop',
            'type': 'puback' if qos == 1 else 'pubrec',
            'id': pid,}

        pid_in_use = self.db.is_pid_in_use(cid, pid)
        if qos > 0 and pid_in_use:
            LOGGER.info('ERROR: %s: Packet identifier in use', cid)
            response['code'] = PACKET_IDENTIFIER_IN_USE
            return response

        topic = request.get('topic')

        can_publish = self.authorize_publish(self.session.get_email(), topic)
        if topic.endswith(CONTROL_SUFFIX):
            self.log_action(self.session.get_email(), topic, 'publish', can_publish)

        if not can_publish:
            LOGGER.info('ERROR: %s: Not authorized to publish to the topic %s', cid, topic)
            response['code'] = NOT_AUTHORIZED
            return response

        payload = request.get('payload')
        payload_format = request.get('properties').get('payload_format_indicator')
        if payload_format == UTF8_FORMAT:
            try:
                payload.decode('utf-8')
            except UnicodeDecodeError as e:
                LOGGER.info('ERROR: %s: Payload format is invalid', cid)
                response['code'] = PAYLOAD_FORMAT_INVALID
                return response

        subs = self.db.get_matching_subs(topic)

        if not subs:
            LOGGER.info('%s: No matching subscriptions', cid)
            response['code'] = NO_MATCHING_SUBSCRIPTIONS
            return response

        # Sending a copy of the message to each subscriber
        client_map = {}
        for sub in subs:
            receiver_cid = sub.get_session_id()

            if sub.get_no_local() and receiver_cid == cid:
                continue

            if client_map.get(receiver_cid):
                if sub.get_sub_id():
                    client_map[receiver_cid]['properties']['subscription_identifier'].append(sub.get_sub_id())
            else:
                email = self.db.get(receiver_cid).get_email()
                resource = sub.get_topic_filter()
                self.log_action(email, resource, 'receive', True)

                # Adjusting the publish package
                message = request.copy()
                message['topic'] = sub.get_topic_filter()
                message['qos'] = min(request.get('qos'), sub.get_max_qos())
                message['properties']['subscription_identifier'] = list()
                if sub.get_sub_id():
                    message['properties']['subscription_identifier'].append(sub.get_sub_id())
                client_map[receiver_cid] = message

        for receiver_cid, message in client_map.items():
            message['command'] = message['type']
            self.publish(
                obj=message,
                queue=env['MESSAGE_SERVICE'],
                correlation_id=receiver_cid)

        if qos == 2:
            self.db.add_packet_id(cid, pid)

        response['code'] = SUCCESS
        return response

    # Process Publish Release package (QoS 2)
    def pubrel(self, request, props):
        cid = props.correlation_id
        code = request.get('code')
        pid = request.get('id')
        pid_in_use = self.db.delete_packet_id(cid, pid)

        if code >= 0x80:
            return None

        return {
            'command': 'forward',
            'type': 'pubcomp',
            'id': pid,
            'code': SUCCESS if pid_in_use else PACKET_IDENTIFIER_NOT_FOUND,}

    # Process Subscribe request
    def subscribe(self, request, props):
        cid = props.correlation_id
        pid = request.get('id')
        topics = request.get('topics')
        
        codes = list()

        for topic in topics:
            topic_filter = topic.get('filter')

            pid_in_use = self.db.is_pid_in_use(cid, pid)
            if pid_in_use:
                LOGGER.info('ERROR: %s: Packet identifier in use', cid)
                codes.append({'code': PACKET_IDENTIFIER_IN_USE})
                continue

            LOGGER.info('%s: Subscribing on topic filter %s', cid, topic_filter)

            read_access, access_time = self.authorize_subscribe(self.session.get_email(), topic_filter)
            self.log_action(self.session.get_email(), topic_filter, 'subscribe', read_access)
            if not read_access:
                LOGGER.info('ERROR: %s: Client is not authorized', cid)
                codes.append({'code': NOT_AUTHORIZED})
                continue

            if request.get('properties'):
                topic['sub_id'] = request.get('properties').get('subscription_identifier') or 0
            else:
                topic['sub_id'] = 0
            topic['session_id'] = cid
            s_id = self.db.add_sub(self.session, topic)

            if access_time > 0:
                actual_time = int(time.time()) + access_time
                self.expiry_table.add_entry(s_id, actual_time)

            codes.append({'code': topic.get('max_qos')})

        if not codes:
            return None

        return {
            'command': 'forward',
            'type': 'suback',
            'id': pid,
            'topics': codes,}

    # Process Unsubscribe request
    def unsubscribe(self, request, props):
        cid = props.correlation_id
        pid = request.get('id')
        topics = request.get('topics')

        codes = list()

        for topic in request.get('topics'):
            topic_filter = topic.get('filter')

            pid_in_use = self.db.is_pid_in_use(cid, pid)
            if pid_in_use:
                LOGGER.info('ERROR: %s: Packet identifier in use', cid)
                codes.append({'code': PACKET_IDENTIFIER_IN_USE})
                continue

            LOGGER.info('%s: Unsubscribing from topic filter %s', cid, topic_filter)
            sub = self.db.get_sub(self.session, topic_filter)

            if not sub:
                LOGGER.info('ERROR: %s: Subscription not existed', cid)
                self.log_action(self.session.get_email(), topic_filter, 'unsubscribe', False)
                codes.append({'code': NO_SUBSCRIPTION_EXISTED})
                continue
            
            self.log_action(self.session.get_email(), topic_filter, 'unsubscribe', True)

            self.db.delete_sub(sub)
            codes.append({'code': SUCCESS})

        if not codes:
            return None

        return {
            'command': 'forward',
            'type': 'unsuback',
            'id': pid,
            'topics': codes,}

    # Get session data if session exists
    def fetch_session(self, request, props):
        return {'email': self.session.get_email() if self.session else None}

    # Create new session, delete if session exists
    def create_session(self, request, props):
        cid = props.correlation_id
        email = request.get('email')
        if self.session:
            self.db.delete(self.session)
        self.db.add(cid, email)

    # Update client email
    def reauthenticate(self, request, props):
        email = request.get('email')
        for sub in self.session.get_subs():
            if not self.authorize_subscribe(email, sub.get_topic_filter()):
                self.db.delete_sub(sub)
        self.session.set_email(email)
        self.db.update(self.session)

    # Get users that subscribed on topic
    def get_subscriptions(self, request, props):
        topic = request.get('topic')
        subscriptions = self.db.get_subscribed_users(topic)
        return {'subscriptions': subscriptions,}

    # Delete subscription
    def delete_subscription(self, request, props):
        topic = request.get('topic')
        email = request.get('email')
        result = self.db.delete_sub_by_email_and_topic(email, topic)
        return {'result': result}


    def authorize_subscribe(self, email, topic):
        if not topic.startswith(DEVICE_PREFIX):
            return True, 0

        # Strip topic from prefix and sufix
        is_device_control_topic = topic.endswith(CONTROL_SUFFIX)
        topic = topic[len(DEVICE_PREFIX):]
        if is_device_control_topic:
            topic = topic[:len(topic)-len(CONTROL_SUFFIX)]

        # Authorize device
        if email.endswith(DEVICE_EMAIL_SUFFIX):
            device_name = email[:len(email)-len(DEVICE_EMAIL_SUFFIX)]
            if is_device_control_topic:
                return topic == device_name, 0
            else:
                return False, 0

        # Authorize client
        if is_device_control_topic:
            return False, 0

        request = {
            'command': 'get_read_access',
            'user': email,
            'resource': topic,}
        response = self.rpc(
            obj=request,
            queue=env['ACCESS_CONTROL_SERVICE'])
        read_access = response.get('read_access')
        if not read_access:
            return False, 0

        return read_access, response.get('access_time')

    def authorize_publish(self, email, topic):
        if not topic.startswith(DEVICE_PREFIX):
            return True

        # Strip topic from prefix and sufix
        is_device_control_topic = topic.endswith(CONTROL_SUFFIX)
        topic = topic[len(DEVICE_PREFIX):]
        if is_device_control_topic:
            topic = topic[:len(topic)-len(CONTROL_SUFFIX)]

        # Authorize device
        if email.endswith(DEVICE_EMAIL_SUFFIX):
            device_name = email[:len(email)-len(DEVICE_EMAIL_SUFFIX)]
            if is_device_control_topic:
                return False
            else:
                return topic == device_name

        #Authorize client
        if not is_device_control_topic:
            return False

        request = {
            'command': 'get_write_access',
            'user': email,
            'resource': topic,}
        response = self.rpc(
            obj=request,
            queue=env['ACCESS_CONTROL_SERVICE'])
        return response.get('write_access')

    def log_action(self, email, resource, action, success):
        if not resource.startswith(DEVICE_PREFIX):
            return

        resource = resource[len(DEVICE_PREFIX):]
        if resource.endswith(CONTROL_SUFFIX):
            resource = resource[:len(resource)-len(CONTROL_SUFFIX)]

        response = {
            'command': 'log',
            'user': email,
            'resource': resource,
            'action': action,
            'success': success,}

        self.publish(
            obj=response,
            queue=env['LOGGER_SERVICE'],)
    # def main(self, request, props):
    #     pid_in_use = False
    #     if props.correlation_id:
    #         LOGGER.info('Fetching session with id %s', props.correlation_id)
    #         self.session = self.db.get(props.correlation_id)
    #         pid_in_use = (self.db.is_pid_in_use(self.session.get_id(), request.get('id')) 
    #                       if self.session else False)
    #     command = request.get('command')
    #     del request['command']
    #     response = {}

    #     if command == 'publish':
    #         code = SUCCESS
    #         qos = request.get('qos')
    #         if qos > 0 and pid_in_use:
    #             LOGGER.info('ERROR: %s: Packet identifier in use', props.correlation_id)
    #             code = PACKET_IDENTIFIER_IN_USE
    #         else:
    #             code = self._publish(request, props)
    #         if qos > 0:
    #             response = {
    #                 'command': 'forward',
    #                 'type': 'puback' if qos == 1 else 'pubrec',
    #                 'id': request.get('id'),
    #                 'code': code,}
    #             if qos == 2 and code == SUCCESS:
    #                 self.db.add_packet_id(props.correlation_id, request.get('id'))

    #     elif command == 'pubrel':
    #         code = SUCCESS if pid_in_use else PACKET_IDENTIFIER_NOT_FOUND
    #         if request.get('code') < 0x80:
    #             response = {
    #                 'command': 'forward',
    #                 'type': 'pubcomp',
    #                 'id': request.get('id'),
    #                 'code': code,}
    #         if code == SUCCESS:
    #             self.db.delete_packet_id(props.correlation_id, request.get('id'))

    #     elif command in ['subscribe', 'unsubscribe']:
    #         codes = list()

    #         for topic in request.get('topics'):
    #             if pid_in_use:
    #                 LOGGER.info('ERROR: %s: Packet identifier in use', props.correlation_id)
    #                 code = PACKET_IDENTIFIER_IN_USE
    #             elif command == 'subscribe':
    #                 code = self.subscribe(request, props, topic)
    #             elif command == 'unsubscribe':
    #                 code = self.unsubscribe(request, props, topic)

    #             codes.append({'code': code})

    #         if codes:
    #             response = {
    #                 'command': 'forward',
    #                 'type': 'suback' if command == 'subscribe' else 'unsuback',
    #                 'id': request.get('id'),
    #                 'topics': codes,}

    #     elif command == 'fetch_session':
    #         LOGGER.info('Received command get')
    #         response = {
    #             'email': self.session.get_email() if self.session else None}

    #     elif command == 'create_session':
    #         if self.session:
    #             self.db.delete(self.session)
    #         self.db.add(props.correlation_id, request.get('email'))

    #     elif command == 'reauthenticate':
    #         email = request.get('email')
    #         for sub in self.session.get_subs():
    #             if not self.authorize_subscribe(email, sub.get_topic_filter()):
    #                 self.db.delete_sub(sub)
    #         self.session.set_email(email)
    #         self.db.update(self.session)
    #     elif command == 'get_subscriptions':
    #         topic = request.get('topic')
    #         subscriptions = self.db.get_subscribed_users(topic)
    #         response['subscriptions'] = subscriptions
    #     elif command == 'delete_subscription':
    #         topic = request.get('topic')
    #         email = request.get('email')
    #         result = self.db.delete_sub_by_email_and_topic(email, topic)
    #         response['result'] = result

    #     if response:
    #         self.publish(
    #             obj=response,
    #             queue=props.reply_to,
    #             correlation_id=props.correlation_id)

    # def _publish(self, request, props):

    #     topic = request.get('topic')

    #     can_publish = self.authorize_publish(self.session.get_email(), topic)
    #     if topic.endswith(CONTROL_SUFFIX)
    #         self.log_action(self.session.get_email(), topic[:len(topic)-len(CONTROL_SUFFIX)],
    #          'publish', can_publish)
    #     if not can_publish:
    #         LOGGER.info('ERROR: %s: Not authorized to publish to the topic %s',
    #                     props.correlation_id, topic)
    #         return NOT_AUTHORIZED

    #     payload = request.get('payload')
    #     payload_format = request.get('properties').get('payload_format_indicator')
    #     if payload_format == UTF8_FORMAT:
    #         try:
    #             payload.decode('utf-8')
    #         except UnicodeDecodeError as e:
    #             LOGGER.info('ERROR: %s: Payload format is invalid', props.correlation_id)
    #             return PAYLOAD_FORMAT_INVALID

    #     subs = self.db.get_matching_subs(topic)

    #     if not subs:
    #         LOGGER.info('%s: No matching subscriptions', props.correlation_id)
    #         return NO_MATCHING_SUBSCRIPTIONS

    #     client_map = {}
    #     for sub in subs:
    #         cid = sub.get_session_id()

    #         if sub.get_no_local() and cid == self.session.get_id():
    #             continue

    #         if client_map.get(cid):
    #             if sub.get_sub_id():
    #                 client_map[cid]['properties']['subscription_identifier'].append(sub.get_sub_id())
    #         else:
    #             email = self.db.get(cid).get_email()
    #             resource = sub.get_topic_filter()
    #             self.log_action(email, resource, 'receive_data', True)

    #             response = request.copy()
    #             response['topic'] = sub.get_topic_filter()
    #             response['qos'] = min(request.get('qos'), sub.get_max_qos())
    #             response['properties']['subscription_identifier'] = list()
    #             if sub.get_sub_id():
    #                 response['properties']['subscription_identifier'].append(sub.get_sub_id())
    #             client_map[cid] = response

    #     for response in client_map.values():
    #         self.publish(
    #             obj=response,
    #             queue=env['MESSAGE_SERVICE'],
    #             correlation_id=sub.get_session_id())

    #     return SUCCESS
        

    # def subscribe(self, request, props, topic):

    #     topic_filter = topic.get('filter')
    #     LOGGER.info('%s: Subscribing on topic filter %s',
    #                 props.correlation_id, topic.get('filter'))

    #     read_access, access_time = self.authorize_subscribe(self.session.get_email(), topic.get('filter')):
    #     self.log_action(self.session.get_email(), topic_filter, 'subscribe', read_access)
    #     if not read_access:
    #         return NOT_AUTHORIZED

    #     if request.get('properties'):
    #         topic['sub_id'] = request.get('properties').get('subscription_identifier') or 0
    #     else:
    #         topic['sub_id'] = 0
    #     topic['session_id'] = self.session.get_id()
    #     s_id = self.db.add_sub(self.session, topic)

    #     if access_time > 0:
    #         actual_time = int(time.time()) + access_time
    #         self.expiry_table.add_entry(s_id, actual_time)

    #     return topic.get('max_qos')

    # def unsubscribe(self, request, props, topic):

    #     LOGGER.info('%s: Unsubscribing from topic filter %s',
    #                 props.correlation_id, topic.get('filter'))
    #     sub = self.db.get_sub(self.session, topic.get('filter'))

    #     if not sub:
    #         return NO_SUBSCRIPTION_EXISTED
    #     else:
    #         self.log_action(self.session.get_email(), topic.get('filter'), 'unsubscribe', True)

    #     self.db.delete_sub(sub)
    #     return SUCCESS




   








