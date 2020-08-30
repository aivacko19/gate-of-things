
import os
import time
import logging

import abstract_service

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

class Service(abstract_service.AbstractService):

    def __init__(self, queue, db, expiry_table, dummy_messenger=None):
        self.db = db
        self.dummy_messenger = dummy_messenger
        self.expiry_table = expiry_table
        abstract_service.AbstractService.__init__(self, queue)
        self.actions = {'publish': self._publish,
                        'pubrel': self.pubrel,
                        'subscribe': self.subscribe,
                        'unsubscribe': self.unsubscribe,
                        'get_session': self.get_session,
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
        topic = request.get('topic')
        payload = request.get('payload')
        properties = request.get('properties')
        pid_in_use = qos > 0 and self.db.is_pid_in_use(cid, pid)
        payload_format = properties.get('payload_format_indicator') if properties else 0
        payload_format_invalid = False
        try:
            payload.decode('utf-8')
        except UnicodeDecodeError as e:
            payload_format_invalid = payload_format == UTF8_FORMAT
        subs = self.db.get_matching_subs(topic)
        response_type = 'puback' if qos == 1 else 'pubrec'
        code = 0

        # Authorize publishing to device
        can_publish = self.authorize_publish(self.session.get_email(), topic)
        if not can_publish:
            LOGGER.info('ERROR: %s: Not authorized to publish to the topic %s', cid, topic)
            code = NOT_AUTHORIZED
        elif pid_in_use:
            LOGGER.info('ERROR: %s: Packet identifier in use', cid)
            code = PACKET_IDENTIFIER_IN_USE
        elif payload_format_invalid:
            LOGGER.info('ERROR: %s: Payload format is invalid', cid)
            code = PAYLOAD_FORMAT_INVALID
        elif not subs:
            LOGGER.info('%s: No matching subscriptions', cid)
            code = NO_MATCHING_SUBSCRIPTIONS
        else:
            code = SUCCESS
            if qos == 2: self.db.add_packet_id(cid, pid)
            # Sending a copy of the message to each subscriber
            client_map = {}
            for sub in subs:
                receiver_cid = sub.get_session_id()
                # If no local flag set, no sending back to publisher
                if sub.get_no_local() and receiver_cid == cid: continue
                # Multiple occurrence of subscriber
                if client_map.get(receiver_cid):
                    if sub.get_sub_id(): client_map[receiver_cid]['sids'].append(sub.get_sub_id())
                    continue
                    
                message = request.copy()
                message['command'] = 'publish'
                message['topic'] = sub.get_topic_filter()
                message['qos'] = min(request.get('qos'), sub.get_max_qos())
                message['sids'] = list()
                if sub.get_sub_id(): message['sids'].append(sub.get_sub_id())
                client_map[receiver_cid] = message
            for receiver_cid, message in client_map.items():
                email = self.db.get(receiver_cid).get_email()
                # Log receiving of message
                self.log_action(email, message['topic'], 'receive', True)
                message['properties']['subscription_identifier'] = message['sids']
                self.publish(request=message, queue=env['MESSAGE_SERVICE'], correlation_id=receiver_cid)

        # Log attempt of access
        self.log_action(self.session.get_email(), topic, 'publish', code in [SUCCESS, NO_MATCHING_SUBSCRIPTIONS])
        if qos > 0: return {'command': 'forward', 'type': response_type, 'id': pid, 'code': code,}

    # Process Publish Release package (QoS 2)
    def pubrel(self, request, props):
        cid = props.correlation_id
        code = request.get('code')
        pid = request.get('id')
        pid_in_use = self.db.delete_packet_id(cid, pid)
        if code < 0x80: return {'command': 'forward',
                                'type': 'pubcomp',
                                'id': pid,
                                'code': SUCCESS if pid_in_use else PACKET_IDENTIFIER_NOT_FOUND,}

    # Process subscribe request                                             // service.py - Subscription Manager
    def subscribe(self, request, props):
        cid = props.correlation_id
        pid, topics, properties = request.get('id'), request.get('topics'), request.get('properties')
        subscription_identifier = properties.get('subscription_identifier') if properties else 0
        pid_in_use = self.db.is_pid_in_use(cid, pid)
        codes = list()
        # Processing multiple topics from request
        for topic in topics:
            topic_filter = topic.get('filter')
            topic['session_id'], topic['sub_id'] = subscription_identifier, cid
            LOGGER.info('%s: Subscribing on topic filter %s', cid, topic_filter)

            # Checking subscription rights 
            read_access, access_time = self.authorize_subscribe(self.session.get_email(), topic_filter)
            # Logging attempt of access
            self.log_action(self.session.get_email(), topic_filter, 'subscribe', read_access and not pid_in_use)

            if not read_access:
                LOGGER.info('ERROR: %s: Client is not authorized', cid)
                code = NOT_AUTHORIZED
            elif pid_in_use:
                LOGGER.info('ERROR: %s: Packet identifier in use', cid)
                code = PACKET_IDENTIFIER_IN_USE
            else:
                s_id = self.db.add_sub(topic)
                code = topic.get('max_qos')
                if access_time > 0: self.expiry_table.add_entry(s_id, access_time)
            codes.append({'code': code})

        if codes: return {'command': 'forward', 'type': 'suback', 'id': pid, 'topics': codes,}

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
            sub = self.db.get_sub(cid, topic_filter)

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
    def get_session(self, request, props):
        return {'email': self.session.get_email() if self.session else None}

    # Create new session, delete if session exists
    def create_session(self, request, props):
        cid = props.correlation_id
        email = request.get('email')
        if self.session:
            self.db.delete(self.session)
        self.db.add(cid, email)
        if email.endswith('@device'):
            topic_filter = DEVICE_PREFIX + cid + CONTROL_SUFFIX
            self.db.add_sub({'filter': topic_filter, 'session_id': cid,})

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
        if not topic.startswith(DEVICE_PREFIX): return True, 0

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

        request = {'command': 'get_read_access',
                   'user': email,
                   'resource': topic,}
        response = self.rpc(obj=request,
                            queue=env['ACCESS_CONTROL_SERVICE'])
        read_access = response.get('read_access')
        if not read_access: return False, 0

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
        if not is_device_control_topic: return False

        request = {'command': 'get_write_access',
                   'user': email,
                   'resource': topic,}
        response = self.rpc(request=request,
                            queue=env['ACCESS_CONTROL_SERVICE'])
        return response.get('write_access')

    def log_action(self, email, resource, action, success):
        if not resource.startswith(DEVICE_PREFIX):
            return

        resource = resource[len(DEVICE_PREFIX):]
        if resource.endswith(CONTROL_SUFFIX):
            resource = resource[:len(resource)-len(CONTROL_SUFFIX)]
        elif email == resource + '@device' and action == 'publish':
            return

        response = {'command': 'log',
                    'user': email,
                    'resource': resource,
                    'action': action,
                    'success': success,}

        self.publish(request=response,
                     queue=env['LOGGER_SERVICE'],)