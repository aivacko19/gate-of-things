
import os
import logging
import time

from . import abstract_service

SUCCESS = 0X00
CONTINUE_AUTHENTICATION = 0x18
REAUTHENTICATE = 0x19
UNSPECIFIED_ERROR = 0x80
MALFORMED_PACKET = 0X81
PROTOCOL_ERROR = 0X82
NOT_AUTHORIZED = 0x87
BAD_AUTHENTICATION_METHOD = 0x8C
SESSION_TAKEN_OVER = 0x8E
PAYLOAD_FORMAT_INVALID = 0X99

LOGGER = logging.getLogger(__name__)

env = {
    'QUEUE': None,
    'OAUTH_SERVICE': None,
    'SUBSCRIPTION_SERVICE': None,
    'MESSAGE_SERVICE': None,
    'DEVICE_SERVICE': None,
}

for key in env:
    service = os.environ.get(key)
    if not service:
        raise Exception('Environment variable %s not defined', key)
    env[key] = service

class Service(abstract_service.AbstractService):

    def __init__(self, queue, db, dummy_messenger=None):
        self.db = db
        self.dummy_messenger = dummy_messenger
        abstract_service.AbstractService.__init__(self, queue)
        self.actions = {'disconnect': self.disconnect,
                        'process': self.process,
                        'oauth_uri': self.oauth_uri,
                        'verify': self.verify,
                        'forward': self.forward,}

    def prepare_action(self, request, props):
        cid = props.correlation_id
        self.conn = self.db.get(cid)

    def reply_to_sender(self, response, properties):
        if response:
            if 'command' in response:
                response['state'] = response['command']
            response['command'] = 'receive_package'
            socket, reply_queue = self.conn.get_socket()
            LOGGER.info('Responding with: %s', response)
            self.publish(request=response,
                         queue=reply_queue,
                         correlation_id=socket)

    # Drop the connection
    def disconnect(self, request, props):
        self.conn = self.db.get_by_socket(props.correlation_id, props.reply_to)
        LOGGER.info('Disconnecting client %s', props.correlation_id)
        self.db.delete_by_socket(props.correlation_id, props.reply_to)

    # Process and route client package
    def process(self, request, props):
        self.conn = self.db.get_by_socket(props.correlation_id, props.reply_to)
        client_connected = self.conn.connected()
        packet_type = request.get('type')
        service = ''
        request['command'] = request.get('type')

        if client_connected and packet_type == 'disconnect':
            return {'state': 'disconnect'}

        # Processing of first client packet                                     // service.py - Request Router
        if not client_connected:
            # Connect
            if packet_type == 'connect':
                cid, clean_start = request.get('client_id'), request.get('clean_start')
                username, password = request.get('username'), request.get('password')
                receive_maximum = request.get('properties').get('receive_maximum')
                method = request.get('properties').get('authentication_method')
                self.conn.set_parameters(cid, not cid, clean_start, method)

                # Request authentication from authentication services
                if method == 'OAuth2.0':
                    service = 'OAUTH_SERVICE'
                    request = {'command': 'get_uri', 'queue': env['QUEUE']}
                elif method == 'SignatureValidation':
                    service = 'DEVICE_SERVICE'
                    request = {'command': 'authenticate', 'resource_uri': username, 'token': password}
                else:
                    return {'state': 'disconnect', 'type': 'connack', 'code': BAD_AUTHENTICATION_METHOD,}
                self.redirect(service, request)
                    
                self.take_over_session(cid) # Disconnect client with same client id if any
                self.db.add(self.conn)      # Save connection
                # Setting message quota for client
                self.redirect('MESSAGE_SERVICE', {'command': 'set_quota', 'receive_maximum': receive_maximum})
                return # No response to Gateway until authentication services replies
            # Ping
            elif packet_type == 'pingreq': return {'state': 'disconnect', 'type': 'pingresp'}
            # Protocol Error if any other packet type
            else: return {'state': 'disconnect', 'type': 'disconnect', 'code': PROTOCOL_ERROR}


        # Waiting for client verification
        if not self.conn.get_email():
            if packet_type != 'auth':
                return {'state': 'disconnect', 'type': 'connack', 'code': PROTOCOL_ERROR}
        # Subscription managment
        elif packet_type in ['publish', 'pubrel', 'subscribe', 'unsubscribe']:
            service = 'SUBSCRIPTION_SERVICE'
            if packet_type == 'publish':
                request['time_received'] = int(time.time()*10**4)
        # Message managment
        elif packet_type in ['puback', 'pubrec', 'pubcomp']:
            service = 'MESSAGE_SERVICE'
        # Reauthenticate
        elif packet_type == 'auth' and request.get('code') == REAUTHENTICATE:
            method = request.get('properties').get('authentication_method')
            if method != self.conn.get_method():
                return {'state': 'disconnect', 'type': 'disconnect', 'code': BAD_AUTHENTICATION_METHOD,}
            if method == 'OAuth2.0':
                service = 'OAUTH_SERVICE'
                request = {'command': 'get_uri', 'queue': env['QUEUE']}
            elif method == 'SignatureValidation':
                service = 'DEVICE_SERVICE'
                request = {'command': 'authenticate', 'resource_uri': username, 'token': password}
            else:
                return {'state': 'disconnect', 'type': 'disconnect', 'code': BAD_AUTHENTICATION_METHOD,}
        # Ping
        elif packet_type == 'pingreq':
            return {'state': 'read', 'type': 'pingresp'}
        else:
            return {'state': 'disconnect', 'type': 'disconnect', 'code': PROTOCOL_ERROR}

        self.redirect(service, request)
        return {'state': 'read'}

    # Receive URI back from OAuth service
    def oauth_uri(self, request, props):
        uri = request.get('uri')
        return {'state': 'idle',
                'type': 'auth',
                'code': CONTINUE_AUTHENTICATION,
                'properties': {
                    'authentication_method': 'OAuth2.0',
                    'authentication_data': uri.encode('utf-8'),},}

    # Complete authentication
    def verify(self, request, props):
        cid = props.correlation_id
        email = request.get('email')

        # Authentiction failed, disconnect client
        if not email:
            LOGGER.info('Authentication failed, email not verified')
            return {'state': 'disconnect',
                    'type': 'disconnect' if self.conn.get_email() else 'connack',
                    'code': NOT_AUTHORIZED}

        LOGGER.info('Authentication successful, user id: %s', email)
        service = 'SUBSCRIPTION_SERVICE'

        # Reauthentication
        if self.conn.get_email():
            # Validate subscriptions only if email is different
            if self.conn.get_email() != email:
                self.conn.set_email(email)
                self.db.update(self.conn)
                self.redirect(service, {'command': 'reauthenticate', 'email': email,})
            return {'state': 'read',
                    'type': 'auth',
                    'code': SUCCESS,
                    'properties': {'authentication_method': 'OAuth2.0',},}

        # First time authenticated
        self.conn.set_email(email)
        self.db.update(self.conn)
        session = self.redirect(service, {'command': 'get_session'}, reply=True)
        session_present = bool(session.get('email')) and not self.conn.get_clean_start()

        # Creating session 
        if not session_present or email != session.get('email'):
            request = {'command': 'reauthenticate' if session_present else 'create_session',
                       'email': email,}
            self.redirect(service, request)

        properties = {'assigned_client_identifier': cid} if self.conn.get_random_id() else {}
        return {'state': 'read',
                'type': 'connack',
                'code': SUCCESS,
                'session_present': session_present,
                'properties': properties,}

    # Forward message from other service to client
    def forward(self, request, props):
        request['state'] = 'read'
        return request

    def redirect(self, target, request, reply=False):
        if not target: return
        if reply:
            self.rpc(request=request,
                     queue=env[target],
                     correlation_id=self.conn.get_id())
        else:
            self.publish(request=request,
                         queue=env[target],
                         correlation_id=self.conn.get_id())

    def take_over_session(self, cid):
        if not cid: return
        conn = self.db.get(cid)
        if not conn: return
        socket, reply_queue = conn.get_socket()
        response = {'state': 'disconnect',
                    'type': 'disconnect',
                    'code': SESSION_TAKEN_OVER,}
        self.publish(request=response,
                     queue=reply_queue,
                     correlation_id=socket)
        self.db.delete(cid)