
import os
import logging
import time

import abstract_service

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

    def __init__(self, queue, db):
        self.db = db
        abstract_service.AbstractService.__init__(self, queue)
        self.actions = {
            'disconnect': self.disconnect,
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
            self.publish(
                obj=response,
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
            return {'state': 'read': 'type': 'pingresp'}
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
            self.rpc(obj=request,
                     queue=env[target],
                     correlation_id=self.conn.get_id())
        else:
            self.publish(obj=request,
                         queue=env[target],
                         correlation_id=self.conn.get_id())

    def take_over_session(self, cid):
        if not cid: return
        conn = self.db.get(cid)
        if not conn: return
        socket, reply_queue = conn.get_socket()
        response = {
            'state': 'disconnect',
            'type': 'disconnect',
            'code': SESSION_TAKEN_OVER,}
        self.publish(
            obj=response,
            queue=reply_queue,
            correlation_id=socket)
        self.db.delete(cid)

    # def main(self, request, props):

    #     command = request.get('command')
    #     del request['command']

    #     conn = (self.db.get(props.correlation_id) 
    #             if command not in ['disconnect', 'process']
    #             else self.db.get_by_socket(props.correlation_id, props.reply_to))
    #     response = {}

    #     if command == 'disconnect':
    #         LOGGER.info('Disconnecting client %s', props.correlation_id)
    #         self.db.delete_by_socket(props.correlation_id, props.reply_to)

    #     elif command == 'process':
    #         response = self.process(request, props, conn)

    #     elif command == 'oauth_uri':
    #         response = {
    #             'command': 'idle',
    #             'type': 'auth',
    #             'code': CONTINUE_AUTHENTICATION,
    #             'properties': {
    #                 'authentication_method': 'OAuth2.0',
    #                 'authentication_data': request['uri'].encode('utf-8'),},}

    #     elif command == 'verify':
    #         email = request.get('email')
    #         if email:
    #             LOGGER.info('Authentication successful, user id: %s', email)
    #             if not conn.get_email():
    #                 conn.set_email(email)
    #                 self.db.update(conn)
    #                 obj = self.rpc(
    #                     obj={'command': 'fetch_session'},
    #                     queue=env['SUBSCRIPTION_SERVICE'],
    #                     correlation_id=props.correlation_id,)
    #                 email = obj.get('email')
    #                 response = {
    #                     'command': 'read',
    #                     'type': 'connack',
    #                     'code': SUCCESS,
    #                     'session_present': bool(email) and not conn.get_clean_start(),
    #                     'properties': {}}
    #                 if conn.get_random_id():
    #                     response['properties']['assigned_client_identifier'] = conn.get_id()

    #                 command = None
    #                 if not response['session_present']:
    #                     command = 'create_session'
    #                 elif conn.get_email() != email:
    #                     command = 'reauthenticate'

    #                 if command:
    #                     session_response = {
    #                         'command': command,
    #                         'email': conn.get_email(),}
    #                     self.redirect(session_response, conn, 'SUBSCRIPTION_SERVICE')

    #                 if conn.get_email().endswith('@device'):
    #                     session_response = {
    #                         'command': 'subscribe',
    #                         'id': 0,
    #                         'topics': [{
    #                             'filter': 'device/%s/ctrl' % props.correlation_id,
    #                             'max_qos': 2,
    #                             'no_local': False,
    #                             'retain_handling': 0,
    #                             'retain_as_published': False}],
    #                     }
    #                     obj = self.rpc(
    #                         obj=session_response,
    #                         queue=env['SUBSCRIPTION_SERVICE'],
    #                         correlation_id=props.correlation_id)


    #             else:

    #                 # Validate subscriptions only if email is different
    #                 if conn.get_email() != email:
    #                     conn.set_email(email)
    #                     self.db.update(conn)
    #                     response = {
    #                         'command': 'reauth',
    #                         'email': email,}
    #                     self.redirect(response, conn, 'SUBSCRIPTION_SERVICE')

    #                 response = {
    #                     'command': 'read',
    #                     'type': 'auth',
    #                     'code': SUCCESS,
    #                     'properties': {
    #                         'authentication_method': 'OAuth2.0',},}
    #         else:
    #             LOGGER.info('Authentication failed, email not verified')
    #             response = {
    #                 'command': 'disconnect' if conn.get_email() else 'connack',
    #                 'type': 'disconnect',
    #                 'code': NOT_AUTHORIZED}

    #     elif command == 'forward':
    #         response = request
    #         response['command'] = 'read'

    #     if response:
    #         socket, reply_queue = conn.get_socket()
    #         LOGGER.info('Responding with: %s', response)
    #         self.publish(
    #             obj=response,
    #             queue=reply_queue,
    #             correlation_id=socket)
        
    # def process(self, request, props):

    #     packet_type = request.get('type')
    #     response = {}

    #     # First packet
    #     if not self.conn.connected():

    #         # Ping
    #         if packet_type == 'pingreq':
    #             response = {
    #                 'command': 'disconnect',
    #                 'type': 'pingresp'}

    #         # Connect
    #         elif packet_type == 'connect':
    #             method = request['properties'].get('authentication_method')
    #             if not method or method not in ['OAuth2.0', 'SignatureValidation']:
    #                 response = {
    #                     'command': 'disconnect',
    #                     'type': 'connack',
    #                     'code': BAD_AUTHENTICATION_METHOD}
    #             else:
    #                 # No response until verified
    #                 cid = request['client_id']
    #                 method = request['properties'].get('authentication_method')
    #                 self.conn.set_method(method)
    #                 self.conn.set_clean_start(request.get('clean_start'))
    #                 if not cid: 
    #                     self.conn.set_random_id(True)
    #                 else:
    #                     self.conn.set_id(cid)
    #                     self.take_over_session(cid)

    #                 cid = self.db.add(self.conn)
    #                 self.redirect(request, 'MESSAGE_SERVICE')
    #                 if method == 'OAuth2.0':
    #                     oauth_request = {
    #                         'command': 'oauth_request',
    #                         'queue': env['QUEUE']}
    #                     self.redirect(oauth_request, 'OAUTH_SERVICE')
    #                 elif method == 'SignatureValidation':
    #                     request['command'] = 'authenticate'
    #                     self.redirect(request, 'DEVICE_SERVICE')

    #         # Protocol Error
    #         else:
    #             response = {
    #                 'command': 'disconnect',
    #                 'type': 'disconnect',
    #                 'code': PROTOCOL_ERROR}

    #     # Waiting for email info from OAuth
    #     elif not self.conn.get_email():

    #         # Auth (ignore)
    #         if packet_type == 'auth':
    #             response['command'] = 'read'

    #         # Disconnect
    #         elif packet_type == 'disconnect':
    #             response['command'] = 'disconnect'

    #         # Protocol Error
    #         else:
    #             response = {
    #                 'command': 'disconnect',
    #                 'type': 'connack',
    #                 'code': PROTOCOL_ERROR}

    #     # Actions
    #     else:
    #         # Ping
    #         if packet_type == 'pingreq':
    #             response = {
    #                 'command': 'read',
    #                 'type': 'pingresp'}

    #         # Disconnect
    #         elif packet_type == 'disconnect':
    #             response['command'] = 'disconnect'

    #         # Reauthenticate
    #         elif packet_type == 'auth':
    #             code = SUCCESS
    #             method = request.get('properties').get('authentication_method')
    #             if request.get('code') != REAUTHENTICATE:
    #                 code = PROTOCOL_ERROR
    #             elif (not method
    #                   or method != self.conn.get_method()
    #                   or method not in ['OAuth2.0', 'SignatureValidation']):
    #                 code = BAD_AUTHENTICATION_METHOD

    #             if code:
    #                 response = {
    #                     'command': 'disconnect',
    #                     'type': 'disconnect',
    #                     'code': code}
    #             else:
    #                 response['command'] = 'read'
    #                 if method == 'OAuth2.0':
    #                     oauth_request = {
    #                         'command': 'oauth_request',
    #                         'queue': env['QUEUE']}
    #                     self.redirect(oauth_request, 'OAUTH_SERVICE')
    #                 elif method == 'SignatureValidation':
    #                     request['command'] = 'authenticate'
    #                     self.redirect(request, 'DEVICE_SERVICE')

    #         # Subscription managment
    #         elif packet_type in ['publish', 'pubrel', 'subscribe', 'unsubscribe']:
    #             response['command'] = 'read'
    #             request['command'] = request.get('type')
    #             if packet_type == 'publish':
    #                 request['time_received'] = int(time.time()*10**4)
    #             self.redirect(request, 'SUBSCRIPTION_SERVICE')

    #         # Message managment
    #         elif packet_type in ['puback', 'pubrec', 'pubcomp']:
    #             response['command'] = 'read'
    #             self.redirect(request, 'MESSAGE_SERVICE')

    #         # Protocol Error
    #         else:
    #             response = {
    #                 'command': 'disconnect',
    #                 'type': 'disconnect',
    #                 'code': PROTOCOL_ERROR}

    #     return response


    