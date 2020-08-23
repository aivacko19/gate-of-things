
import os
import logging
import time

import amqp_helper

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
    'OAUTH_URI_SERVICE': None,
    'SUBSCRIPTION_SERVICE': None,
    'MESSAGE_SERVICE': None,
    'DEVICE_SERVICE': None,
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
            response['_command'] = response['command']
            response['command'] = 'main'
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

        packet_type = request.get('type')
        response = {}

        # First packet
        if not self.conn.connected():

            # Ping
            if packet_type == 'pingreq':
                response = {
                    'command': 'disconnect',
                    'type': 'pingresp'}

            # Connect
            elif packet_type == 'connect':
                method = request['properties'].get('authentication_method')
                if not method or method not in ['OAuth2.0', 'SignatureValidation']:
                    response = {
                        'command': 'disconnect',
                        'type': 'connack',
                        'code': BAD_AUTHENTICATION_METHOD}
                else:
                    # No response until verified
                    cid = request['client_id']
                    method = request['properties'].get('authentication_method')
                    self.conn.set_method(method)
                    self.conn.set_clean_start(request.get('clean_start'))
                    if not cid: 
                        self.conn.set_random_id(True)
                    else:
                        self.conn.set_id(cid)
                        self.take_over_session(cid)

                    cid = self.db.add(self.conn)
                    request['command'] = request['type']
                    self.redirect(request, 'MESSAGE_SERVICE')
                    if method == 'OAuth2.0':
                        oauth_request = {
                            'command': 'oauth_request',
                            'queue': env['QUEUE']}
                        self.redirect(oauth_request, 'OAUTH_URI_SERVICE')
                    elif method == 'SignatureValidation':
                        request['command'] = 'authenticate'
                        self.redirect(request, 'DEVICE_SERVICE')

            # Protocol Error
            else:
                response = {
                    'command': 'disconnect',
                    'type': 'disconnect',
                    'code': PROTOCOL_ERROR}

        # Waiting for email info from OAuth
        elif not self.conn.get_email():

            # Auth (ignore)
            if packet_type == 'auth':
                response['command'] = 'read'

            # Disconnect
            elif packet_type == 'disconnect':
                response['command'] = 'disconnect'

            # Protocol Error
            else:
                response = {
                    'command': 'disconnect',
                    'type': 'connack',
                    'code': PROTOCOL_ERROR}

        # Actions
        else:
            # Ping
            if packet_type == 'pingreq':
                response = {
                    'command': 'read',
                    'type': 'pingresp'}

            # Disconnect
            elif packet_type == 'disconnect':
                response['command'] = 'disconnect'

            # Reauthenticate
            elif packet_type == 'auth':
                code = SUCCESS
                method = request.get('properties').get('authentication_method')
                if request.get('code') != REAUTHENTICATE:
                    code = PROTOCOL_ERROR
                elif (not method
                      or method != self.conn.get_method()
                      or method not in ['OAuth2.0', 'SignatureValidation']):
                    code = BAD_AUTHENTICATION_METHOD

                if code:
                    response = {
                        'command': 'disconnect',
                        'type': 'disconnect',
                        'code': code}
                else:
                    response['command'] = 'read'
                    if method == 'OAuth2.0':
                        oauth_request = {
                            'command': 'oauth_request',
                            'queue': env['QUEUE']}
                        self.redirect(oauth_request, 'OAUTH_URI_SERVICE')
                    elif method == 'SignatureValidation':
                        request['command'] = 'authenticate'
                        self.redirect(request, 'DEVICE_SERVICE')

            # Subscription managment
            elif packet_type in ['publish', 'pubrel', 'subscribe', 'unsubscribe']:
                response['command'] = 'read'
                request['command'] = request.get('type')
                if packet_type == 'publish':
                    request['time_received'] = int(time.time()*10**4)
                self.redirect(request, 'SUBSCRIPTION_SERVICE')

            # Message managment
            elif packet_type in ['puback', 'pubrec', 'pubcomp']:
                response['command'] = 'read'
                request['command'] = request['type']
                self.redirect(request, 'MESSAGE_SERVICE')

            # Protocol Error
            else:
                response = {
                    'command': 'disconnect',
                    'type': 'disconnect',
                    'code': PROTOCOL_ERROR}

        return response

    # Receive URI back from OAuth service
    def oauth_uri(self, request, props):
        uri = request.get('uri')
        return {
            'command': 'idle',
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
            return {
                'command': 'disconnect',
                'type': 'disconnect' if self.conn.get_email() else 'connack',
                'code': NOT_AUTHORIZED}

        LOGGER.info('Authentication successful, user id: %s', email)

        # Reauthentication
        if self.conn.get_email():

            # Validate subscriptions only if email is different
            if self.conn.get_email() != email:
                self.conn.set_email(email)
                self.db.update(self.conn)
                response = {
                    'command': 'reauthenticate',
                    'email': email,}
                self.redirect(response, 'SUBSCRIPTION_SERVICE')

            return {
                'command': 'read',
                'type': 'auth',
                'code': SUCCESS,
                'properties': {
                    'authentication_method': 'OAuth2.0',},}

        # First time authenticated
        self.conn.set_email(email)
        self.db.update(self.conn)
        obj = self.rpc(
            obj={'command': 'fetch_session'},
            queue=env['SUBSCRIPTION_SERVICE'],
            correlation_id=cid,)
        email = obj.get('email')

        session_present = bool(email) and not self.conn.get_clean_start()

        command = None
        if not session_present:
            command = 'create_session'
        elif self.conn.get_email() != email:
            command = 'reauthenticate'

        response = {'email': self.conn.get_email(),}
        if not session_present:
            response['command'] = 'create_session'
            self.redirect(response, 'SUBSCRIPTION_SERVICE')
        elif self.conn.get_email() != email:
            response['command'] = 'reauthenticate'
            self.redirect(response, 'SUBSCRIPTION_SERVICE')

        # Subscribing device to its receiving topic
        if self.conn.get_email().endswith('@device'):
            response = {
                'command': 'subscribe',
                'id': 0,
                'topics': [{
                    'filter': 'device/%s/ctrl' % cid,
                    'max_qos': 2,
                    'no_local': False,
                    'retain_handling': 0,
                    'retain_as_published': False}],}
            self.rpc(
                obj=response,
                queue=env['SUBSCRIPTION_SERVICE'],
                correlation_id=cid)

        response = {
            'command': 'read',
            'type': 'connack',
            'code': SUCCESS,
            'session_present': session_present,
            'properties': {}}
        if self.conn.get_random_id():
            response['properties']['assigned_client_identifier'] = self.conn.get_id()

        return response

    # Forward message from other service to client
    def forward(self, request, props):
        request['command'] = 'read'
        return request

    def redirect(self, request, target):
        self.publish(
            obj=request,
            queue=env[target],
            correlation_id=self.conn.get_id())

    def take_over_session(self, cid):
        conn = self.db.get(cid)
        if not conn:
            return
        socket, reply_queue = conn.get_socket()
        response = {
            'command': 'disconnect',
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
    #                     self.redirect(oauth_request, 'OAUTH_URI_SERVICE')
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
    #                     self.redirect(oauth_request, 'OAUTH_URI_SERVICE')
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


    