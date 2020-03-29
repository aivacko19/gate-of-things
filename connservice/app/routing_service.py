
import os
import logging
import time

import amqp_helper
from db import ConnectionDB

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
    'OAUTH_URI_SERVICE': None,
    'SUBSCRIPTION_SERVICE': None,
    'PUBLISH_SERVICE': None,
    'MESSAGE_SERVICE': None
}

for key in env:
    service = os.environ.get(key)
    if not service:
        raise Exception('Environment variable %s not defined', key)
    env[key] = service

class RoutingService(amqp_helper.AmqpAgent):

    def __init__(self, queue, db):
        self.db = db
        amqp_helper.AmqpAgent.__init__(self, queue)

    def main(self, request, props):

        if request.get('service'):
            del request['service']
            conn = self.db.get(props.correlation_id)
            socket, reply_queue = conn.get_socket()
            self.publish(
                obj=request,
                queue=reply_queue,
                correlation_id=socket)
            return

        reply_queue = props.reply_to
        socket = props.correlation_id

        if request.get('disconnect'):
            LOGGER.info('Disconnecting client %s', socket)
            self.db.delete_by_socket(socket, reply_queue)
            return

        conn = self.db.get_by_socket(socket, reply_queue)
        LOGGER.info('Connection info: id=%s, email=%s', conn.get_id(), conn.get_email())

        command = request.get('type')
        response = {'commands': {}}

        # First packet
        if not conn.connected():
            response['type'] = 'connack'
            response['commands']['write'] = True
            response['commands']['disconnect'] = True

            # Ping
            if command == 'pingreq':
                response['type'] = 'pingresp'

            # Connect
            elif command == 'connect':
                method = request['properties'].get('authentication_method')
                if not method or method not in ['OAuth2.0']:
                    response['code'] = BAD_AUTHENTICATION_METHOD
                else:
                    cid = request['client_id']
                    method = request['properties'].get('authentication_method')
                    conn.set_method(method)
                    conn.set_clean_start(request.get('clean_start'))
                    if not cid: 
                        conn.set_random_id(True)
                    else:
                        conn.set_id(cid)
                        self.take_over_session(self.db, cid)

                    cid = self.db.add(conn)
                    self.redirect(request, conn, 'MESSAGE_SERVICE')
                    if method == 'OAuth2.0':
                        self.redirect({'oauth_request': True}, conn, 'OAUTH_URI_SERVICE')
                    response = {'commands': {}}

            # Protocol Error
            else:
                response['code'] = PROTOCOL_ERROR

        # Waiting for email info from OAuth
        elif not conn.get_email():

            # Auth (ignore)
            if command == 'auth':
                response['commands']['read'] = True

            # Disconnect
            elif command == 'disconnect':
                response['commands']['disconnect'] = True

            # Protocol Error
            else:
                response['type'] = 'connack'
                response['code'] = PROTOCOL_ERROR
                response['commands']['write'] = True
                response['commands']['disconnect'] = True

        # Actions
        else:
            # Ping
            if command == 'pingreq':
                response['type'] = 'pingresp'
                response['commands']['write'] = True
                response['commands']['read'] = True

            # Disconnect
            elif command == 'disconnect':
                response['commands']['disconnect'] = True

            # Reauthenticate
            elif command == 'auth':
                response['commands']['write'] = True
                response['commands']['disconnect'] = True
                if request.get('code') != REAUTHENTICATE:
                    response['code'] = PROTOCOL_ERROR
                method = request['properties'].get('authentication_method')
                if (not method
                    or method != conn.get_method()
                    or method not in ['OAuth2.0']):
                    response['code'] = BAD_AUTHENTICATION_METHOD
                else:
                    if method == 'OAuth2.0':
                        self.redirect({'oauth_request': True}, conn, 'OAUTH_URI_SERVICE')
                    response = {'commands': {}}

            # Subscription managment
            elif command in ['subscribe', 'unsubscribe']:
                self.redirect(request, conn, 'SUBSCRIPTION_SERVICE')
                response['commands']['read'] = True

            # Publishing
            elif command in ['publish', 'pubrel']:
                request['received'] = int(time.time()*10**4)
                self.redirect(request, conn, 'PUBLISH_SERVICE')
                response['commands']['read'] = True

            # Message managment
            elif command in ['puback', 'pubrec', 'pubcomp']:
                self.redirect(request, conn, 'MESSAGE_SERVICE')
                response['commands']['read'] = True

            # Protocol Error
            else:
                response['type'] = 'disconnect'
                response['code'] = PROTOCOL_ERROR
                response['commands']['write'] = True
                response['commands']['disconnect'] = True

        self.publish(
            obj=response,
            queue=props.reply_to,
            correlation_id=props.correlation_id)

    def redirect(self, request, connection, target):
        self.publish(
            obj=request,
            queue=env[target],
            correlation_id=connection.get_id())

    def take_over_session(self, cid):
        conn = self.db.get(cid)
        if conn:
            socket, reply_queue = conn.get_socket()
            response['type'] = 'disconnect',
            response['code'] = SESSION_TAKEN_OVER,
            self.publish(
                obj=response,
                queue=reply_queue,
                correlation_id=socket)
            self.db.delete(cid)