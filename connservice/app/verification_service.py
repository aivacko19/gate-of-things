
import os
import logging

import amqp_helper

SUCCESS = 0x00
NOT_AUTHORIZED = 0x87

LOGGER = logging.getLogger(__name__)
LOG_FORMAT = ('%(levelname) -10s %(asctime)s %(name)s %(funcName)s %(lineno)d: %(message)s')
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)

env = {
    'SESSION_SERVICE': None
}

for key in env:
    service = os.environ.get(key)
    if not service:
        raise Exception('Environment variable %s not defined', key)
    env[key] = service

class VerificationService(amqp_helper.AmqpAgent):

    def __init__(self, queue, db):
        self.db = db
        amqp_helper.AmqpAgent.__init__(self, queue)

    def main(self, request, props):

        LOGGER.info("Start verification")

        email = request.get('email')
        cid = props.correlation_id
        conn = self.db.get(cid)
        response = {'commands': {'write': True}, 'properties': {}}

        LOGGER.info('Client Info ::: email: %s, client_id: %s', email, cid)

        if request.get('oauth'):
            if email:
                if not conn.get_email():
                    conn.set_email(email)
                    self.db.update(conn)
                    self.publish(
                        obj={'command': 'get'},
                        queue=env['SESSION_SERVICE'],
                        correlation_id=conn.get_id())
                    return

                response['type'] = 'auth'
                response['code'] = SUCCESS
                response['commands']['read'] = True
                response['properties']['authentication_method'] = 'OAuth2.0'

                # Validate subscriptions only if email is different
                if conn.get_email() != email:
                    conn.set_email(email)
                    self.db.update(conn)
                    self.publish(
                        obj={'command': 'reauth',
                             'email': email,},
                        queue=env['SESSION_SERVICE'],
                        correlation_id=conn.get_id())

            else:
                LOGGER.info('Authentication failed, email not verified')

                response['type'] = 'disconnect' if conn.get_email() else 'connack'
                response['code'] = NOT_AUTHORIZED
                response['commands']['disconnect'] = True


        if request.get('session'):
            response['type'] = 'connack'
            response['code'] = SUCCESS
            response['session_present'] = not (not email) and not conn.get_clean_start()
            if conn.get_random_id():
                response['properties']['assigned_client_identifier'] = conn.get_id()

            command = None
            if not response['session_present']:
                command = 'add'
            elif conn.get_email() != email:
                command = 'reauth'

            if command:
                self.publish(
                    obj={'command': command,
                         'email': conn.get_email(),},
                    queue=env['SESSION_SERVICE'],
                    correlation_id=conn.get_id())

        LOGGER.info('Responding with: %s', response)

        socket, reply_queue = conn.get_socket()
        self.publish(
            obj=response,
            queue=reply_queue,
            correlation_id=socket,)