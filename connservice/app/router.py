import logging
import mailer
import const
from request_uri_client import oauth_service

class Router:
    def __init__(self, sender, packet, addr):
        self.sender = sender
        self.packet = packet
        self.connection = connections.get(addr)
        self.type = packet.get('type', 'reserved')

    def route(self):
        new_packet = {'commands': {}}
        if not self.connection:
            new_packet['type'] = 'connack'
            new_packet['commands']['write'] = True
            new_packet['commands']['disconnect'] = True
            if self.type not in ['connect', 'pingreq']:
                new_packet['code'] = const.PROTOCOL_ERROR
            elif self.type == 'pingreq':
                new_packet['type'] = 'pingresp'
            elif self.type == 'connect':
                method = self.packet['properties'].get('authentication_method')
                if not method or method not in ['OAuth2']:
                    new_packet['code'] = const.BAD_AUTHENTICATION_METHOD
                else:
                    new_packet = self.connect()
        elif not self.connection.verified:
            if self.type not in ['auth', 'disconnect']:
                new_packet['type'] = 'connack'
                new_packet['code'] = const.PROTOCOL_ERROR
                new_packet['commands']['write'] = True
                new_packet['commands']['disconnect'] = True
            elif self.type == 'auth':
                new_packet['commands']['read'] = True
            elif self.type == 'disconnect':
                new_packet['commands']['disconnect'] = True
        else:
            if self.type not in ['pingreq', 'disconnect', 'auth', 'publish',
                                 'puback', 'pubrec', 'pubrel', 'pubcomp',
                                 'subscribe', 'unsubscribe']:
                new_packet['type'] = 'disconnect'
                new_packet['code'] = const.PROTOCOL_ERROR
                new_packet['commands']['write'] = True
                new_packet['commands']['disconnect'] = True
            elif self.type == 'pingreq':
                new_packet['type'] = 'pingresp'
                new_packet['read'] = True
            elif self.type == 'disconnect':
                new_packet['commands']['disconnect'] = True
            elif self.type == 'auth':
                new_packet['commands']['write'] = True
                new_packet['commands']['disconnect'] = True
                if packet.get('code') != const.REAUTHENTICATE:
                    new_packet['code'] = const.PROTOCOL_ERROR
                method = self.packet['properties'].get('authentication_method')
                if (not method
                    or method != self.connection.method
                    or method not in ['OAuth2']):
                    new_packet['code'] = const.BAD_AUTHENTICATION_METHOD
                new_packet = self.reauthenticate()
            else:
                new_packet['commands']['read'] = True
                if self.type in ['subscribe', 'unsubscribe']:
                    self.subscription()
                else:
                    self.publishing()
        return new_packet

    def connect(self):
        addr = self.packet['addr']
        sender = self.packet['response_queue_name']
        client_id = self.packet['client_id']
        clean_start = self.packet['clean_start']
        method = self.packet['properties'].get('authentication_method')

        conn = connection.Connection(addr, sender, client_id, method)
        # TODO persist connection

        auth_data = {
            'user_reference': client_id,
            'method': method,
            'data': self.packet.get('properties').get('authentication_data'),
            'username': self.packet.get('username'),
            'password': self.packet.get('password')
        }
        return self.authenticating(auth_data)

    def reauthenticate(self):
        auth_data = {
            'user_reference': self.connection.id,
            'method': self.packet.get('properties').get('authentication_method'),
            'data': self.packet.get('properties').get('authentication_data'),
        }
        return self.authenticating(auth_data)

    def publish(self):
        message = {
            'client_id': connection.id,
            'qos': self.packet['qos'],
            'retain': self.packet['retain'],
            'duplicate': self.packet['duplicate'],
            'topic': self.packet['topic'],
            'payload': self.packet['payload']
        }
        if message['qos'] > 0:
            message['packet_id'] = self.packet['id']
        if 'payload_format_indicator' in self.packet:
            message['payload_format_indicator']  = self.packet['payload_format_indicator']
        if 'message_expiry' in self.packet:
            message['expiry'] = self.packet['message_expiry']
        if 'content_type' in self.packet:
            message['content_type'] = self.packet['content_type']
        publishing(message)

        return {'read': True}

    def qosing(self):

        return {'read': True}


    def subscribe(self):
        sub_packet = {
            'client_id': self.connection.id,
            'packet_id': self.packet['id'],
            'subscriptions': self.packet['topics']
        }
        if 'subscription_identifier' in packet:
            sub_packet['sub_id'] = packet['subscription_identifier']

        subscribing(sub_packet)

    def unsubscribe(self):
        sub_packet = {
            'client_id': self.connection.id,
            'packet_id': self.packet['id'],
            'subscriptions': self.packet['topics']
        }

        unsubscribing(unsub_packet)

    def authenticating(self, auth_data):
        if auth_data['method'] == 'OAuth2.0'
            request_uri = oauth_service.get_uri(auth_data['user_reference'])
            return {
                'type': 'auth',
                'code': const.CONTINUE_AUTHENTICATION,
                'authentication_method': 'OAuth2.0',
                'authentication_data': request_uri.encode('utf-8'),
                'commands': {'write': True}
            }
        else:
            return {'commands': {'read': True}}

            