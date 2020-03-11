import logging
import mailer
import const
import oauth_request_uri

class Router:
    def __init__(self, sender, packet, addr):
        self.sender = sender
        self.packet = packet
        self.connection = connections.get(addr)
        self.type = packet.get('type', 'reserved')

    def disconnect(self):
        if 'disconnect' in self.packet:
            if self.packet['disconnect']:
                logging.info(f"Disconnecting client {self.packet['addr']}")
                if self.connection:
                    connections.delete(self.connection)
                return True

        if 'error' in self.packet:
            logging.info("Error {} in the packet".format(self.packet['error']))
            self.send_disconnect(self.packet['error'])
            return True

        if self.type in ['connack', 'suback', 'unsuback', 'pingresp']:
            logging.info(f"Unsupported packet type: {self.type}")
            self.send_disconnect(const.PROTOCOL_ERROR)
            return True

        if not self.connection and self.type != 'connect':
            self.send_disconnect(const.PROTOCOL_ERROR)
            return True

        if self.type == 'disconnect':
            if self.connection:
                normal_disconnection = self.packet['code'] == const.SUCCESS
                connections.delete(self.connection, normal_disconnection)
            return True

        if not self.connection.is_authenticated():
            return True

        if self.connection and self.type == 'connect':
            self.send_disconnect(const.PROTOCOL_ERROR)
            return True

        return False

    def send_disconnect(self, reason_code):
        new_packet = {
            'code': reason_code,
            'write': True,
            'read': False
        }
        if self.type == 'connect':
            new_packet['type'] = 'connack'
        else:
            new_packet['type'] = 'disconnect'
        self.sender.send(new_packet)
        if self.connection:
            connections.delete(self.connection)

    def send_ack(self):
        new_packet = {
            'write': False,
            'read': True
        }
        self.sender.send(new_packet)


    def route(self):
        if self.disconnect():
            return 

        if ptype == 'pingreq':
            self.ping()
        elif ptype == 'connect':
            self.connect()
        elif ptype == 'auth':
            self.reauthenticate()
        elif ptype == 'publish':
            self.publish()
        elif ptype in ['puback', 'pubrec', 'pubrel', 'pubcomp']:
            self.qosing()
        elif ptype == 'subscibe':
            self.subscribe()
        elif ptype == 'unsubscribe':
            self.unsubscribe()
        else:
            send_disconnect(self, const.MALFORMED_PACKET)

    def ping(self):
        new_packet = {
            'type' = 'pingresp',
            'write' = True
        }
        if connection:
            new_packet['read'] = True
        self.sender.send(new_packet)

    def connect(self):
        addr = self.packet['addr']
        sender = self.packet['response_queue_name']
        client_id = self.packet['client_id']
        clean_start = self.packet['clean_start']

        auth_data = {}
        if 'username' in self.packet:
            auth_data['username'] = self.packet['username']
        if 'password' in self.packet:
            auth_data['password'] = self.packet['password']
        if 'authentication_method' in self.packet['properties']:
            auth_data['method'] = self.packet['properties']['authentication_method']
        if 'authentication_data' in self.packet['properties']:
            auth_data['data'] = self.packet['properties']['authentication_data']

        will = None
        will_delay = 0
        if 'will' in self.packet:
            will = self.packet['will']
            will_delay = self.packet['will']['properties']['will_delay_interval']

        keep_alive = self.packet['keep_alive']
        session_expiry = 0
        if 'session_expiry' in self.packet['properties']:
            session_expiry = self.packet['properties']['session_expiry_interval']

        conn = connection.Connection(addr, sender, client_id, clean_start, will,
                                     will_delay, keep_alive, session_expiry, auth_data)
        # TODO persist connection

        auth_data['id'] = client_id


        self.authenticating(auth_data)

    def reauthenticate(self):
        code = self.packet['code']
        if code != const.REAUTHENTICATE:
            self.send_disconnect(const.PROTOCOL_ERROR)

        auth_data = {}
        auth_data['method'] = self.packet['properties']['authentication_method']
        if 'authentication_data' in self.packet:
            auth_data['data'] = self.packet['properties']['authentication_data']
        auth_data['id'] = connection.id

        if self.connection.method != auth_data['method']:
            self.send_disconnect(const.BAD_AUTHENTICATION_METHOD)

        self.authenticating(auth_data)

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
            if self.packet['payload_format_indicator'] == 1:
                try:
                    message['payload'].decode('utf-8')
                except UnicodeDecodeError:
                    self.send_disconnect(const.PAYLOAD_FORMAT_INVALID)
                    return
        if 'message_expiry' in self.packet:
            message['expiry'] = self.packet['message_expiry']
        if 'content_type' in self.packet:
            message['content_type'] = self.packet['content_type']
        self.send_ack()
        publishing(message)

    def qosing(self):
        self.send_ack()
        return

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
            request_uri = oauth_request_uri.get_request_uri(self.sender.channel, auth_data['id'])
            new_packet = {
                'type': 'auth',
                'code': const.CONTINUE_AUTHENTICATION,
                'authentication_method': 'OAuth2.0',
                'authentication_data': request_uri.encode('utf-8'),
                'write': True,
                'read': False, 
                'wait': True
            }
            self.sender.send(new_packet)
        else:
            new_packet = {
                'type': 'connack',
                'code': const.BAD_AUTHENTICATION_METHOD,
                'write': True,
                'read': False
            }
            if self.type == 'auth':
                new_packet['type'] = 'disconnect'
            self.sender.send(new_packet)













