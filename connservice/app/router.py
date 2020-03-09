import logging
import mailer
import const

def route(sender, packet):

    client_addr = packet['addr']

    if 'disconnect' in packet:
        if packet['disconnect']:
            logging.info(f"Disconnecting client {client_addr}")
            disconnecting(client_addr, 1)
            return

    ptype = packet.get('type', 'reserved')

    if 'error' in packet:
        logging.info("Error {} in the packet".format(packet['error']))
        new_packet = {
            'write': True,
            'read': False,
            'code': packet['error']
        }
        if ptype == 'connect':
            new_packet['type'] = 'connack'
        else:
            new_packet['type'] = 'disconnect'
        sender.send(new_packet)

        disconnecting(client_addr, 1)
        return

    if ptype in ['connack', 'suback', 'unsuback', 'pingresp']:
        new_packet = {
            'type': 'disconnect',
            'code': const.PROTOCOL_ERROR,
            'write': True,
            'read': False
        }
        sender.send(new_packet)
        disconnecting(client_addr, 1)
        return

    connection = connections.get(client_addr)

    if ptype == 'pingreq':
        new_packet = {
            'type' = 'pingresp',
            'write' = True
        }
        if connection:
            new_packet['read'] = True
        sender.send(new_packet)
        return

    if not connection:
        if ptype != 'connect':
            new_packet = {
                'type': 'connack',
                'code': const.PROTOCOL_ERROR,
                'write': True,
                'read': False
            }
            sender.send(new_packet)
            disconnecting(client_addr, 1)
            return

    if ptype == 'disconnect':
        disconnecting(client_addr, packet['code'])
        return

    if not connection.is_authenticated():
        if ptype != 'auth':
            new_packet = {
                'type': 'connack',
                'code': const.PROTOCOL_ERROR,
                'write': True,
                'read': False
            }
            sender.send(new_packet)
            disconnecting(client_addr, 1)
            return

    if ptype in ['connect', 'auth']:
        new_packet = {
            'type': 'disconnect',
            'code': const.PROTOCOL_ERROR,
            'write': True,
            'read': False
        }
        if ptype == 'connect':
            new_packet['type'] = 'connack'
        sender.send(new_packet)
        disconnecting(client_addr, 1)
        return

    if ptype == 'connect':
        connect(sender, packet)
    elif ptype == 'auth':
        authenticate(connection, packet)
    elif ptype == 'publish':
        publish(connection, packet)
    elif ptype in ['puback', 'pubrec', 'pubrel', 'pubcomp']:
        messaging(connection, packet)
    elif ptype in ['subscribe', 'unsubscribe']:
        subscribing(connection, packet)

def connect(sender, packet):
    addr = packet['addr']
    client_id = packet['client_id']
    clean_start = packet['clean_start']

    auth_data = {}
    if 'username' in packet:
        auth_data['username'] = packet['username']
    if 'password' in packet:
        auth_data['password'] = packet['password']
    if 'authentication_method' in packet['properties']:
        auth_data['method'] = packet['properties']['authentication_method']
    if 'authentication_data' in packet['properties']:
        auth_data['data'] = packet['properties']['authentication_data']

    will = None
    will_delay = 0
    if 'will' in packet:
        will = packet['will']
        will_delay = packet['will']['properties']['will_delay_interval']

    keep_alive = packet['keep_alive']
    session_expiry = 0
    if 'session_expiry' in packet['properties']:
        session_expiry = packet['properties']['session_expiry_interval']

    conn = connection.Connection(addr, sender, client_id, clean_start, will,
                                 will_delay, keep_alive, session_expiry, auth_data)
    # TODO persist connection

    auth_data['id'] = client_id

    authentication(auth_data)

def authenticate(connection, packet):
    auth_data = {}
    auth_data['method'] = packet['properties']['authentication_method']
    if 'authentication_data' in packet:
        auth_data['data'] = packet['properties']['authentication_data']
    auth_data['id'] = connection.id

    authentication(auth_data)


