import logging
from .const import *
from . import stream as stream_module

class MalformedPacketError(Exception):
    def __init__(self, message):
        super().__init__(message)

class ProtocolError(Exception):
    def __init__(self, message):
        super().__init__(message)

class UnsupportedProtocolError(Exception):
    def __init__(self, message):
        super().__init__(message)

#==========================+++++++++++++++++==========================
#                          +++++       +++++
#                          +++++ PARSE +++++
#                          +++++       +++++
#==========================+++++++++++++++++==========================

def read(stream):
    packet = {}
    try:

        # Parse fixed header                                                                // parser.py
        packet['type'], dup, qos, retain = stream.get_header()
        if packet['type'] == RESERVED:
            raise MalformedPacketError("Bad packet type")
        elif packet['type'] == PUBLISH:
            if qos not in range(3): raise MalformedPacketError("Bad QoS")
            packet['dup'], packet['qos'], packet['retain'] = dup, qos, retain
        elif dup or retain or qos != 0: raise MalformedPacketError("Bytes 0-3 reserved")
        stream.get_var_int()

        # Parse variable header and payload
        # Ping packets - no variable header and no payload
        if packet['type'] in [PINGREQ, PINGRESP]: pass
        # Publish packet
        if packet['type'] == PUBLISH: read_pub_packet(packet, stream)
        # Subscription packets
        elif packet['type'] in [SUBSCRIBE, SUBACK, UNSUBSCRIBE, UNSUBACK]: read_sub_packet(packet, stream)
        # Connection packet
        elif packet['type'] == CONNECT: read_connect_packet(packet, stream)
        # Connection acknowledge, publish handshake (QoS > 0), authenticate
        # and disconnect packets - no payload 
        else: 
            if packet['type'] == CONNACK:
                packet['session_present'], reserved = stream.get_connack_flags()
                if reserved != 0: raise MalformedPacketError("Bytes 1-7 reserved")
            if packet['type'] in [PUBACK, PUBREC, PUBREL, PUBCOMP]: packet['id'] = stream.get_int()
            packet['code'] = stream.get_byte()
            packet['properties'] = get_properties(stream, packet['type'])


        logging.info(f"Received {packet['type']} packet type")
    except (MalformedPacketError,
            stream_module.PropertiesError,
            stream_module.MalformedVariableIntegerError,
            stream_module.OutOfBoundsError) as e:
        packet['error'] = MALFORMED_PACKET
        logging.error(repr(e))
    except ProtocolError as e: 
        packet['error'] = PROTOCOL_ERROR
        logging.error(repr(e))
    except UnsupportedProtocolError as e:
        packet['error'] = UNSUPPORTED_PROTOCOL_ERROR
        logging.error(repr(e))
    except Exception as e:
        packet['error'] = UNSPECIFIED_ERROR
        logging.error(repr(e))
    return packet

def read_pub_packet(packet, stream):
    logging.info("Parsing PUB packet")
    packet['topic'] = stream.get_string()
    if packet['qos'] > 0:
        packet['id'] = stream.get_int()
    packet['properties'] = get_properties(stream, packet['type'])
    packet['payload'] = stream.dump()

def read_sub_packet(packet, stream):
    logging.info("Parsing SUB packet")
    packet['id'] = stream.get_int()
    packet['properties'] = get_properties(stream, packet['type'])
    packet['topics'] = list()
    while not stream.empty():
        topic = {}
        if packet['type'] in [SUBSCRIBE, UNSUBSCRIBE]:
            topic['filter'] = stream.get_string()
        if packet['type'] == SUBSCRIBE:
            retain_handling, retain_as_published, \
                no_local, max_qos, reserved = stream.get_sub_flags()
            if reserved != 0:
                raise MalformedPacketError("Bytes 6-7 reserved")
            if max_qos not in range(3):
                raise ProtocolError("Bad QoS")
            if retain_handling not in range(3):
                raise ProtocolError("Bad Retain Handling options")
            topic['max_qos'] = max_qos
            topic['retain_handling'] = retain_handling
            topic['no_local'] = no_local
            topic['retain_as_published'] = retain_as_published
        if packet['type'] in [SUBACK, UNSUBACK]:
            topic['code'] = stream.get_byte()
        packet['topics'].append(topic)

def read_connect_packet(packet, stream):
    logging.info("Parsing CONNECT packet")
    protocol_name = stream.get_string()
    if protocol_name != 'MQTT':
        raise UnsupportedProtocolError(f"Server does not support protocol: {protocol_name}")
    protocol_version = stream.get_byte()
    if protocol_version != 5:
        raise UnsupportedProtocolError(f"Server does not support MQTT version {protocol_version}")
    username_flag, password_flag, retain, qos, \
        will_flag, clean_start, reserved = stream.get_connect_flags()
    if reserved:
        raise MalformedPacketError("Byte 0 reserved")
    if will_flag:
        if qos not in range(3):
            raise MalformedPacketError("Bad QoS")
    else:
        if retain or qos != 0:
            raise MalformedPacketError("Bytes 3-5 reserved")
    packet['clean_start'] = clean_start
    packet['keep_alive'] = stream.get_int()
    packet['properties'] = get_properties(stream, packet['type'])
    packet['client_id'] = stream.get_string()
    if will_flag:
        will_properties = get_properties(stream, WILL)
        topic = stream.get_string()
        payload = stream.get_binary()
        packet['will'] = {
            'qos': qos,
            'retain': retain,
            'topic': topic,
            'payload': payload,
            'properties': will_properties
        }
    if username_flag:
        packet['username'] = stream.get_string()
    if password_flag:
        packet['password'] = stream.get_binary()

def get_properties(stream, packet_type):
    packed_properties = stream.get_properties()
    properties = {}
    for prop in packed_properties:
        code, value = prop[0], prop[1]
        features = DICT[code]

        if packet_type not in features['group']:
            raise MalformedPacketError(f"Packet type ({packet_type})" +
                f" does not support property ({features['name']})")
        if features['bool'] and value not in range(2):
            raise ProtocolError(f"Value other than 0 or 1" + 
                f" for property ({features['name']}) not allowed")
        if features['nonzero'] and value == 0:
            raise ProtocolError(f"Value 0 " + 
                f"for property ({features['name']}) not allowed")

        if (features['name'] == 'user_property' 
            or (features['name'] == 'subscription_identifier' and packet_type == 'publish')):
            if features['name'] not in properties:
                properties[features['name']] = list()
            properties[features['name']].append(value)
        else:
            if features['name'] in properties:
                raise ProtocolError(f"Property ({features['name']})" +
                    f" can't be included more than once")
            properties[features['name']] = value

    if DICT[AUTHENTICATION_DATA]['name'] in properties:
        if DICT[AUTHENTICATION_METHOD]['name'] not in properties:
            raise ProtocolError("Missing property authentication_method" +
            " for property authentication_data")
    return properties

#==========================+++++++++++++++++++==========================
#                          +++++         +++++
#                          +++++ COMPOSE +++++
#                          +++++         +++++
#==========================+++++++++++++++++++==========================

def write(packet, stream):
    packet_type = packet['type']

    if packet_type == PUBLISH:
        write_pub_packet(packet, stream)
        stream.put_header(packet_type, 
            packet.get('dup', False), packet.get('qos', 0), packet.get('retain', False))
        return

    if packet_type in [SUBSCRIBE, SUBACK, UNSUBSCRIBE, UNSUBACK]:
        write_sub_packet(packet, stream)
    elif packet_type == CONNECT:
        write_connect_packet(packet, stream)
    elif packet_type not in [PINGREQ, PINGRESP]:
        if packet_type == CONNACK:
            stream.put_connack_flags(packet.get('session_present', False))
        if packet_type in [PUBACK, PUBREC, PUBREL, PUBCOMP]:
            stream.put_int(packet['id'])
        stream.put_byte(packet["code"])
        put_properties(stream, packet.get('properties', {}))
    stream.put_header(packet_type)

def write_pub_packet(packet, stream):
    stream.put_string(packet['topic'])
    if packet.get('qos', 0) > 0:
        stream.put_int(packet['id'])
    put_properties(stream, packet.get('properties', {}), 'publish')
    stream.append(packet.get('payload', b""))

def write_sub_packet(packet, stream):
    stream.put_int(packet['id'])
    put_properties(stream, packet.get('properties', {}))
    for topic in packet['topics']:
        if packet['type'] in [SUBSCRIBE, UNSUBSCRIBE]:
            stream.put_string(topic['filter'])
        if packet['type'] == SUBSCRIBE:
            stream.put_sub_flags(
                topic.get('retain_handling', 0),
                topic.get('retain_as_published', False),
                topic.get('no_local', False),
                topic.get('max_qos', 2),)
        if packet["type"] in [SUBACK, UNSUBACK]:
            stream.put_byte(topic['code'])

def write_connect_packet(packet, stream):
    stream.put_string('MQTT')
    stream.put_byte(5)
    username_flag = 'username' in packet
    password_flag = 'password' in packet
    will_flag = 'will' in packet
    retain, qos = False, 0
    if will_flag:
        retain = packet['will']['retain']
        qos = packet['will']['qos']
    clean_start = packet.get('clean_start', False)
    stream.put_connect_flags(username_flag, password_flag, 
        retain, qos, will_flag, clean_start)
    stream.put_int(packet.get('keep_alive', 0))
    put_properties(stream, packet.get('properties', {}))
    stream.put_string(packet['client_id'])
    if will_flag:
        put_properties(stream, packet["will"]["properties"])
        stream.put_string(packet["will"]["topic"])
        stream.put_binary(packet["will"]["payload"])
    if username_flag:
        stream.put_string(packet['username'])
    if password_flag:
        stream.put_binary(packet['password'])

def put_properties(stream, unpacked_properties, packet_type='unknown'):
    properties = list()
    for key in unpacked_properties:
        value = unpacked_properties[key]
        features = None
        code = 0
        for mcode in DICT:
            mfeatures = DICT[mcode]
            if mfeatures['name'] == key:
                code = mcode
                features = mfeatures
                break
        if features is None:
            continue
        if (features['name'] == 'user_property' 
            or (features['name'] == 'subscription_identifier' and packet_type == 'publish')):
            for element in value:
                properties.append((code, element))
        else:
            properties.append((code, value))
    stream.put_properties(properties)