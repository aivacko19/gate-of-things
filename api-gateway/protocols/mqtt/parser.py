import logging

RESERVED = 0
CONNECT = 1
CONNACK = 2
PUBLISH = 3
PUBACK = 4
PUBREC = 5
PUBREL = 6
PUBCOMP = 7
SUBSCRIBE = 8
SUBACK = 9
UNSUBSCRIBE = 10
UNSUBACK = 11
PINGREQ = 12
PINGRESP = 13
DISCONNECT = 14
AUTH = 15
WILL = 16

class MalformedPacketException(Exception):
    def __init__(self, message):
        super().__init__(message)

def read(stream):
    packet = {}
    try:
        packet_type, dup, qos, retain = stream.get_header()
        stream.get_var_int()
        if packet_type == RESERVED:
            raise MalformedPacketException("Bad packet type")
        packet['type'] = packet_type

        if packet_type == PUBLISH:
            if qos not in range(2):
                raise MalformedPacketException("Bad QoS")
            packet['dup'] = dup
            packet['qos'] = qos
            packet['retain'] = retain
            read_pub_packet(stream)
            return packet

        if dup or retain or qos != 0:
            raise MalformedPacketException("Bytes 0-3 reserved")
        if packet_type in [SUBSCRIBE, SUBACK, UNSUBSCRIBE, UNSUBACK]:
            read_sub_packet(stream)
        elif packet_type == CONNECT:
            read_connect_packet(stream)
        elif packet_type not in [PINGREQ, PINGRESP]:
            if packet_type == CONNACK:
                packet['session_present'], reserved = stream.get_connack_flags()
                if reserved != 0:
                    raise MalformedPacketException("Bytes 1-7 reserved")
            if packet_type in [PUBACK, PUBREC, PUBREL, PUBCOMP]:
                packet['id'] = stream.get_int()
            packet['code'] = stream.get_byte()
            packet['properties'] = stream.get_properties(packet['type'])
    except Exception as e:
        logging.error(repr(e))
        packet = {'type': 0}
    return packet

def read_pub_packet(packet, stream):
    packet['topic'] = stream.get_string()
    if qos > 0:
        packet['id'] = stream.get_int()
    packet['properties'] = stream.get_properties(packet['type'])
    packet['payload'] = stream.dump()

def read_sub_packet(packet, stream):
    packet['id'] = stream.get_int()
    packet['properties'] = stream.get_properties(packet['type'])
    packet['topics'] = list()
    while not stream.empty():
        topic = {}
        if packet['type'] in [SUBSCRIBE, UNSUBSCRIBE]:
            topic['filter'] = stream.get_string()
        if packet['type'] == SUBSCRIBE:
            retain_handling, retain_as_published, \
                no_local, max_qos, reserved = stream.get_pub_flags()
            if reserved != 0:
                raise MalformedPacketException("Bytes 6-7 reserved")
            if max_qos not in range(2):
                raise MalformedPacketException("Bad QoS")
            if retain_handling not in range(2):
                raise MalformedPacketException("Bad Retain Handling options")
            topic['max_qos'] = max_qos
            topic['retain_handling'] = retain_handling
            topic['no_local'] = no_local
            topic['retain_as_published'] = retain_as_published
        if packet['type'] in [SUBACK, UNSUBACK]:
            topic['code'] = stream.get_byte()
        packet['topics'].append(topic)

def read_connect_packet(packet, stream):
    packet['protocol_name'] = stream.get_string()
    packet['protocol_version'] = stream.get_byte()
    username_flag, password_flag, retain, qos, \
        will_flag, clean_start, reserved = stream.get_connect_flags()
    if reserved:
        raise MalformedPacketException("Byte 0 reserved")
    if will_flag:
        if qos not in range(2):
            raise MalformedPacketException("Bad QoS")
    else:
        if retain or qos != 0:
            raise MalformedPacketException("Bytes 3-5 reserved")
    packet['keep_alive'] = stream.get_int()
    packet['properties'] = stream.get_properties(packet['type'])
    packet['client_id'] = stream.get_string()
    if will_flag:
        packet['properties'] = stream.get_properties(WILL)
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

def write(packet, stream):
    packet_type = packet['type']

    if packet_type == PUBLISH:
        write_pub_packet(stream)
        stream.put_header(packet_type, 
            packet['dup'], packet['qos'], packet['retain'])
        return

    if packet_type in [SUBSCRIBE, SUBACK, UNSUBSCRIBE, UNSUBACK]:
        write_sub_packet(stream)
    elif packet_type == CONNECT:
        write_connect_packet(stream)
    elif packet_type not in [PINGREQ, PINGRESP]:
        if packet_type == CONNACK:
            stream.put_connack_flags(packet['session_present'])
        if packet_type in [PUBACK, PUBREC, PUBREL, PUBCOMP]:
            stream.put_int(packet['id'])
        stream.put_byte(packet["code"])
        stream.put_properties(packet["properties"])
    stream.put_header(packet_type)

def write_pub_packet(packet, stream):
    stream.put_string(packet['topic'])
    if packet['qos'] > 0:
        stream.put_int(packet['id'])
    stream.put_properties(packet["properties"])
    stream.append(packet["payload"])

def write_sub_packet(packet, stream):
    stream.put_int(packet['id'])
    stream.put_properties(packet["properties"])
    for topic in packet['topics']:
        if packet['type'] in [SUBSCRIBE, UNSUBSCRIBE]:
            stream.put_string(topic['filter'])
        if packet['type'] == SUBSCRIBE:
            stream.put_pub_flags(topic['retain_handling'], topic['retain_as_published'],
                topic['no_local'], topic['max_qos'])
        if packet["type"] in [SUBACK, UNSUBACK]:
            stream.put_byte(topic['code'])
        packet['topics'].append(topic)

def write_connect_packet(packet, stream):
    stream.put_string(packet['protocol_name'])
    stream.put_byte(packet['protocol_version'])
    username_flag = packet.has_key('username')
    password_flag = packet.has_key('password')
    will_flag = packet.has_key('will')
    retain, qos = False, 0
    if will_flag:
        retain = packet['will']['retain']
        qos = packet['will']['qos']
    clean_start = packet.has_key('clean_start')
    if clean_start:
        clean_start = packet['clean_start']
    stream.put_connack_flags(username_flag, password_flag, 
        will_retain, will_qos, will_flag, clean_start)
    stream.put_int(packet['keep_alive'])
    stream.put_properties(packet["properties"])
    stream.put_string(packet['client_id'])
    if will_flag:
        stream.put_properties(packet["will"]["properties"])
        stream.put_string(packet["will"]["topic"])
        stream.put_binary(packet["will"]["payload"])
    if username_flag:
        stream.put_string(packet['username'])
    if password_flag:
        stream.put_binary(packet['password'])