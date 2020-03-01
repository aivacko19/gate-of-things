import datatypes
from datatypes import decode
from datatypes import encode

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
PINGRESP 13
DISCONNECT = 14
AUTH = 15
WILL = 16

PAYLOAD_FORMAT_INDICATOR = 0x01
MESSAGE_EXPIRY_INTERVAL = 0x02
CONTENT_TYPE = 0x03
RESPONSE_TOPIC = 0x08
CORRELATION_DATA = 0x09
SUBSCRIPTION_IDENTIFIER = 0x0B
SESSION_EXPIRY_INTERVAL = 0x11
ASSIGNED_CLIENT_IDENTIFIER = 0X12
SERVER_KEEP_ALIVE = 0X13
AUTHENTICATION_METHOD = 0X15
AUTHENTICATION_DATA = 0X16
REQUEST_PROBLEM_INFORMATION = 0X17
WILL_DELAY_INTERVAL = 0X18
REQUEST_RESPONSE_INFORMATION = 0X19
RESPONSE_INFORMATION = 0X1A
SERVER_REFERENCE = 0X1C
REASON_STRING = 0X1F
RECEIVE_MAXIMUM = 0X21
TOPIC_ALIAS_MAXIMUM = 0X22
TOPIC_ALIAS = 0X23
MAXIMUM_QOS = 0X24
RETAIN_AVAILABLE = 0X25
USER_PROPERTY = 0X26
MAXIMUM_PACKET_SIZE = 0X27
WILDCARD_SUBSCRIPTION_AVAILABLE = 0X28
SUBSCRIPTION_IDENTIFIER_AVAILABLE = 0X29
SHARED_SUBSCRIPTION_AVAILABLE = 0X2A

pub_group = [PUBLISH]
will_group = [WILL]
pub_will_group = [PUBLISH, WILL]
pub_sub_group = [PUBLISH, SUBSCRIBE]

conn_group = [CONNECT]
connack_group = [CONNACK]
conn_connack_group = [CONNECT, CONNACK]
conn_dis_group = [CONNECT, DISCONNECT]
conn_connack_dis_group = [CONNECT, CONNACK, DISCONNECT]
auth_group = [CONNECT, CONNACK, AUTH]

reason_str_group = [CONNACK, PUBACK, PUBREC, PUBREL, 
    PUBCOMP, SUBACK, UNSUBACK, DISCONNECT, AUTH]
user_prop_group = [CONNECT, CONNACK, PUBLISH, WILL, PUBACK, 
    PUBREC, PUBREL, PUBCOM, SUBSCRIBE, SUBACK,
    UNSUBSCRIBE, UNSUBACK, DISCONNECT, AUTH]

property_dict = {
    PAYLOAD_FORMAT_INDICATOR: {
        'type': datatypes.BYTE,
        'group': pub_will_group
    },
    MESSAGE_EXPIRY_INTERVAL: {
        'type': datatypes.FOUR_BYTE_INT,
        'group': pub_will_group
    },
    CONTENT_TYPE: {
        'type': datatypes.UTF8_ENCODED_STRING,
        'group': pub_will_group
    },
    RESPONSE_TOPIC: {
        'type': datatypes.UTF8_ENCODED_STRING,
        'group': pub_will_group
    },
    CORRELATION_DATA: {
        'type': datatypes.BINARY_DATA,
        'group': pub_will_group
    },
    SUBSCRIPTION_IDENTIFIER: {
        'type': datatypes.VARIABLE_BYTE_INT,
        'group': pub_sub_group
    },
    SESSION_EXPIRY_INTERVAL: {
        'type': datatypes.FOUR_BYTE_INT,
        'group': conn_connack_dis_group
    },
    ASSIGNED_CLIENT_IDENTIFIER: {
        'type': datatypes.UTF8_ENCODED_STRING,
        'group': connack_group
    },
    SERVER_KEEP_ALIVE: {
        'type': datatypes.TWO_BYTE_INT,
        'group': connack_group
    },
    AUTHENTICATION_METHOD: {
        'type': datatypes.UTF8_ENCODED_STRING,
        'group': auth_group
    },
    AUTHENTICATION_DATA: {
        'type': datatypes.BINARY_DATA,
        'group': auth_group
    },
    REQUEST_PROBLEM_INFORMATION: {
        'type': datatypes.TWO_BYTE_INT,
        'group': conn_group
    },
    WILL_DELAY_INTERVAL: {
        'type': datatypes.FOUR_BYTE_INT,
        'group': will_group
    },
    REQUEST_RESPONSE_INFORMATION: {
        'type': datatypes.TWO_BYTE_INT,
        'group': conn_group
    },
    RESPONSE_INFORMATION: {
        'type': datatypes.UTF8_ENCODED_STRING,
        'group': connack_group
    },
    SERVER_REFERENCE: {
        'type': datatypes.UTF8_ENCODED_STRING,
        'group': conn_dis_group
    },
    REASON_STRING: {
        'type': datatypes.UTF8_ENCODED_STRING,
        'group': reason_str_group
    },
    RECEIVE_MAXIMUM: {
        'type': datatypes.TWO_BYTE_INT,
        'group': conn_connack_group
    },
    TOPIC_ALIAS_MAXIMUM: {
        'type': datatypes.TWO_BYTE_INT,
        'group': conn_connack_group
    },
    TOPIC_ALIAS: {
        'type': datatypes.TWO_BYTE_INT,
        'group': pub_group
    },
    MAXIMUM_QOS: {
        'type': datatypes.TWO_BYTE_INT,
        'group': connack_group
    },
    RETAIN_AVAILABLE: {
        'type': datatypes.TWO_BYTE_INT,
        'group': connack_group
    },
    USER_PROPERTY: {
        'type': UTF8_STRING_PAIR,
        'group': user_prop_group
    },
    MAXIMUM_PACKET_SIZE: {
        'type': datatypes.FOUR_BYTE_INT,
        'group': conn_connack_group
    },
    WILDCARD_SUBSCRIPTION_AVAILABLE: {
        'type': datatypes.TWO_BYTE_INT,
        'group': connack_group
    },
    SUBSCRIPTION_IDENTIFIER_AVAILABLE: {
        'type': datatypes.TWO_BYTE_INT,
        'group': connack_group
    },
    SHARED_SUBSCRIPTION_AVAILABLE: {
        'type': datatypes.TWO_BYTE_INT,
        'group': connack_group
    }
}

def decode_properties(stream, packet_type):
    properties = {}
    length, index = decode(stream, datatypes.VARIABLE_BYTE_INT)
    stream = stream[index:]
    index += length
    if length != 0:
        while length > 0:
            prop_code, index = decode(stream, datatypes.VARIABLE_BYTE_INT)
            stream = stream[index:]
            length -= index
            if not property_dict.has_key(prop_code):
                raise Exception('Malformed Variable Byte Integer')
            prop = property_dict[prop_code]
            if packet_type not in prop['group']:
                raise Exception('Malformed Variable Byte Integer')

            if prop_code == USER_PROPERTY:
                if not properties.has_key(prop_code):
                    properties[USER_PROPERTY] = list()

                pair, index = decode(stream, datatypes.UTF8_STRING_PAIR)
                stream = stream[index:]
                length -= index
                properties[USER_PROPERTY].append(pair)
            else:
                if properties.has_key(prop_code):
                    raise Exception('Malformed Variable Byte Integer')

                data, index = decode(stream, prop['type'])
                stream = stream[index:]
                length -= index
                properties[prop_code] = data
    return properties, index

def encode_properties(properties, packet_type):
    stream = b""
    for prop_code, value in properties:
        if not property_dict.has_key(prop_code):
            raise Exception('Malformed Variable Byte Integer')
        prop = property_dict[prop_code]
        if packet_type not in prop['group']:
            raise Exception('Malformed Variable Byte Integer')
        if prop_code == USER_PROPERTY:
            for string_pair in value:
                stream += encode(prop_code, datatypes.VARIABLE_BYTE_INT)
                stream += encode(string_pair, datatypes.UTF8_STRING_PAIR)
        else:
            stream += encode(prop_code, datatypes.VARIABLE_BYTE_INT)
            stream += encode(value, prop['type'])
    length = encode(len(stream), datatypes.VARIABLE_BYTE_INT)
    stream = length + stream
    return stream
