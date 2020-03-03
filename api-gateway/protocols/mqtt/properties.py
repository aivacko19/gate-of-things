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

SERVER_REFERENCE = 0X1C # Unique

WILL_DELAY_INTERVAL = 0X18 # Unique

# App Message Info
PAYLOAD_FORMAT_INDICATOR = 0x01 # Unique
MESSAGE_EXPIRY_INTERVAL = 0x02 # Unique
CONTENT_TYPE = 0x03 # Unique

# Request/Response
RESPONSE_TOPIC = 0x08 # Unique
CORRELATION_DATA = 0x09 # Unique
REQUEST_RESPONSE_INFORMATION = 0X19 # Unique, Bool
RESPONSE_INFORMATION = 0X1A # Unique

TOPIC_ALIAS = 0X23 # Unique, Non-zero

SUBSCRIPTION_IDENTIFIER = 0x0B # List, Non-zero

SESSION_EXPIRY_INTERVAL = 0x11 # Unique

REQUEST_PROBLEM_INFORMATION = 0X17 # Unique, Bool

# Server Capabilities (CONNACK)
MAXIMUM_QOS = 0X24 # Unique, 0-1
RETAIN_AVAILABLE = 0X25 # Unique, Bool
WILDCARD_SUBSCRIPTION_AVAILABLE = 0X28 # Unique, Bool
SUBSCRIPTION_IDENTIFIER_AVAILABLE = 0X29 # Unique, Bool
SHARED_SUBSCRIPTION_AVAILABLE = 0X2A # Unique, Bool

ASSIGNED_CLIENT_IDENTIFIER = 0X12 # Unique (If no client_id_in CONNECT)
SERVER_KEEP_ALIVE = 0X13 # Unique (If keep alive in CONNECT too big)
RECEIVE_MAXIMUM = 0X21 # Unique, Non-zero
TOPIC_ALIAS_MAXIMUM = 0X22 # Unique
MAXIMUM_PACKET_SIZE = 0X27 # Unique, Non-zero

# Auth
AUTHENTICATION_METHOD = 0X15 # Unique
AUTHENTICATION_DATA = 0X16 # Unique, Requires Method

REASON_STRING = 0X1F # Unique

USER_PROPERTY = 0X26 # List

pub_group = [PUBLISH]
will_group = [WILL]
pub_will_group = [PUBLISH, WILL]
pub_sub_group = [PUBLISH, SUBSCRIBE]

conn_group = [CONNECT]
connack_group = [CONNACK]
conn_connack_group = [CONNECT, CONNACK]
connack_dis_group = [CONNACK, DISCONNECT]
conn_connack_dis_group = [CONNECT, CONNACK, DISCONNECT]
auth_group = [CONNECT, CONNACK, AUTH]

reason_str_group = [CONNACK, PUBACK, PUBREC, PUBREL, 
    PUBCOMP, SUBACK, UNSUBACK, DISCONNECT, AUTH]
user_prop_group = [CONNECT, CONNACK, PUBLISH, WILL, PUBACK, 
    PUBREC, PUBREL, PUBCOM, SUBSCRIBE, SUBACK,
    UNSUBSCRIBE, UNSUBACK, DISCONNECT, AUTH]

dictionary = {
    PAYLOAD_FORMAT_INDICATOR: {
        'list': False,
        'bool': False,
        'nonzero': False,
        'name': 'payload_format_indicator',
        'type': datatypes.BYTE,
        'group': pub_will_group
    },
    MESSAGE_EXPIRY_INTERVAL: {
        'list': False,
        'bool': False,
        'nonzero': False,
        'name': 'message_expiry_interval',
        'type': datatypes.FOUR_BYTE_INT,
        'group': pub_will_group
    },
    CONTENT_TYPE: {
        'list': False,
        'bool': False,
        'nonzero': False,
        'name': 'content_type',
        'type': datatypes.UTF8_ENCODED_STRING,
        'group': pub_will_group
    },
    RESPONSE_TOPIC: {
        'list': False,
        'bool': False,
        'nonzero': False,
        'name': 'response_topic',
        'type': datatypes.UTF8_ENCODED_STRING,
        'group': pub_will_group
    },
    CORRELATION_DATA: {
        'list': False,
        'bool': False,
        'nonzero': False,
        'name': 'correlation_data',
        'type': datatypes.BINARY_DATA,
        'group': pub_will_group
    },
    SUBSCRIPTION_IDENTIFIER: {
        'list': True,
        'bool': False,
        'nonzero': True,
        'name': 'subscription_identifier',
        'type': datatypes.VARIABLE_BYTE_INT,
        'group': pub_sub_group
    },
    SESSION_EXPIRY_INTERVAL: {
        'list': False,
        'bool': False,
        'nonzero': False,
        'name': 'session_expiry_interval',
        'type': datatypes.FOUR_BYTE_INT,
        'group': conn_connack_dis_group
    },
    ASSIGNED_CLIENT_IDENTIFIER: {
        'list': False
        'bool': False
        'nonzero': False,
        'name': 'assigned_client_identifier',
        'type': datatypes.UTF8_ENCODED_STRING,
        'group': connack_group
    },
    SERVER_KEEP_ALIVE: {
        'list': False,
        'bool': False,
        'nonzero': False,
        'name': 'server_keep_alive',
        'type': datatypes.TWO_BYTE_INT,
        'group': connack_group
    },
    AUTHENTICATION_METHOD: {
        'list': False,
        'bool': False,
        'nonzero': False,
        'name': 'authentication_method',
        'type': datatypes.UTF8_ENCODED_STRING,
        'group': auth_group
    },
    AUTHENTICATION_DATA: {
        'list': False,
        'bool': False,
        'nonzero': False,
        'name': 'authentication_data',
        'type': datatypes.BINARY_DATA,
        'group': auth_group
    },
    REQUEST_PROBLEM_INFORMATION: {
        'list': False,
        'bool': True,
        'nonzero': False,
        'name': 'request_problem_information',
        'type': datatypes.TWO_BYTE_INT,
        'group': conn_group
    },
    WILL_DELAY_INTERVAL: {
        'list': False,
        'bool': False,
        'nonzero': False,
        'name': 'will_delay_interval',
        'type': datatypes.FOUR_BYTE_INT,
        'group': will_group
    },
    REQUEST_RESPONSE_INFORMATION: {
        'list': False,
        'bool': True,
        'nonzero': False,
        'name': 'request_response_information',
        'type': datatypes.TWO_BYTE_INT,
        'group': conn_group
    },
    RESPONSE_INFORMATION: {
        'list': False,
        'bool': False,
        'nonzero': False,
        'name': 'response_information',
        'type': datatypes.UTF8_ENCODED_STRING,
        'group': connack_group
    },
    SERVER_REFERENCE: {
        'list': False,
        'bool': False,
        'nonzero': False,
        'name': 'server_reference',
        'type': datatypes.UTF8_ENCODED_STRING,
        'group': connack_dis_group
    },
    REASON_STRING: {
        'list': False,
        'bool': False,
        'nonzero': False,
        'name': 'reason_string',
        'type': datatypes.UTF8_ENCODED_STRING,
        'group': reason_str_group
    },
    RECEIVE_MAXIMUM: {
        'list': False,
        'bool': False,
        'nonzero': True,
        'name': 'receive_maximum',
        'type': datatypes.TWO_BYTE_INT,
        'group': conn_connack_group
    },
    TOPIC_ALIAS_MAXIMUM: {
        'list': False,
        'bool': False,
        'nonzero': False,
        'name': 'topic_alias_maximum',
        'type': datatypes.TWO_BYTE_INT,
        'group': conn_connack_group
    },
    TOPIC_ALIAS: {
        'list': False,
        'bool': False,
        'nonzero': False,
        'name': 'topic_alias',
        'type': datatypes.TWO_BYTE_INT,
        'group': pub_group
    },
    MAXIMUM_QOS: {
        'list': False,
        'bool': True,
        'nonzero': False,
        'name': 'maximum_qos',
        'type': datatypes.TWO_BYTE_INT,
        'group': connack_group
    },
    RETAIN_AVAILABLE: {
        'list': False,
        'bool': True,
        'nonzero': False,
        'name': 'retain_available',
        'type': datatypes.TWO_BYTE_INT,
        'group': connack_group
    },
    USER_PROPERTY: {
        'list': True,
        'bool': False,
        'nonzero': False,
        'name': 'user_property',
        'type': UTF8_STRING_PAIR,
        'group': user_prop_group
    },
    MAXIMUM_PACKET_SIZE: {
        'list': False,
        'bool': False,
        'nonzero': True,
        'name': 'maximum_packet_size',
        'type': datatypes.FOUR_BYTE_INT,
        'group': conn_connack_group
    },
    WILDCARD_SUBSCRIPTION_AVAILABLE: {
        'list': False,
        'bool': True,
        'nonzero': False,
        'name': 'wildcard_subscription_available',
        'type': datatypes.TWO_BYTE_INT,
        'group': connack_group
    },
    SUBSCRIPTION_IDENTIFIER_AVAILABLE: {
        'list': False,
        'bool': True,
        'nonzero': False,
        'name': 'subscription_identifier_available',
        'type': datatypes.TWO_BYTE_INT,
        'group': connack_group
    },
    SHARED_SUBSCRIPTION_AVAILABLE: {
        'list': False,
        'bool': True,
        'nonzero': False,
        'name': 'shared_subscription_available',
        'type': datatypes.TWO_BYTE_INT,
        'group': connack_group
    }
}

class PropertiesException(Exception):
    def __init__(self, message):
        super().__init__(message)

def unpack(packed_properties, packet_type):
    properties = {}
    for prop in packed_properties:
        code, value = prop[0], prop[1]

        if not dictionary.has_key(code):
            raise PropertiesException(f"Wrong property code {code}")
        features = dictionary[code]

        if packet_type not in features['group']:
            raise PropertiesException(f"Packet type ({packet_type})" +
                f" does not support property ({features['name']})")
        if features['bool'] and value not in range(1):
            raise PropertiesException(f"Value other than 0 or 1" + 
                f" for property ({features['name']}) not allowed")
        if features['nonzero'] and value == 0:
            raise PropertiesException(f"Value 0 " + 
                f"for property ({features['name']}) not allowed")

        if features['list']:
            if not properties.has_key(features['name']):
                properties[features['name']] = list()
            properties[features['name']].append(value)
        else:
            if properties.has_key(features['name']):
                raise PropertiesException(f"Property ({features['name']})" +
                    f" can't be included more than once")
            properties[features['name']] = value
    if properties.has_key(dictionary[AUTHENTICATION_DATA]['name']):
        if not properties.has_key(dictionary[AUTHENTICATION_METHOD]['name']):
            raise PropertiesException("Missing property authentication_method" +
            " for property authentication_data")
    return properties

def pack(unpacked_properties):
    properties = list()
    for key, value in unpacked_properties:
        features = None
        code = 0
        for mcode, mfeatures in dictionary:
            if mfeatures['name'] == key:
                code = mcode
                features = mfeatures
                break
        if features is None:
            raise PropertiesException(f"Unexisting property ({key})")

        if features['list']:
            for element in value:
                properties.append((code, element))
        else:
            properties.append((code, value))

    return properties



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
