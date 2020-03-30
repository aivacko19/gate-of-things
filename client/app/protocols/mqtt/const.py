BYTE = 0
TWO_BYTE_INT = 1
FOUR_BYTE_INT = 2
UTF8_ENCODED_STRING = 3
VARIABLE_BYTE_INT = 4
BINARY_DATA = 5
UTF8_STRING_PAIR = 6

RESERVED = 'reserved'
CONNECT = 'connect'
CONNACK = 'connack'
PUBLISH = 'publish' 
PUBACK = 'puback'
PUBREC = 'pubrec' 
PUBREL = 'pubrel' 
PUBCOMP = 'pubcomp'
SUBSCRIBE = 'subscribe'
SUBACK = 'suback'
UNSUBSCRIBE = 'unsubscribe' 
UNSUBACK = 'unsuback'
PINGREQ = 'pingreq'
PINGRESP = 'pingresp'
DISCONNECT = 'disconnect'
AUTH = 'auth'
WILL = 'will'

TYPE_TRANSLATOR = {
    RESERVED: 0x00,
    CONNECT: 0x10,
    CONNACK: 0x20,
    PUBLISH: 0x30,
    PUBACK: 0x40,
    PUBREC: 0x50,
    PUBREL: 0x60,
    PUBCOMP: 0x70,
    SUBSCRIBE: 0x80,
    SUBACK: 0x90,
    UNSUBSCRIBE: 0xA0,
    UNSUBACK: 0xB0,
    PINGREQ: 0xC0,
    PINGRESP: 0xD0,
    DISCONNECT: 0xE0,
    AUTH: 0xF0,
}

def get_type_str(value):
    return list(TYPE_TRANSLATOR.keys())[
            list(TYPE_TRANSLATOR.values()).index(value)
        ]

def get_type_code(value):
    return TYPE_TRANSLATOR.get(value)

UNSPECIFIED_ERROR = 0x80
MALFORMED_PACKET = 0X81
PROTOCOL_ERROR = 0X82
UNSUPPORTED_PROTOCOL_VERSION = 0x84
PAYLOAD_FORMAT_INVALID = 0x99

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
    PUBREC, PUBREL, PUBCOMP, SUBSCRIBE, SUBACK,
    UNSUBSCRIBE, UNSUBACK, DISCONNECT, AUTH]

DICT = {
    PAYLOAD_FORMAT_INDICATOR: {
        'bool': False,
        'nonzero': False,
        'name': 'payload_format_indicator',
        'type': BYTE,
        'group': pub_will_group
    },
    MESSAGE_EXPIRY_INTERVAL: {
        'bool': False,
        'nonzero': False,
        'name': 'message_expiry_interval',
        'type': FOUR_BYTE_INT,
        'group': pub_will_group
    },
    CONTENT_TYPE: {
        'bool': False,
        'nonzero': False,
        'name': 'content_type',
        'type': UTF8_ENCODED_STRING,
        'group': pub_will_group
    },
    RESPONSE_TOPIC: {
        'bool': False,
        'nonzero': False,
        'name': 'response_topic',
        'type': UTF8_ENCODED_STRING,
        'group': pub_will_group
    },
    CORRELATION_DATA: {
        'bool': False,
        'nonzero': False,
        'name': 'correlation_data',
        'type': BINARY_DATA,
        'group': pub_will_group
    },
    SUBSCRIPTION_IDENTIFIER: {
        'bool': False,
        'nonzero': True,
        'name': 'subscription_identifier',
        'type': VARIABLE_BYTE_INT,
        'group': pub_sub_group
    },
    SESSION_EXPIRY_INTERVAL: {
        'bool': False,
        'nonzero': False,
        'name': 'session_expiry_interval',
        'type': FOUR_BYTE_INT,
        'group': conn_connack_dis_group
    },
    ASSIGNED_CLIENT_IDENTIFIER: {
        'bool': False,
        'nonzero': False,
        'name': 'assigned_client_identifier',
        'type': UTF8_ENCODED_STRING,
        'group': connack_group
    },
    SERVER_KEEP_ALIVE: {
        'bool': False,
        'nonzero': False,
        'name': 'server_keep_alive',
        'type': TWO_BYTE_INT,
        'group': connack_group
    },
    AUTHENTICATION_METHOD: {
        'bool': False,
        'nonzero': False,
        'name': 'authentication_method',
        'type': UTF8_ENCODED_STRING,
        'group': auth_group
    },
    AUTHENTICATION_DATA: {
        'bool': False,
        'nonzero': False,
        'name': 'authentication_data',
        'type': BINARY_DATA,
        'group': auth_group
    },
    REQUEST_PROBLEM_INFORMATION: {
        'bool': True,
        'nonzero': False,
        'name': 'request_problem_information',
        'type': TWO_BYTE_INT,
        'group': conn_group
    },
    WILL_DELAY_INTERVAL: {
        'bool': False,
        'nonzero': False,
        'name': 'will_delay_interval',
        'type': FOUR_BYTE_INT,
        'group': will_group
    },
    REQUEST_RESPONSE_INFORMATION: {
        'bool': True,
        'nonzero': False,
        'name': 'request_response_information',
        'type': TWO_BYTE_INT,
        'group': conn_group
    },
    RESPONSE_INFORMATION: {
        'bool': False,
        'nonzero': False,
        'name': 'response_information',
        'type': UTF8_ENCODED_STRING,
        'group': connack_group
    },
    SERVER_REFERENCE: {
        'bool': False,
        'nonzero': False,
        'name': 'server_reference',
        'type': UTF8_ENCODED_STRING,
        'group': connack_dis_group
    },
    REASON_STRING: {
        'bool': False,
        'nonzero': False,
        'name': 'reason_string',
        'type': UTF8_ENCODED_STRING,
        'group': reason_str_group
    },
    RECEIVE_MAXIMUM: {
        'bool': False,
        'nonzero': True,
        'name': 'receive_maximum',
        'type': TWO_BYTE_INT,
        'group': conn_connack_group
    },
    TOPIC_ALIAS_MAXIMUM: {
        'bool': False,
        'nonzero': False,
        'name': 'topic_alias_maximum',
        'type': TWO_BYTE_INT,
        'group': conn_connack_group
    },
    TOPIC_ALIAS: {
        'bool': False,
        'nonzero': False,
        'name': 'topic_alias',
        'type': TWO_BYTE_INT,
        'group': pub_group
    },
    MAXIMUM_QOS: {
        'bool': True,
        'nonzero': False,
        'name': 'maximum_qos',
        'type': TWO_BYTE_INT,
        'group': connack_group
    },
    RETAIN_AVAILABLE: {
        'bool': True,
        'nonzero': False,
        'name': 'retain_available',
        'type': TWO_BYTE_INT,
        'group': connack_group
    },
    USER_PROPERTY: {
        'bool': False,
        'nonzero': False,
        'name': 'user_property',
        'type': UTF8_STRING_PAIR,
        'group': user_prop_group
    },
    MAXIMUM_PACKET_SIZE: {
        'bool': False,
        'nonzero': True,
        'name': 'maximum_packet_size',
        'type': FOUR_BYTE_INT,
        'group': conn_connack_group
    },
    WILDCARD_SUBSCRIPTION_AVAILABLE: {
        'bool': True,
        'nonzero': False,
        'name': 'wildcard_subscription_available',
        'type': TWO_BYTE_INT,
        'group': connack_group
    },
    SUBSCRIPTION_IDENTIFIER_AVAILABLE: {
        'bool': True,
        'nonzero': False,
        'name': 'subscription_identifier_available',
        'type': TWO_BYTE_INT,
        'group': connack_group
    },
    SHARED_SUBSCRIPTION_AVAILABLE: {
        'bool': True,
        'nonzero': False,
        'name': 'shared_subscription_available',
        'type': TWO_BYTE_INT,
        'group': connack_group
    }
}