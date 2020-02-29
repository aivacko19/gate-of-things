import sys
import json
import io
import struct

class MessageFactory:
    def __init__(self):
        self.port = 1887

    def get_port():
        return self.port

    def create(fd, addr):
        return Message(fd, addr)

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

BYTE = 1
TWO_BYTE_INT = 2
FOUR_BYTE_INT = 3
UTF8_ENCODED_STRING = 4
VARIABLE_BYTE_INT = 5
BINARY_DATA = 6
UTF8_STRING_PAIR = 7

pub_group = [PUBLISH]
pub_will_group = [PUBLISH, WILL]
pub_sub_group = [PUBLISH, SUBSCRIBE]
conn_group = [CONNECT]
connack_group = [CONNACK]
conn_connack_group = [CONNECT, CONNACK]
conn_connack_auth_group = [CONNECT, CONNACK, AUTH]
conn_connack_dis_group = [CONNECT, CONNACK, DISCONNECT]
conn_dis_group = [CONNECT, DISCONNECT]
ack_group = [CONNACK, PUBACK, PUBREC, PUBREL, PUBCOMP, SUBACK, UNSUBACK, DISCONNECT, AUTH]
will_group = [WILL]

properties_dictionary = {
    PAYLOAD_FORMAT_INDICATOR: {
        'type': BYTE,
        'group': pub_will_group
    },
    MESSAGE_EXPIRY_INTERVAL: {
        'type': FOUR_BYTE_INT,
        'group': pub_will_group
    },
    CONTENT_TYPE: {
        'type': UTF8_ENCODED_STRING,
        'group': pub_will_group
    },
    RESPONSE_TOPIC: {
        'type': UTF8_ENCODED_STRING,
        'group': pub_will_group
    },
    CORRELATION_DATA: {
        'type': BINARY_DATA,
        'group': pub_will_group
    },
    SUBSCRIPTION_IDENTIFIER: {
        'type': VARIABLE_BYTE_INT,
        'group': pub_sub_group
    },
    SESSION_EXPIRY_INTERVAL: {
        'type': FOUR_BYTE_INT,
        'group': conn_connack_dis_group
    },
    ASSIGNED_CLIENT_IDENTIFIER: {
        'type': UTF8_ENCODED_STRING,
        'group': connack_group
    },
    SERVER_KEEP_ALIVE: {
        'type': TWO_BYTE_INT,
        'group': connack_group
    },
    AUTHENTICATION_METHOD: {
        'type': UTF8_ENCODED_STRING,
        'group': conn_connack_auth_group
    },
    AUTHENTICATION_DATA: {
        'type': BINARY_DATA,
        'group': conn_connack_auth_group
    },
    REQUEST_PROBLEM_INFORMATION: {
        'type': BYTE,
        'group': conn_group
    },
    WILL_DELAY_INTERVAL: {
        'type': FOUR_BYTE_INT,
        'group': will_group
    },
    REQUEST_RESPONSE_INFORMATION: {
        'type': BYTE,
        'group': conn_group
    },
    RESPONSE_INFORMATION: {
        'type': UTF8_ENCODED_STRING,
        'group': connack_group
    },
    SERVER_REFERENCE: {
        'type': UTF8_ENCODED_STRING,
        'group': conn_dis_group
    },
    REASON_STRING: {
        'type': UTF8_ENCODED_STRING,
        'group': ack_group
    },
    RECEIVE_MAXIMUM: {
        'type': TWO_BYTE_INT,
        'group': conn_connack_group
    },
    TOPIC_ALIAS_MAXIMUM: {
        'type': TWO_BYTE_INT,
        'group': conn_connack_group
    },
    TOPIC_ALIAS: {
        'type': TWO_BYTE_INT,
        'group': pub_group
    },
    MAXIMUM_QOS: {
        'type': BYTE,
        'group': connack_group
    },
    RETAIN_AVAILABLE: {
        'type': BYTE,
        'group': connack_group
    },
    MAXIMUM_PACKET_SIZE: {
        'type': FOUR_BYTE_INT,
        'group': conn_connack_group
    },
    WILDCARD_SUBSCRIPTION_AVAILABLE: {
        'type': BYTE,
        'group': connack_group
    },
    SUBSCRIPTION_IDENTIFIER_AVAILABLE: {
        'type': BYTE,
        'group': connack_group
    },
    SHARED_SUBSCRIPTION_AVAILABLE: {
        'type': BYTE,
        'group': connack_group
    }
}

class Message:
    def __init__(self, fd, addr):
        self.fd = fd
        self.addr = addr
        self._buffer = b""
        self._jsonheader_len = None
        self.jsonheader = None
        self.request = None
        self.request_created = False
        self.response_created = False
        self._packetloaded = False
        self._length = None
        self._type = None
        self._packet = {}


    def _json_encode(self, obj, encoding):
        return json.dumps(obj, ensure_ascii=False).encode(encoding)

    def _json_decode(self, json_bytes, encoding):
        tiow = io.TextIOWrapper(
            io.BytesIO(json_bytes), encoding=encoding, newline=""
        )
        obj = json.load(tiow)
        tiow.close()
        return obj

    def _decode(self, stream, data_type):
        func_map = {
            BYTE: self._byte_decode,
            TWO_BYTE_INT: self._two_byte_int_decode,
            FOUR_BYTE_INT: self._four_byte_int_decode,
            UTF8_ENCODED_STRING: self._utf8_encoded_string_decode,
            VARIABLE_BYTE_INT: self._variable_byte_int_decode,
            BINARY_DATA: self._binary_data_decode,
            UTF8_STRING_PAIR: self._utf8_string_pair_decode
        }
        if not func_map.has_key(data_type):
            return -1, -1
        return func_map[data_type](stream)

    def _byte_decode(self, stream):
        if len(stream) < 1:
            raise Exception('Malformed Variable Byte Integer')
        value = stream[0]
        return value, 1

    def _two_byte_int_decode(self, stream):
        if len(stream) < 2:
            raise Exception('Malformed Variable Byte Integer')
        value = struct.unpack(">H", stream[:2])[0]
        return value, 2

    def _four_byte_int_decode(self, stream):
        if len(stream) < 4:
            raise Exception('Malformed Variable Byte Integer')
        value = struct.unpack(">I", stream[:4])[0]
        return value, 4

    def _utf8_encoded_string_decode(self, stream):
        string_length, index = self._two_byte_int_decode(stream)
        if len(stream) < index + string_length:
            raise Exception('Malformed Variable Byte Integer')
        value = stream[index:(string_length+index)].decode("utf-8")
        return value, string_length + index

    def _variable_byte_int_decode(self, stream):
        multiplier = 1
        value = 0
        index = 0
        while len(stream) >= index + 1:
            encoded_byte = stream[index]
            value += (encoded_byte & 127) * multiplier
            if multiplier > 128*128*128:
                raise Exception('Malformed Variable Byte Integer')
            multiplier *= 128
            index += 1
            if encoded_byte & 128 == 0:
                return value, index
        return -1, -1

    def _binary_data_decode(self, stream):
        data_length, index = self._two_byte_int_decode(stream)
        if len(stream) < index + string_length:
            raise Exception('Malformed Variable Byte Integer')
        value = stream[index:(string_length+index)]
        return value, string_length + index

    def _utf8_string_pair_decode(self, stream):
        key, index1 = self._utf8_string_pair_decode(stream)
        value, index2 = self._utf8_string_pair_decode(stream[index1:])
        return (key, value), index1 + index2

    def read(self, data):
        self._buffer += data

        if not self._type is None:
            self.process_fix_header()

        if self._type is not None:
            if self._length is None:
                self.process_length()

        if self._length is not None:
            if not self._packetloaded:
                self.load_packet()

        if self._packetloaded:
            self.process_var_header()
            self.process_payload()
            self.process_request()
            return True

        return False

    def process_request():
        if self._type == CONNECT:


    def process_fix_header(self):
        if len(self._buffer) == 0:
            return
        value, index = self._decode(self._buffer, BYTE)
        self._buffer = self._buffer[index:]

        flags = value & 0x0F
        self._type = value >> 4
        if self._type == RESERVED:
            raise Exception('Malformed Variable Byte Integer')
        if self._type != PUBLISH and flags != 0:
            raise Exception('Malformed Variable Byte Integer')
        if self._type == PUBLISH:
            self._packet['dup'] = flags & 0x08 != 0
            self._packet['qos'] = (flags >> 1) & 0x03
            if self._packet['qos'] == 0x03:
                raise Exception('Malformed Variable Byte Integer')
            self._packet['retain'] = flags & 0x01 != 0

    def process_length(self):
        value, index = self._decode(self._buffer, VARIABLE_BYTE_INT)
        if index < 0:
            return
        self._buffer = self._buffer[index:]
        self._length = value

    def load_packet(self):
        if len(self._buffer) < self._length:
            return
        if len(self._buffer) > self._length:
            raise Exception('Malformed Variable Byte Integer')
        self._packetloaded = True

    def process_var_header(self):
        if self._type & 0x0E == 0x0C:
            return
        self.process_connect()
        self.process_connack()
        self.process_topic()
        self.process_id()
        self.process_code()
        self.process_properties()

    def process_payload(self):
        if self._type == CONNECT:
            self.process_credentials()
        elif self._type == PUBLISH:
            self.process_app_message()
        elif self._type in [SUBSCRIBE, UNSUBSCRIBE]:
            self.process_topics()
        elif self._type in [SUBACK, UNSUBACK]:
            self.process_topic_codes()

        if self._length != 0:
            raise Exception('Malformed Variable Byte Integer')

    def process_connect(self):
        if self._type != CONNECT:
            return

        value, index = self._decode(self._buffer, UTF8_ENCODED_STRING)
        self._buffer = self._buffer[index:]
        self._length -= index
        self._packet['protocol_name'] = value
        value, index = self._decode(self._buffer, BYTE)
        self._buffer = self._buffer[index:]
        self._length -= index
        self._packet['protocol_version'] = value

        value, index = self._decode(self._buffer, BYTE)
        self._buffer = self._buffer[index:]
        self._length -= index

        if value & 0x04 != 0:
            self._packet['will'] = {
                'qos': (value >> 3) & 0x03,
                'retain': value & 0x20 != 0
            }
            if self._packet['will']['qos'] == 3:
                raise Exception('Malformed Variable Byte Integer')
        else:
            if (value >> 3) & 0x03 != 0 or value & 0x20 != 0:
                raise Exception('Malformed Variable Byte Integer')

        self._packet['clean_start'] = value & 0x02 != 0
        self._packet['password_flag'] = value & 0x40 != 0
        self._packet['username_flag'] = value & 0x80 != 0
        if value & 0x01 != 0:
            raise Exception('Malformed Variable Byte Integer')

        value, index = self._decode(self._buffer, TWO_BYTE_INT)
        self._buffer = self._buffer[index:]
        self._length -= index
        self._packet['keep_alive'] = value

    def process_connack(self):
        if self._type != CONNACK:
            return
        value, index = self._decode(self._buffer, BYTE)
        self._buffer = self._buffer[index:]
        self._length -= index
        if (value & 0xFE) != 0:
            raise Exception('Malformed Variable Byte Integer')
        self._packet['session_present'] = value != 0

    def process_topic(self):
        if self._type != PUBLISH:
            return
        value, index = self._decode(self._buffer, UTF8_ENCODED_STRING)
        self._buffer = self._buffer[index:]
        self._length -= index
        if len(value) == 0:
            raise Exception('Malformed Variable Byte Integer')
        self._packet['topic'] = value

    def process_id(self):
        if self._type in [CONNECT, CONNACK, DISCONNECT, AUTH]:
            return
        if self._type == PUBLISH and self._packet['qos'] == 0:
            raise Exception('Malformed Variable Byte Integer')
        value, index = self._decode(self._buffer, TWO_BYTE_INT)
        self._buffer = self._buffer[index:]
        self._length -= index
        self._packet['id'] = value

    def process_code(self):
        if self._type in [CONNECT, PUBLISH, SUBSCRIBE, SUBACK, UNSUBSCRIBE, UNSUBACK]:
            return
        value, index = self._decode(self._buffer, BYTE)
        self._buffer = self._buffer[index:]
        self._length -= index
        self._packet['code'] = value

    def process_properties(self):
        properties, index = self._process_properties(self._type, self._buffer)
        self._buffer = self._buffer[index:]
        self._length -= index
        self._packet['properties'] = properties

    def process_credentials(self):
        value, index = self._decode(self._buffer, UTF8_ENCODED_STRING)
        self._buffer = self._buffer[index:]
        self._length -= index
        self._packet['client_id'] = value

        if self._packet.has_key('will'):
            properties, index = self._process_properties(WILL, self._buffer)
            self._buffer = self._buffer[index:]
            self._length -= index
            self._packet['will']['properties'] = properties
            value, index = self._decode(self._buffer, UTF8_ENCODED_STRING)
            self._buffer = self._buffer[index:]
            self._length -= index
            self._packet['will']['topic'] = value
            value, index = self._decode(self._buffer, BINARY_DATA)
            self._buffer = self._buffer[index:]
            self._length -= index
            self._packet['will']['payload'] = value

        if self._packet['username_flag']:
            value, index = self._decode(self._buffer, UTF8_ENCODED_STRING)
            self._buffer = self._buffer[index:]
            self._length -= index
            self._packet['username'] = value
        if self._packet['password_flag']:
            value, index = self._decode(self._buffer, BINARY_DATA)
            self._buffer = self._buffer[index:]
            self._length -= index
            self._packet['password'] = value

    def process_app_message(self):
        if self._length == 0:
            self._packet['app_message'] = b""
        else:
            self._packet['app_message'] = self._buffer
            self._buffer = b""
            self._length = 0

    def process_topics(self):
        self._packet['topics'] = list()
        while True:
            value, index = self._decode(self._buffer, UTF8_ENCODED_STRING)
            self._buffer = self._buffer[index:]
            self._length -= index
            topic = {'filter': value}

            if self._type == SUBSCRIBE:
                value, index = self._decode(self._buffer, BYTE)
                self._buffer = self._buffer[index:]
                self._length -= index
                topic['qos'] = value & 0x03
                if topic['qos'] == 0x03:
                    raise Exception('Malformed Variable Byte Integer')
                topic['no_local'] = value & 0x04 != 0
                topic['retained'] = value & 0x08 != 0
                topic['retain_opt'] = (value >> 4) & 0x03
                if topic['retain_opt'] == 0x03:
                    raise Exception('Malformed Variable Byte Integer')

            self._packet['topics'].append(topic)
            if self._length == 0:
                break

    def process_topic_codes(self):
        self._packet['topics'] = list()
        while True:
            value, index = self._decode(self._buffer, BYTE)
            self._buffer = self._buffer[index:]
            self._length -= index
            self._packet['topics'].append(value)
            if self._length <= 0:
                break

    def _process_properties(self, mtype, stream):
        total_index = 0
        properties = {}
        length, index = self._decode(stream, VARIABLE_BYTE_INT)
        stream = stream[index:]
        total_index += index
        if length != 0:
            while length > 0:
                prop_code, index = self._decode(stream, VARIABLE_BYTE_INT)
                stream = stream[index:]
                total_index += index
                length -= index

                if prop_code == USER_PROPERTY:
                    if not properties.has_key(prop_code):
                        properties[USER_PROPERTY] = list()

                    pair, index = self._decode(stream, UTF8_ENCODED_STRING)
                    stream = stream[index:]
                    total_index += index
                    length -= index
                    properties[USER_PROPERTY].append(pair)
                else:
                    if not properties_dictionary.has_key(prop_code):
                        raise Exception('Malformed Variable Byte Integer')
                    if properties.has_key(prop_code):
                        raise Exception('Malformed Variable Byte Integer')

                    prop = properties_dictionary[prop_code]
                    if mtype not in prop['group']:
                        raise Exception('Malformed Variable Byte Integer')

                    data, index = self._decode(stream, prop['type'])
                    stream = stream[index:]
                    total_index += index
                    length -= index
                    properties[prop_code] = data
        return properties, total_index

    def process_protoheader(self):
        hdrlen = 2
        if len(self._buffer) >= hdrlen:
            self._jsonheader_len = struct.unpack(
                ">H", self._buffer[:hdrlen]
            )[0]
            self._buffer = self._buffer[hdrlen:]

    def process_jsonheader(self):
        hdrlen = self._jsonheader_len
        if len(self._buffer) >= hdrlen:
            self.jsonheader = self._json_decode(
                self._buffer[:hdrlen], "utf-8"
            )
            self._buffer = self._buffer[hdrlen:]
            for reqhdr in (
                "byteorder",
                "content-length",
                "content-type",
                "content-encoding",
            ):
                if reqhdr not in self.jsonheader:
                    raise ValueError(f'Missing required header "{reqhdr}".')

    def process_request(self):
        content_len = self.jsonheader["content-length"]
        if not len(self._buffer) >= content_len:
            return
        data = self._buffer[:content_len]
        self._buffer = self._buffer[content_len:]

        if self.jsonheader["content-type"] == "text/json":
            encoding = self.jsonheader["content-encoding"]
            self.request = self._json_decode(data, encoding)
            print("received request", repr(self.request), "from", self.addr)
        else:
            # Binary or unknown content-type
            self.request = data
            print(
                f'received {self.jsonheader["content-type"]} request from',
                self.addr,
            )

        # Set selector to listen for write events, we're done reading.
        #self._set_selector_events_mask("w")

    def create_response(self):
        if self.jsonheader["content-type"] == "text/json":
            response = self._create_response_json_content()
        else:
            # Binary or unknown content-type
            response = self._create_response_binary_content()
        message = self._create_message(**response)
        self.response_created = True
        self._buffer += message

        def _create_message(
        self, *, content_bytes, content_type, content_encoding
    ):
        jsonheader = {
            "byteorder": sys.byteorder,
            "content-type": content_type,
            "content-encoding": content_encoding,
            "content-length": len(content_bytes),
        }
        jsonheader_bytes = self._json_encode(jsonheader, "utf-8")
        message_hdr = struct.pack(">H", len(jsonheader_bytes))
        message = message_hdr + jsonheader_bytes + content_bytes
        return message

    def _create_response_json_content(self):
        action = self.request.get("action")
        if action == "search":
            query = self.request.get("value")
            answer = request_search.get(query) or f'No match for "{query}".'
            content = {"result": answer}
        else:
            content = {"result": f'Error: invalid action "{action}".'}
        content_encoding = "utf-8"
        response = {
            "content_bytes": self._json_encode(content, content_encoding),
            "content_type": "text/json",
            "content_encoding": content_encoding,
        }
        return response

    def _create_response_binary_content(self):
        response = {
            "content_bytes": b"First 10 bytes of request: "
            + self.request[:10],
            "content_type": "binary/custom-server-binary-type",
            "content_encoding": "binary",
        }
        return response

    def get_buffer(self):
        return self._buffer

    def set_buffer(self, _buffer):
        self._buffer = _buffer

    def is_request_created(self):
        return self.request_created

