import sys
import json
import io
import struct
import datatypes
import properties

from datatypes import encode
from properties import encode_properties
from stream import MQTTStream

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

class MalformedPacketException(Exception):
    def __init__(self, message):
        super().__init__(message)

class MQTTFactory:
    def __init__(self):
        self.port = 1887

    def get_port():
        return self.port

    def create_stream():
        return MQTTStream()

    def create_message():
        return MQTTMessage()

class MQTTMessage:
    def __init__(self):
        self._buffer = b""
        self._jsonheader_len = None
        self.jsonheader = None
        self.request = None
        self.request_created = False
        self.response_created = False
        self._packetloaded = False
        self._length = None
        self._type = None
        self._packet = None
        self.disconnect = False
        self.action = None

    def _json_encode(self, obj, encoding):
        return json.dumps(obj, ensure_ascii=False).encode(encoding)

    def _json_decode(self, json_bytes, encoding):
        tiow = io.TextIOWrapper(
            io.BytesIO(json_bytes), encoding=encoding, newline=""
        )
        obj = json.load(tiow)
        tiow.close()
        return obj

    def _decode(self, data_type)
        value, index = datatypes.decode(self._buffer, data_type)
        self._buffer = self._buffer[index:]
        if self._length is not None:
            self._length -= index
        return value

    def _decode_properties(self, packet_type):
        value, index = properties.decode_properties(self._buffer, packet_type)
        self._buffer = self._buffer[index:]
        if self._length is not None:
            self._length -= index
        return value

    def read(self, stream):
        packet_type, dup, qos, retain = stream.get_header()

        if packet_type == RESERVED:
            raise MalformedPacketException("Bad packet type")
        self._packet['type'] = packet_type

        if packet_type != PUBLISH:
            if dup or retain or qos != 0:
                raise MalformedPacketException("Bytes 0-3 reserved")

            if packet_type == PINGREQ:
                self._packet['type'] = PINGRESP
                self._action = 'write'
                return

            if packet_type == PINGRESP:
                self._action = 'listen'
                return 

            if packet_type == CONNECT:
                self.read_connect_packet(stream)
                return 

            if packet_type == CONNACK:
                self.read_connack_packet(stream)
                return

            if packet_type in [SUBSCRIBE, SUBACK, UNSUBSCRIBE, UNSUBACK]:
                self.read_sub_packet(stream)
                return 

            if packet_type not in [DISCONNECT, AUTH]:
                self._packet['id'] = stream.get_int()
            self._packet['code'] = stream.get_byte()
            packed_properties = stream.get_properties()
            self._packet['properties'] = properties.unpack(packed_properties, packet_type)


        if qos not in range(2):
            raise MalformedPacketException("Bad QoS")
        self._packet['dup'] = dup
        self._packet['qos'] = qos
        self._packet['retain'] = retain
        self._packet['topic'] = stream.get_string()
        if qos > 0:
            self._packet['id'] = stream.get_int()
        packed_properties = stream.get_properties()
        self._packet['properties'] = properties.unpack(packed_properties, PUBLISH)
        self._packet['payload'] = stream.dump()


    def read_connect_packet(self, stream):
        self._packet['protocol_name'] = stream.get_string()
        self._packet['protocol_version'] = stream.get_byte()

        username_flag, password_flag, retain, qos, 
         will_flag, clean_start, reserved = stream.get_connect_flags()
        if reserved:
            raise MalformedPacketException("Byte 0 reserved")

        if will_flag:
            if qos not in range(2):
                raise MalformedPacketException("Bad QoS")
        else:
            if retain or qos != 0:
                raise MalformedPacketException("Bytes 3-5 reserved")

        self._packet['keep_alive'] = stream.get_int()

        packed_properties = stream.get_properties()
        self._packet['properties'] = properties.unpack(packed_properties, CONNECT)

        self._packet['client_id'] = stream.get_string()
        if will_flag:
            packed_properties = stream.get_properties()
            will_properties = properties.unpack(packed_properties, WILL)
            topic = stream.get_string()
            payload = self.get_binary()
            self._packet['will'] = {
                'qos': qos,
                'retain': retain,
                'topic': topic,
                'payload': payload,
                'properties': will_properties
            }
        if username_flag:
            self._packet['username'] = stream.get_string()
        if password_flag:
            self._packet['password'] = stream.get_binary()

    def read(self, data):
        self._buffer += data
        is_finished = False
        try:
            self.decode_message()
            is_finished = self._packetloaded
        except Exception as e:
            self.handle_error(e)
            if self.action == 'write':
                self.encode_message()
            is_finished = True
        if not self._packetloaded:
            return is_finished
        
        self.process_request()
        if self.action == 'write':
            self.encode_message()
        return is_finished


    def decode_message(self):
        if not self._type is None:
            self.decode_fix_header()

        if self._type is not None:
            if self._length is None:
                self._length = self._decode(datatypes.VARIABLE_BYTE_INT)

        if self._length is not None:
            if not self._packetloaded:
                self.load_packet()

        if self._packetloaded and len(self._buffer) != 0:
            self.decode_var_header()
            self.decode_payload()
            if self._length != 0 or len(self._buffer) != 0:
                raise Exception('Malformed Variable Byte Integer')

    def decode_fix_header(self):
        if len(self._buffer) == 0:
            return
        value = self._decode(datatypes.BYTE)
        flags = value & 0x0F
        self._type = value >> 4
        slef._packet = {}
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

    def load_packet(self):
        if len(self._buffer) < self._length:
            return
        if len(self._buffer) > self._length:
            raise Exception('Malformed Variable Byte Integer')
        self._packetloaded = True

    def decode_var_header(self):
        if self._type == CONNECT:
            self.decode_connect_header()

        elif self._type == CONNACK:
            flags = self._decode(datatypes.BYTE)
            if flags >> 1 != 0:
                raise Exception('Malformed Variable Byte Integer')
            self._packet['session_present'] = flags != 0

        elif self._type == PUBLISH:
            self._packet['topic'] = self._decode(datatypes.UTF8_ENCODED_STRING)
            if len(self._packet['topic']) == 0:
                raise Exception('Malformed Variable Byte Integer')

        if self._type not in [CONNECT, CONNACK, DISCONNECT, AUTH]:
            self._packet['id'] = self._decode(datatypes.TWO_BYTE_INT)

        if self._type not in [CONNECT, PUBLISH, SUBSCRIBE, SUBACK, UNSUBSCRIBE, UNSUBACK]:
            self._packet['code'] = self._decode(datatypes.BYTE)

        self._packet['properties'] = self._decode_properties(self._type)

    def decode_connect_header(self):
        self._packet['protocol_name'] = self._decode(datatypes.UTF8_ENCODED_STRING)
        self._packet['protocol_version'] = self._decode(datatypes.TWO_BYTE_INT)
        flags = self._decode(datatypes.BYTE)

        if flags & 0x04 != 0:
            self._packet['will'] = {
                'qos': (flags >> 3) & 0x03,
                'retain': flags & 0x20 != 0
            }
            if self._packet['will']['qos'] == 3:
                raise Exception('Malformed Variable Byte Integer')
        else:
            if (flags >> 3) & 0x03 != 0 or flags & 0x20 != 0:
                raise Exception('Malformed Variable Byte Integer')

        self._packet['clean_start'] = flags & 0x02 != 0
        if flags & 0x40 != 0:
            self._packet['password'] = 0
        if flags & 0x80 != 0:
            self._packet['username'] = 0
        if flags & 0x01 != 0:
            raise Exception('Malformed Variable Byte Integer')

        self._packet['keep_alive'] = self._decode(datatypes.TWO_BYTE_INT)

    def decode_payload(self):
        if self._type == CONNECT:
            self.decode_connect_payload()
        elif self._type == PUBLISH:
            self._packet['app_message'] = self._buffer
            self._buffer = b""
            self._length = 0
        elif self._type in [SUBSCRIBE, UNSUBSCRIBE, SUBACK, UNSUBACK]:
            self.decode_topics()

    def decode_connect_payload(self):
        self._packet['client_id'] = self._decode(datatypes.UTF8_ENCODED_STRING)
        if self._packet.has_key('will'):
            self._packet['will']['properties'] = self._decode_properties(WILL)
            self._packet['will']['topic'] = self._decode(datatypes.UTF8_ENCODED_STRING)
            self._packet['will']['payload'] = self._decode(datatypes.BINARY_DATA)
        if self._packet.has_key('username'):
            self._packet['username'] = self._decode(datatypes.UTF8_ENCODED_STRING)
        if self._packet.has_key('password'):
            self._packet['password'] = self._decode(datatypes.BINARY_DATA)

    def decode_topics(self):
        self._packet['topics'] = list()
        while True:
            topic = {}
            if self._type in [SUBSCRIBE, UNSUBSCRIBE]:
                topic['filter'] = self._decode(datatypes.UTF8_ENCODED_STRING)
            if self._type == SUBSCRIBE:
                flags = self._decode(datatypes.BYTE)
                topic['qos'] = flags & 0x03
                if topic['qos'] == 0x03:
                    raise Exception('Malformed Variable Byte Integer')
                topic['no_local'] = flags & 0x04 != 0
                topic['retained'] = flags & 0x08 != 0
                topic['retain_opt'] = (flags >> 4) & 0x03
                if topic['retain_opt'] == 0x03:
                    raise Exception('Malformed Variable Byte Integer')
                if flags >> 6 != 0:
                    raise Exception('Malformed Variable Byte Integer')
            if self._type in [SUBACK, UNSUBACK]:
                topic['code'] = self._decode(datatypes.BYTE)

            self._packet['topics'].append(topic)
            if self._length == 0:
                break

    def process_request(self):
        if self._type == CONNECT:
            self.process_connect()
        elif self._type == AUTH:
            self.process_auth()
        elif self._type == SUBSCRIBE:
            self.process_subscribe()
        elif self._type == UNSUBSCRIBE:
            self.process_unsubscribe()
        elif self._type == PUBLISH:
            self.process_publish()
        elif self._type == PINGREQ:
            self.process_ping()

    def process_connect(self):

    def process_ping(self):
        self._type == PINGRESP
        self.action = 'write'

    def encode_message(self):
        fix_header = encode_fix_header()
        var_header = encode_var_header()
        payload = encode_payload()
        content = var_header + payload
        length = encode(len(content), datatypes.VARIABLE_BYTE_INT)
        self._buffer = fix_header + length + var_header + payload

    def encode_fix_header(self):
        byte = self._type << 4
        if self._type == PUBLISH:
            if self._packet['dup']:
                byte |= 0x08
            if self._packet['retain']:
                byte |= 0x01
            qos = self._packet['qos']
            if qos > 2 or qos < 0:
                raise Exception('Invalid QoS')
            byte |= qos << 1
        return encode(byte, datatypes.BYTE)

    def encode_var_header(self):
        stream = b""
        if self._type in [PINGREQ, PINGRESP]:
            return stream
        if self._type == CONNECT:
            stream += self.encode_connect_header()
        elif self._type == CONNACK:
            flags = 0
            if self._packet['session_present']:
                flags = 1
            stream += encode(flags, datatypes.BYTE)
        elif self._type == PUBLISH:
            stream += encode(self._packet['topic'], datatypes.UTF8_ENCODED_STRING)
        if self._type not in [CONNECT, CONNACK, DISCONNECT, AUTH]:
            stream += encode(self._packet['id'], datatypes.TWO_BYTE_INT)
        if self._type not in [CONNECT, PUBLISH, SUBSCRIBE, SUBACK, UNSUBSCRIBE, UNSUBACK]:
            stream += encode(self._packet['code'], datatypes.BYTE)
        stream += encode_properties(self._packet['properties'], self._type)
        return stream

    def encode_connect_header(self):
        stream = b""
        stream += encode(self._packet['protocol_name'], datatypes.UTF8_ENCODED_STRING)
        stream += encode(self._packet['protocol_version'], datatypes.TWO_BYTE_INT)
        flags = 0
        if self._packet.has_key('will'):
            flags |= 0x04
            if self._packet['will']['retain']:
                flags |= 0x20
            qos = self._packet['will']['qos']
            if qos not in range(2):
                raise Exception('Ivalid Will QoS')
            flags |= qos << 3
        if self._packet['clean_start']:
            flags |= 0x02
        if self._packet.has_key['password']:
            flags |= 0x40
        if self._packet.has_key['username']:
            flags |= 0x80
        stream += encode(flags, datatypes.BYTE)
        stream += encode(self._packet['keep_alive'], datatypes.TWO_BYTE_INT)
        return stream

    def encode_payload(self):
        stream = b""
        if self._type in [PINGREQ, PINGRESP]:
            return stream
        if self._type == CONNECT:
            stream += self.encode_connect_payload()
        elif self._type == PUBLISH and self._packet.has_key('app_message'):
            stream += self._packet['app_message']
        elif self._type in [SUBSCRIBE, UNSUBSCRIBE, SUBACK, UNSUBACK]:
            stream += self.encode_topics()
        return stream

    def encode_connect_payload(self):
        stream = b""
        stream += encode(self._packet['client_id'], datatypes.UTF8_ENCODED_STRING)
        if self._packet.has_key('will'):
            stream += encode_properties(self._packet['will']['properties'], WILL)
            stream += encode(self._packet['will']['topic'], datatypes.UTF8_ENCODED_STRING)
            stream += encode(self._packet['will']['payload'], datatypes.BINARY_DATA)
        if self._packet.has_key('username'):
            stream += encode(self._packet['username'], datatypes.UTF8_ENCODED_STRING)
        if self._packet.has_key('password'):
            stream += encode(self._packet['password'], datatypes.BINARY_DATA)
        return stream

    def encode_topics(self):
        stream = b""
        if not self._packet.has_key('topics'):
            raise Exception("No topics, at least one needed")
        topics = self._packet['topics']
        if len(topics) == 0:
            raise Exception("No topics, at least one needed")
        for topic in topics:
            if self._type in [SUBSCRIBE, UNSUBSCRIBE]:
                stream += encode(topic['filter'], datatypes.UTF8_ENCODED_STRING)
            if self._type == SUBSCRIBE:
                if topic['qos'] not in range(2):
                    raise Exception('Malformed Variable Byte Integer')
                flags = topic['qos']
                if topic['no_local']:
                    flags |= 0x04
                if topic['retained']:
                    flags |= 0x08
                if topic['retain_opt'] not in range(2):
                    raise Exception('Malformed Variable Byte Integer')
                flags |= topic['retain_opt'] << 4
                stream += encode(flags, datatypes.BYTE)
            if self._type in [SUBACK, UNSUBACK]:
                stream += encode(topic['code'], datatypes.BYTE)
        return stream

    # def process_protoheader(self):
    #     hdrlen = 2
    #     if len(self._buffer) >= hdrlen:
    #         self._jsonheader_len = struct.unpack(
    #             ">H", self._buffer[:hdrlen]
    #         )[0]
    #         self._buffer = self._buffer[hdrlen:]

    # def process_jsonheader(self):
    #     hdrlen = self._jsonheader_len
    #     if len(self._buffer) >= hdrlen:
    #         self.jsonheader = self._json_decode(
    #             self._buffer[:hdrlen], "utf-8"
    #         )
    #         self._buffer = self._buffer[hdrlen:]
    #         for reqhdr in (
    #             "byteorder",
    #             "content-length",
    #             "content-type",
    #             "content-encoding",
    #         ):
    #             if reqhdr not in self.jsonheader:
    #                 raise ValueError(f'Missing required header "{reqhdr}".')

    # def process_request(self):
    #     content_len = self.jsonheader["content-length"]
    #     if not len(self._buffer) >= content_len:
    #         return
    #     data = self._buffer[:content_len]
    #     self._buffer = self._buffer[content_len:]

    #     if self.jsonheader["content-type"] == "text/json":
    #         encoding = self.jsonheader["content-encoding"]
    #         self.request = self._json_decode(data, encoding)
    #         print("received request", repr(self.request), "from", self.addr)
    #     else:
    #         # Binary or unknown content-type
    #         self.request = data
    #         print(
    #             f'received {self.jsonheader["content-type"]} request from',
    #             self.addr,
    #         )

    #     # Set selector to listen for write events, we're done reading.
    #     #self._set_selector_events_mask("w")

    # def create_response(self):
    #     if self.jsonheader["content-type"] == "text/json":
    #         response = self._create_response_json_content()
    #     else:
    #         # Binary or unknown content-type
    #         response = self._create_response_binary_content()
    #     message = self._create_message(**response)
    #     self.response_created = True
    #     self._buffer += message

    #     def _create_message(
    #     self, *, content_bytes, content_type, content_encoding
    # ):
    #     jsonheader = {
    #         "byteorder": sys.byteorder,
    #         "content-type": content_type,
    #         "content-encoding": content_encoding,
    #         "content-length": len(content_bytes),
    #     }
    #     jsonheader_bytes = self._json_encode(jsonheader, "utf-8")
    #     message_hdr = struct.pack(">H", len(jsonheader_bytes))
    #     message = message_hdr + jsonheader_bytes + content_bytes
    #     return message

    # def _create_response_json_content(self):
    #     action = self.request.get("action")
    #     if action == "search":
    #         query = self.request.get("value")
    #         answer = request_search.get(query) or f'No match for "{query}".'
    #         content = {"result": answer}
    #     else:
    #         content = {"result": f'Error: invalid action "{action}".'}
    #     content_encoding = "utf-8"
    #     response = {
    #         "content_bytes": self._json_encode(content, content_encoding),
    #         "content_type": "text/json",
    #         "content_encoding": content_encoding,
    #     }
    #     return response

    # def _create_response_binary_content(self):
    #     response = {
    #         "content_bytes": b"First 10 bytes of request: "
    #         + self.request[:10],
    #         "content_type": "binary/custom-server-binary-type",
    #         "content_encoding": "binary",
    #     }
    #     return response

    def get_buffer(self):
        return self._buffer

    def update_buffer(self, sent):
        self._buffer = self._buffer[sent:]

    def action(self):
        return self.action