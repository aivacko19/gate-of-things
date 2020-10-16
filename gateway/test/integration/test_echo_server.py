import unittest
import socket

from app.protocols.mqtt.parser import read
from app.protocols.mqtt.parser import write
from app.protocols.mqtt.const import *
from app.protocols.mqtt.datatypes import *
from app.protocols.mqtt import stream

binary_message = b'ferea\x30\x14\x1Afae' # size 11
topic_name = 'topic' # size 5

datasets = [
    (
        encode_byte(CONNECT)
        + encode_variable_byte_int(69)
        + encode_utf8_encoded_string('MQTT')
        + encode_byte(5)
        + encode_byte(0b11001110)
        + encode_two_byte_int(10)
        + encode_variable_byte_int(5)
        + encode_variable_byte_int(SESSION_EXPIRY_INTERVAL)
        + encode_four_byte_int(10)
        + encode_utf8_encoded_string('iamtheclient')
        + encode_variable_byte_int(0)
        + encode_utf8_encoded_string(topic_name)
        + encode_binary_data(binary_message)
        + encode_utf8_encoded_string('user1')
        + encode_binary_data(b'password1'),
        {
            'type': CONNECT,
            'protocol_name': 'MQTT',
            'protocol_version': 5,
            'properties': {DICT[SESSION_EXPIRY_INTERVAL]['name']: 10},
            'will': {'retain': False,
                     'qos': 1,
                     'topic': topic_name,
                     'properties': {},
                     'payload': binary_message},
            'keep_alive': 10,
            'clean_start': True,
            'client_id': 'iamtheclient',
            'username': 'user1',
            'password': b'password1'
        }
    ),
    (
        encode_byte(PUBLISH | (1<<1))
        + encode_variable_byte_int(21)
        + encode_utf8_encoded_string(topic_name)
        + encode_two_byte_int(10)
        + encode_variable_byte_int(0)
        + binary_message,
        {
            'type': PUBLISH,
            'dup': False,
            'qos': 1,
            'retain': False,
            'topic': topic_name,
            'id': 10,
            'properties': {},
            'payload': binary_message
        }
    ),
    (
        encode_byte(PUBACK)
        + encode_variable_byte_int(4)
        + encode_two_byte_int(10)
        + encode_byte(0x00)
        + encode_variable_byte_int(0),
        {
            'type': PUBACK,
            'id': 10,
            'code': 0x00,
            'properties': {}
        }
    ),
    (
        encode_byte(SUBSCRIBE)
        + encode_variable_byte_int(20)
        + encode_two_byte_int(10)
        + encode_variable_byte_int(0)
        + encode_utf8_encoded_string(topic_name)
        + encode_byte(0b00000001)
        + encode_utf8_encoded_string(topic_name + '1')
        + encode_byte(0b00000010),
        {
            'type': SUBSCRIBE,
            'id': 10,
            'properties': {},
            'topics': [{'filter': topic_name,
                        'max_qos': 1,
                        'retain_handling': 0,
                        'no_local': False,
                        'retain_as_published': False},
                       {'filter': topic_name + '1',
                        'max_qos': 2,
                        'retain_handling': 0,
                        'no_local': False,
                        'retain_as_published': False}]
        }
    ),
    (
        encode_byte(UNSUBSCRIBE)
        + encode_variable_byte_int(18)
        + encode_two_byte_int(10)
        + encode_variable_byte_int(0)
        + encode_utf8_encoded_string(topic_name)
        + encode_utf8_encoded_string(topic_name + '1'),
        {
            'type': UNSUBSCRIBE,
            'id': 10,
            'properties': {},
            'topics': [{'filter': topic_name},
                       {'filter': topic_name + '1'}]
        }
    ),
    (
        encode_byte(SUBACK)
        + encode_variable_byte_int(5)
        + encode_two_byte_int(10)
        + encode_variable_byte_int(0)
        + encode_byte(0x00)
        + encode_byte(0x00),
        {
            'type': SUBACK,
            'id': 10,
            'properties': {},
            'topics': [{'code': 0x00},
                       {'code': 0x00}]
        }
    ),
    (
        encode_byte(PINGREQ)
        + encode_variable_byte_int(0),
        {
            'type': PINGREQ
        }
    ),
    (
        encode_byte(DISCONNECT)
        + encode_variable_byte_int(7)
        + encode_byte(0x00)
        + encode_variable_byte_int(5)
        + encode_byte(SESSION_EXPIRY_INTERVAL)
        + encode_four_byte_int(0),
        {
            'type': DISCONNECT,
            'code': 0x00,
            'properties': {DICT[SESSION_EXPIRY_INTERVAL]['name']: 0}
        }
    )
]

class TestEchoServer(unittest.TestCase):

    def test_echo_server(self):

        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect_ex(('localhost', 1887))

        for dataset in datasets:
            data = dataset[0]
            sent = sock.sendall(data)

            s = stream.Stream()
            while True:
                data = sock.recv(4096)
                s.load(data)
                if not s.is_loading():
                    break
            recv_data = s.dump()

            self.assertEqual(recv_data, data)

        sock.close()


if __name__ == '__main__':
    unittest.main()