import unittest

from src.protocols.mqtt.parser import read
from src.protocols.mqtt.parser import write
from src.protocols.mqtt.const import *
from src.protocols.mqtt.datatypes import *
from src.protocols.mqtt import stream

binary_message = b'ferea\x30\x14\xF3fae' # size 11
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

class TestParse(unittest.TestCase):

    def test_parse(self):
        for dataset in datasets:
            data = dataset[0]
            result = dataset[1]
            s = stream.Stream()
            s.load(data)
            self.assertFalse(s.is_loading())
            packet = read(s)
            for key in packet:
                self.assertEqual(packet[key], result[key])
            self.assertEqual(packet, result)

    def test_compose(self):
        for dataset in datasets:
            packet = dataset[1]
            result = dataset[0]
            s = stream.Stream()
            write(packet, s)
            data = s.dump()
            self.assertEqual(data, result)


if __name__ == '__main__':
    unittest.main()