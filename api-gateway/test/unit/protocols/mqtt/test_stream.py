import unittest

from src.protocols.mqtt import stream
from src.protocols.mqtt import const

datasets = [
    [
        (const.WILL_DELAY_INTERVAL, b'\x00\x00\x00\x14', 20),
        (const.AUTHENTICATION_DATA, b'\x00\x07mojem\x03d', b'mojem\x03d'),
        (const.TOPIC_ALIAS, b'\x00\x20', 32),
        (const.SUBSCRIPTION_IDENTIFIER, b'\x80\x01', 128),
        (const.USER_PROPERTY, b'\x00\x03mal\x00\x04maca', ('mal', 'maca')),
        (const.ASSIGNED_CLIENT_IDENTIFIER, b'\x00\x0AMomo_Kapor', 'Momo_Kapor'),
        (const.PAYLOAD_FORMAT_INDICATOR, b'\x90', 0x90),
        (const.SERVER_REFERENCE, b'\x00\x09localhost', 'localhost'),
    ],
]

class TestStream(unittest.TestCase):

    def test_loading(self):
        data = b'\xF0\x02\x03\x04'
        s = stream.Stream()
        s.load(data)
        self.assertFalse(s.is_loading())
        result = s.get_byte()
        self.assertEqual(result, 0xF0)
        result = s.get_var_int()
        self.assertEqual(result, 2)
        for i in range(result):
            if i == 0:
                result = s.get_byte()
                self.assertEqual(result, 3)
            else:
                result = s.get_var_int()
                self.assertEqual(result, 4)
        with self.assertRaises(stream.OutOfBoundsError):
            s.get_byte()

    def test_overflow(self):
        data = b'f\x0A\x04asdf\x00\x00\x00\x10\x04asdf'
        s = stream.Stream()
        s.load(data[:5])
        self.assertTrue(s.is_loading())
        s.load(data[5:9])
        self.assertTrue(s.is_loading())
        with self.assertRaises(OverflowError):
            s.load(data[9:])

    def test_num(self):
        data = b'\xC0\x12\x00\x08\x00\x00\x00\x08\x80\x80\x01\x80\x80\x80\x01\xFF\xFF\xFF\xFF\xFF'
        s = stream.Stream()
        s.load(data)
        self.assertFalse(s.is_loading())
        s.get_byte()
        s.get_var_int()
        self.assertEqual(s.get_int(), 8)
        self.assertEqual(s.get_long(), 8)
        self.assertEqual(s.get_var_int(), 16384)
        self.assertEqual(s.get_var_int(), 2097152)
        with self.assertRaises(stream.MalformedVariableIntegerError):
            s.get_var_int()

    def test_string(self):
        data = b'\xA0\x1A\x00\x06encode\x00\x03\xAB\xCD\xEF\x00\x05onamo\x00\x04namo'
        s = stream.Stream()
        s.load(data)
        self.assertFalse(s.is_loading())
        s.get_byte()
        self.assertEqual(s.get_var_int(), 26)
        self.assertEqual(s.get_string(), 'encode')
        self.assertEqual(s.get_binary(), data[12:15])
        self.assertEqual(s.get_string_pair(), ('onamo', 'namo'))

    def test_empty(self):
        data = b''
        s = stream.Stream()
        s.load(data)
        self.assertTrue(s.is_loading())
        self.assertEqual(s.dump(), data)

    def test_put(self):
        s = stream.Stream()
        s.put_byte(0)
        s.put_byte(0)
        s.put_byte(0)
        s.put_byte(204)
        self.assertEqual(s.get_long(), 204)
        s.put_string('mali')
        s.put_string('krali')
        self.assertEqual(s.get_string_pair(), ('mali', 'krali'))
        s.put_byte(0x80)
        s.put_byte(0x80)
        s.put_byte(0x80)
        s.put_byte(1)
        self.assertEqual(s.get_var_int(), 2097152)
        s.put_string('bytes')
        self.assertEqual(s.get_binary(), b'bytes')

    def test_flags(self):
        s = stream.Stream()
        data = b'\x37\xAA\x37\xB6'
        s.append(data)
        packet_type, dup, qos, retain = s.get_header()
        self.assertEqual(packet_type, 0x30)
        self.assertEqual(dup, False)
        self.assertEqual(qos, 3)
        self.assertEqual(retain, True)
        un, pw, wr, wq, wf, cs, res = s.get_connect_flags()
        self.assertEqual(un, True)
        self.assertEqual(pw, False)
        self.assertEqual(wr, True)
        self.assertEqual(wq, 1)
        self.assertEqual(wf, False)
        self.assertEqual(cs, True)
        self.assertEqual(res, False)
        f, res = s.get_connack_flags()
        self.assertEqual(f, True)
        self.assertEqual(res, True)
        rh, rp, nl, mqos, res = s.get_sub_flags()
        self.assertEqual(rh, 3)
        self.assertEqual(rp, False)
        self.assertEqual(nl, True)
        self.assertEqual(mqos, 2)
        self.assertEqual(res, True)
        s.put_header(packet_type, dup, qos, retain)
        h = s.get_byte()
        s.get_var_int()
        s.put_byte(h)
        s.put_connect_flags(un, pw, wr, wq, wf, cs)
        s.put_connack_flags(f)
        s.put_sub_flags(rh, rp, nl, mqos)
        buf = s.dump()
        for i in range(len(buf)):
            self.assertEqual(buf[i], data[i] & buf[i])

    def test_properties(self):
        s = stream.Stream()
        for dataset in datasets:
            s.put_var_int(60)
            for row in dataset:
                s.put_var_int(row[0])
                s.append(row[1])
            buf = s.dump()
            self.assertEqual(len(buf), 61)
            s.append(buf)
            props = s.get_properties()
            self.assertTrue(s.empty())
            for prop in props:
                for row in dataset:
                    if prop[0] == row[0]:
                        self.assertEqual(prop[1], row[2])
            s.put_properties(props)
            # self.assertEqual(s.get_var_int(), 60)
            self.assertEqual(s.get_properties(), props)

    def test_put_properties(self):
        props = [(const.USER_PROPERTY, ('a', 'b'))]
        for prop in props:
            self.assertEqual(len(prop), 2)
            self.assertEqual(len(prop[1]), 2)
            value = prop[1]
            self.assertFalse(not isinstance(value, tuple))
            self.assertFalse(len(value) < 2)
        s = stream.Stream()
        s.put_properties(props)
        self.assertTrue(s.dump())






if __name__ == '__main__':
	unittest.main()