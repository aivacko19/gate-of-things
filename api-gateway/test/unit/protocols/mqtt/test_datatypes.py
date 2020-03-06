import unittest

from src.protocols.mqtt import datatypes

datasets =[
    (b'\x04', 4, 1),
    (b'\x7F', 127, 1),
    (b'\x80\x01', 128, 2),
    (b'\xFF\x7F', 16383, 2),
    (b'\x80\x80\x01', 16384, 3),
    (b'\xFF\xFF\x7F', 2097151, 3),
    (b'\x80\x80\x80\x01', 2097152, 4),
    (b'\xFF\xFF\xFF\x7F', 268435455, 4),
]

class TestDatatypes(unittest.TestCase):

    def test_decode_var(self):
        for dataset in datasets:
            data = dataset[0]
            expect = dataset[1]
            expect_index = dataset[2]
            result, index = datatypes.decode_variable_byte_int(data)
            self.assertEqual(result, expect)
            self.assertEqual(index, expect_index)

    def test_encode_var(self):
        for dataset in datasets:
            expect = dataset[0]
            data = dataset[1]
            result = datatypes.encode_variable_byte_int(data)
            self.assertEqual(result, expect)

if __name__ == '__main__':
    unittest.main()