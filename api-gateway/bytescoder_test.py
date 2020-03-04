import json
import unittest

import bytescoder


class TestBytesCoder(unittest.TestCase):
	def test_encode(self):
		"""
		Test Encoder 
		"""
		packet = {1: 'string', 2: b"bytes", 3: 3}
		expect = '{"1": "string", "2": {"__bytes__": true, "__value__": "bytes"}, "3": 3}'
		result = json.dumps(packet, cls=bytescoder.BytesEncoder)
		self.assertEqual(result, expect)
	def test_decode(self):
		"""
		Test Decoder 
		"""
		body = '{"1": "string", "2": {"__bytes__": true, "__value__": "bytes"}, "3": 3}'
		packet = json.loads(body, object_hook=bytescoder.as_bytes)
		self.assertIsInstance(packet["2"], bytes)
	def test_integrate(self):
		"""
		Test Integrate
		"""
		packet = {'1': 'string', '2': b"bytes", '3': 3}
		body = json.dumps(packet, cls=bytescoder.BytesEncoder).encode('utf-8')
		result = json.loads(body.decode('utf-8'), object_hook=bytescoder.as_bytes)
		self.assertEqual(result, packet)


if __name__ == '__main__':
	unittest.main()