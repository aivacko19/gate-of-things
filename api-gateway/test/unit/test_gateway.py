import json
import unittest
import queue
import socket
import threading
import time

from src import gateway

class TestProtocol:
    port = 1887

    def new_stream():
        return {'buffer': b""}

    def parse(stream):
        if not stream['buffer']:
            return {}
        string = stream['buffer'].decode('utf-8')
        return json.loads(string)

    def compose(packet):
        return {'buffer': json.dumps(packet).encode('utf-8')}

    def write(stream, data):
        stream['buffer'] += data

    def peek(stream, size):
        return stream['buffer'][:size]

    def position(stream, position):
        stream['buffer'] = stream['buffer'][position:]

    def busy(stream):
        return False

    def empty(stream):
        return not stream['buffer']

def thread_target(api_gateway):
    api_gateway.start_listening()
    api_gateway.close()

class TestGateway(unittest.TestCase):

    def init_gateway(self):
        self.request_queue = queue.Queue()
        self.receive_queue = queue.Queue()
        protocol = TestProtocol
        self.api_gateway = gateway.Gateway('localhost', protocol, self.request_queue, self.receive_queue)

        self.thread = threading.Thread(
            target=thread_target,
            args=(self.api_gateway,),
            daemon=True)
        self.thread.start()
        active = True

    def test_send(self):
        self.init_gateway()
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect_ex(('localhost', 1887))
        packet = {'prvi': 1, 'drugi': 'drugi', 'treci': True}
        result = {}
        data = json.dumps(packet).encode('utf-8')
        sent = sock.send(data)
        time.sleep(0.5)
        if not self.request_queue.empty():
            result = self.request_queue.get()
            del result['addr']
        self.assertEqual(packet, result)
        self.api_gateway.stop()
        sock.close()


    def test_send_and_receive(self):
        self.init_gateway()
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect_ex(('localhost', 1887))
        packet = {'prvi': 1, 'drugi': 'drugi', 'treci': True}
        result = {}
        data = json.dumps(packet).encode('utf-8')
        sent = sock.send(data)
        time.sleep(0.5)
        if not self.request_queue.empty():
            requested_packet = self.request_queue.get()
            requested_packet['write'] = True
            self.receive_queue.put(requested_packet)
            rcv_data = sock.recv(4096)
            result = json.loads(rcv_data.decode('utf-8'))
        self.assertEqual(packet, result)
        self.api_gateway.stop()
        sock.close()

    def test_send_and_disc(self):
        self.init_gateway()
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect_ex(('localhost', 1887))
        packet = {'prvi': 1, 'drugi': 'drugi', 'treci': True}
        data = json.dumps(packet).encode('utf-8')
        result = b"WRONg"
        sent = sock.send(data)
        time.sleep(0.5)
        if not self.request_queue.empty():
            requested_packet = self.request_queue.get()
            requested_packet['disconnect'] = True
            self.receive_queue.put(requested_packet)
            result = sock.recv(4096)
        self.assertEqual(result, b"")
        self.api_gateway.stop()
        sock.close()

    def test_disc(self):
        self.init_gateway()
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect_ex(('localhost', 1887))
        packet = {'prvi': 1, 'drugi': 'drugi', 'treci': True}
        data = json.dumps(packet).encode('utf-8')
        result = {'disconnect': False}
        sent = sock.send(data)
        time.sleep(0.5)
        self.assertFalse(self.request_queue.empty())
        rq_pack = self.request_queue.get()
        self.receive_queue.put(rq_pack)
        sock.close()
        time.sleep(0.5)
        self.assertFalse(self.request_queue.empty())
        result = self.request_queue.get()
        self.assertTrue(result['disconnect'])
        self.api_gateway.stop()



if __name__ == '__main__':
    unittest.main()