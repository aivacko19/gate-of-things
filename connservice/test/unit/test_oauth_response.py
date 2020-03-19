import unittest
import threading

import pika

from app.request_uri_client import RequestUriRpcClient

RABBITMQ = 'localhost'
MOCK_SERVER = 'MOCK'

class TestRequestUriClient(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super(TestRequestUriClient, self).__init__(*args, **kwargs)
        self.init_mock()

    def init_mock(self):
        conn = pika.BlockingConnection(
            pika.ConnectionParameters(
                host=RABBITMQ,
                connection_attempts=10,
                retry_delay=5,
                heartbeat=0,))
        channel = conn.channel()
        channel.queue_declare(queue=MOCK_SERVER)
        channel.basic_qos(prefetch_count=1)
        channel.basic_consume(queue=MOCK_SERVER, on_message_callback=self.mock_routine)
        thread = threading.Thread(
            target=channel.start_consuming,
            daemon=True)
        thread.start()

    def mock_routine(self, ch, method, props, body):
        ch.basic_publish(exchange='',
                         routing_key=props.reply_to,
                         properties=pika.BasicProperties(
                            correlation_id=props.correlation_id),
                         body='www.example.com')
        ch.basic_ack(delivery_tag=method.delivery_tag)

    def test_get_uri(self):
        RequestUriRpcClient.initInstance(RABBITMQ, MOCK_SERVER)
        client = RequestUriRpcClient.getInstance()

        uri = client.get_uri('user_ref')
        self.assertEqual(uri, 'www.example.com')

if __name__ == '__main__':
    unittest.main()