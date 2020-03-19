import unittest
import threading
import uuid
import time

import pika

from app.auth_publisher import AuthenticationPublisher

RABBITMQ = 'localhost'
MOCK_SERVER = 'MOCK'

class TestAuthPublisher(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super(TestAuthPublisher, self).__init__(*args, **kwargs)
        self.init_mock()
        self.user_ref = None
        self.email = None

    def init_mock(self):
        conn = pika.BlockingConnection(
            pika.ConnectionParameters(
                host=RABBITMQ,
                connection_attempts=10,
                retry_delay=5,
                heartbeat=0,))
        channel = conn.channel()
        channel.queue_declare(queue=MOCK_SERVER)
        channel.basic_consume(
            queue=MOCK_SERVER, 
            on_message_callback=self.mock_routine,
            auto_ack=True)
        thread = threading.Thread(
            target=channel.start_consuming,
            daemon=True)
        thread.start()

    def mock_routine(self, ch, method, props, body):
        self.email = body
        self.user_ref = props.correlation_id

    def test_get_uri(self):
        AuthenticationPublisher.initInstance(RABBITMQ, MOCK_SERVER)
        client = AuthenticationPublisher.getInstance()

        client.publish('user_ref', 'email@gmail.com')
        time.sleep(1)
        self.assertEqual(self.email, 'email@gmail.com')
        self.assertEqual(self.user_ref, 'user_ref')

if __name__ == '__main__':
    unittest.main()