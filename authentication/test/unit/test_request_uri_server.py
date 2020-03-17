import unittest
import threading
import uuid

import pika

from app.request_uri_server import RequestUriRpcServer

RABBITMQ = 'localhost'
MOCK_SERVER = 'MOCK'

class TestRequestUriServer(unittest.TestCase):

    def on_response(self, ch, method, props, body):
        if self.corr_id == props.correlation_id:
            self.response = body

    def test_get_uri(self):
        server = RequestUriRpcServer(RABBITMQ, MOCK_SERVER, '127.0.0.1:5000')
        server.run()

        conn = pika.BlockingConnection(
            pika.ConnectionParameters(
                host=RABBITMQ,
                connection_attempts=10,
                retry_delay=5,
                heartbeat=0,))
        channel = conn.channel()
        result = channel.queue_declare(queue='', exclusive=True)
        callback_queue = result.method.queue

        channel.basic_consume(
            queue=callback_queue,
            on_message_callback=self.on_response,
            auto_ack=True)

        self.response = None
        self.corr_id = str(uuid.uuid4())
        channel.basic_publish(
            exchange='',
            routing_key=MOCK_SERVER,
            properties=pika.BasicProperties(
                reply_to=callback_queue,
                correlation_id=self.corr_id,),
            body='user_reference')

        while self.response is None:
            conn.process_data_events()

        self.assertEqual(self.response, 'www.example.com')

if __name__ == '__main__':
    unittest.main()