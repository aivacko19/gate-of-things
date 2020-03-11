import sys
import os

import pika

class AuthenticationPublisher:

    def __init__(self, rabbitmq, recepient):
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(
                host=rabbitmq,
                connection_attempts=10,
                retry_delay=5,))

        self.channel = self.connection.channel()
        self.recepient = recepient

    def publish(self, user_reference, email='none'):
        self.channel.basic_publish(
            exchange='',
            routing_key=self.recepient,
            properties=pika.BasicProperties(
                correlation_id=user_reference),
            body=email)

    def on_response(self, ch, method, props, body):
        if self.corr_id == props.correlation_id:
            self.response = body

    def get_uri(self, user_reference):
        self.response = None
        self.corr_id = str(uuid.uuid4())
        self.channel.basic_publish(
            exchange='',
            routing_key=self.server,
            properties=pika.BasicProperties(
                reply_to=self.callback_queue,
                correlation_id=self.corr_id,),
            body=user_reference)
        while self.response is None:
            self.connection.process_data_events()
        return self.response

RABBITMQ = os.environ.get('RABBITMQ')
if not RABBITMQ:
    sys.exit(1)
AUTH_RECEPIENT = os.environ.get('AUTH_RECEPIENT')
if not AUTH_RECEPIENT:
    sys.exit(1)

publisher = AuthenticationPublisher(RABBITMQ, AUTH_RECEPIENT)