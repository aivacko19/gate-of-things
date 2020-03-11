import pika
import uuid
import os
import sys

class RequestUriRpcClient(object):

    def __init__(self, rabbitmq, remote_server):
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(
                host=rabbitmq,
                connection_attempts=10,
                retry_delay=5,))

        self.channel = self.connection.channel()
        self.server = remote_server

        result = self.channel.queue_declare(queue='', exclusive=True)
        self.callback_queue = result.method.queue

        self.channel.basic_consume(
            queue=self.callback_queue,
            on_message_callback=self.on_response,
            auto_ack=True)

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

RABBITMQ_HOSTNAME = os.environ.get('RABBITMQ_HOSTNAME')
if not RABBITMQ_HOSTNAME:
    sys.exit(1)
AUTH_QUEUE_NAME = os.environ.get('AUTH_QUEUE_NAME')
if not AUTH_QUEUE_NAME:
    sys.exit(1)

oauth_service = RequestUriRpcClient(RABBITMQ_HOSTNAME, AUTH_QUEUE_NAME)