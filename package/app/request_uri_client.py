import pika
import uuid
import os
import sys

class RequestUriRpcClient:

    __instance = None

    @staticmethod
    def initInstance(rabbitmq, remote_server):
        RequestUriRpcClient.__instance = RequestUriRpcClient(rabbitmq, remote_server)

    @staticmethod
    def getInstance():
        return RequestUriRpcClient.__instance

    def __init__(self, rabbitmq, remote_server):
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(
                host=rabbitmq,
                connection_attempts=10,
                retry_delay=5,
                heartbeat=0,))
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
            self.connection.process_data_events(time_limit=2)
        return self.response