import sys
import os
import threading

import pika

from providers import google as provider 

class RequestUriRpcServer:

    def __init__(self, rabbitmq, server, redirect_uri):

        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(
                host=rabbitmq,
                connection_attempts=10,
                retry_delay=5,))

        self.channel = self.connection.channel()
        self.server = server
        self.redirect_uri = redirect_uri

        self.channel.queue_declare(queue=server)

    def on_request(self, ch, method, properties, body):
        user_reference = body

        provider_cfg = provider.get_cfg()
        authorization_endpoint = provider_cfg["authorization_endpoint"]

        request_uri = provider.client.prepare_request_uri(
            authorization_endpoint,
            redirect_uri=self.redirect_uri,
            scope=["openid", "email", "profile"],
            state=user_reference
        )

        ch.basic_publish(exchange='',
                         routing_key=props.reply_to,
                         properties=pika.BasicProperties(
                            correlation_id=props.correlation_id),
                         body=request_uri)
        ch.basic_ack(delivery_tag=method.delivery_tag)

    def run(self):
        self.thread = threading.Thread(
            target=self.consume,
            daemon=True)
        self.thread.start()

    def consume(self):
        self.channel.basic_qos(prefetch_count=1)
        self.channel.basic_consume(queue=self.server, on_message_callback=self.on_request)
        self.channel.start_consuming()