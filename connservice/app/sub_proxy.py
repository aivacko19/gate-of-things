import sys
import os
import logging
import json
import threading

import pika
import bytescoder
import connection_db

class SubscriptionProxy:

    __instance = None

    @staticmethod
    def initInstance(rabbitmq, recepient):
        SubscriptionProxy.__instance = SubscriptionProxy(rabbitmq, recepient)

    @staticmethod
    def getInstance():
        return SubscriptionProxy.__instance

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
        thread = threading.Thread(
            target=channel.start_consuming,
            daemon=True)
        thread.start()

        logging.info(f"Created SubscriptionProxy publishing on queue {remote_server}")

    def publish(self, cid, packet):
        logging.info(f"Publishing Packet to Session for {cid}")
        body = json.dumps(packet, cls=bytescoder.BytesEncoder)
        self.correlation_id = cid
        self.channel.basic_publish(
            exchange='',
            routing_key=self.recepient,
            properties=pika.BasicProperties(
                reply_to=self.callback_queue,
                correlation_id=cid),
            body=body)

    def on_response(self, ch, method, props, body):
        db = connection_db.getInstance()
        conn = db.get(props.correlation_id)
        socket, reply_queue = conn.get_socket()
        ch.basic_publish(
            exchange='',
            routing_key=reply_queue,
            properties=pika.BasicProperties(
                correlation_id=socket),
            body=body)

    