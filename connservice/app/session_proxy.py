import sys
import os
import logging
import json

import pika
import bytescoder

class SessionProxy:

    __instance = None

    @staticmethod
    def initInstance(rabbitmq, recepient):
        SessionProxy.__instance = SessionProxy(rabbitmq, recepient)

    @staticmethod
    def getInstance():
        return SessionProxy.__instance

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

        logging.info(f"Created SessionProxy publishing on queue {remote_server}")

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
        if self.correlation_id == props.correlation_id:
            self.packet = json.loads(body, object_hook=bytescoder.as_bytes)

    def get(self, cid):
        self.packet = None

        self.publish(cid, {
            'command': 'get'})

        while not self.packet:
            self.connection.process_data_events()

        if self.packet.get('none'): 
            return None

        return self.packet

    def add(self, cid, email):
        self.publish(cid, {
            'command': 'add',
            'email': email})

    def reauth(self, cid, email):
        self.publish(cid, {
            'command': 'reauth',
            'email': email})