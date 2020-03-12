#!/usr/bin/env python3

import sys
import time
import pika
import logging
import json
import threading
import queue

import bytescoder

class Mail:

    def __init__(self, rabbitmq, remote_server):
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(
                host=rabbitmq,
                connection_attempts=10,
                retry_delay=5,))
        self.channel = connection.channel()
        result = self.channel.queue_declare(queue='', exclusive=True)
        self.callback_queue = result.method.queue
        self.channel.basic_consume(
            queue=self.callback_queue,
            on_message_callback=self.on_response,
            auto_ack=True)
        self.server = remote_server
        self.inbox = queue.Queue()
        self.outbox = queue.Queue()
        self.sender_thread = threading.Thread(
            target=self.request_loop,
            deamon=True)
        self.receiver_thread = threading.Thread(
            target=self.channel.start_consuming,
            deamon=True)

    def on_response(self, ch, method, props, body):
        packet = json.loads(body, object_hook=bytescoder.as_bytes)
        self.inbox.put((props.correlation_id, packet))

    def request_loop(self):
        while True:
            user_reference, packet = self.outbox.get()
            body = json.dumps(packet, cls=bytescoder.BytesEncoder)
            channel.basic_publish(
                exchange='', 
                routing_key=self.server,
                properties=pika.BasicProperties(
                    reply_to=self.callback_queue,
                    correlation_id=user_reference),
                body=body)

    def start(self):
        self.sender_thread.start()
        self.receiver_thread.start()

    def put(self, user_reference, packet):
        self.outbox.put((user_reference, packet))

    def get(self):
        if self.inbox.empty():
            return None, None
        return self.inbox.get()

    def is_alive(self):
        return (
            self.sender_thread.is_alive()
            and self.receiver_thread.is_alive())
