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
        self.rabbitmq = rabbitmq
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(
                host=rabbitmq,
                connection_attempts=10,
                retry_delay=5,))
        self.receiver_channel = self.connection.channel()
        result = self.receiver_channel.queue_declare(queue='', exclusive=True)
        self.callback_queue = result.method.queue
        self.receiver_channel.basic_consume(
            queue=self.callback_queue,
            on_message_callback=self.on_response,
            auto_ack=True)
        self.server = remote_server
        self.inbox = queue.Queue()
        self.outbox = queue.Queue()
        self.sender_thread = threading.Thread(
            target=self.request_loop,
            daemon=True)
        self.receiver_thread = threading.Thread(
            target=self.receiver_channel.start_consuming,
            daemon=True)

    def on_response(self, ch, method, props, body):
        packet = json.loads(body, object_hook=bytescoder.as_bytes)
        self.inbox.put((props.correlation_id, packet))

    def request_loop(self):
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(
                host=self.rabbitmq,
                connection_attempts=10,
                retry_delay=5,))
        channel = connection.channel()
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
