import json
import logging
import threading

import pika
from session import Session
from sesison_db import SessionDB

class SubscriptionListener:

    def __init__(self, rabbitmq, listener):
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(
                host=rabbitmq,
                connection_attempts=10,
                retry_delay=5,
                heartbeat=0,))
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue=listener)
        self.channel.basic_consume(
            queue=listener,
            on_message_callback=self.on_response,
            auto_ack=True)
        self.thread = threading.Thread(
            target=self.channel.start_consuming,
            daemon=True)

    def on_response(self, ch, method, props, body):
        session_db = SessionDB.getInstance()
        # logging.info(f"Received: {body}")
        packet = json.loads(body, object_hook=bytescoder.as_bytes)

        reply_queue = props.reply_to
        cid = props.correlation_id

        session = session_db.get(cid)
        sub_id = packet.get('properties').get('subscription_identifier') or 0

        for topic in packet.get('topics'):
            topic_filter = topic.get('filter')
            email = session.get_email()
            # Check authorization topic_filter email
            # If ok 
            sub = {
                'topic_filter': topic_filter,
                'subscription_id': sub_id,
                'max_qos': topic.get('max_qos'),
                'no_local': topic.get('no_local'),
                'retain_as_published': topic.get('retain_as_published'),
                'retain_handling': topic.get('retain_handling'),}
            session_db.add_sub(session, sub)

        body = json.dumps(packet, cls=bytescoder.BytesEncoder)
        ch.basic_publish(
            exchange='',
            routing_key=reply_queue,
            properties=pika.BasicProperties(
                correlation_id=socket),
            body=body)
        
    def run(self):
        # self.thread.start()
        self.channel.start_consuming()

    def is_alive(self):
        return self.thread.is_alive()