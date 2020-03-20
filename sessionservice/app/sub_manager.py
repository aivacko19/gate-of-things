import json
import logging
import threading

import pika
from session import Session
from session import Subscripion
from sesison_db import SessionDB
import const
import bytescoder

class SubscriptionManager:

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
        db = SessionDB.getInstance()
        # logging.info(f"Received: {body}")
        request = json.loads(body, object_hook=bytescoder.as_bytes)

        reply_queue = props.reply_to
        cid = props.correlation_id

        session = db.get(cid)
        command = request.get('type')
        response = {}

        if command == 'subscribe':
            email = session.get_email()
            sub_id = request.get('properties').get('subscription_identifier') or 0


            topics = list()
            for topic in request.get('topics'):
                topic_filter = topic.get('filter')

                authorized = True
                # Authorize (email, topic_filter)
                if not authorized:
                    topics.append({'code': const.NOT_AUTHORIZED})
                    continue

                sub = Subscripion(sub_id=sub_id, packet=topic)
                db.add_sub(session, sub)
                topics.append({'code': topic.get('max_qos')})

            response['type'] = 'suback'
            response['id'] = request.get('id')
            response['topics'] = topics

        elif command == 'unsubscribe':
            topics = list()
            for topic in request.get('topics'):
                sub = db.get_sub(session, topic.get('filter'))
                if not sub:
                    topics.append({'code': const.NO_SUBSCRIPTION_EXISTED})
                    continue

                db.delete_sub(session, sub)
                topics.append({'code': const.SUCCESS})

            response['type'] = 'suback'
            response['id'] = request.get('id')
            response['topics'] = topics

        else:
            topics.append({'error': 'Wrong command.'})


        if response:
            body = json.dumps(response, cls=bytescoder.BytesEncoder)
            ch.basic_publish(
                exchange='',
                routing_key=reply_queue,
                properties=pika.BasicProperties(
                    correlation_id=cid),
                body=body)
        
    def run(self):
        # self.thread.start()
        self.channel.start_consuming()

    def is_alive(self):
        return self.thread.is_alive()