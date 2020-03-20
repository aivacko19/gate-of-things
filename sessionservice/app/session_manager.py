import json
import logging
import threading

import pika
from session import Session
from sesison_db import SessionDB

class SessionManager:

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
        command = request.get('command')
        response = {}

        if command == 'get':
            if not session:
                response['none'] = True
            else:
                response['email'] = session.get_email()
        elif command == 'add':
            if session:
                db.delete(session)
            db.add(Session((cid, request.get('email'))))
        elif command == 'reauth':
            email = request.get('email')
            for sub in session.get_subs():
                topic_filter = sub.get_topic_filter()

                authorized = True
                # Authorize (email, topic_filter)

                if not authorized:
                    db.delete_sub(session, sub)
            session.set_email(email)
            db.update_email(session)
        else:
            response['error'] = 'Wrong command'

        if response:
            body = json.dumps(response)
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