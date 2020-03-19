import threading
import pika
import logging
import json

import const

from session_request import SesisonPublisher
from connection_db import ConnectionDB

class OAuthListener:

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
        logging.info(f"Created OAuthListener listening on queue {listener}")

    def on_response(self, ch, method, props, body):
        publisher = SessionPublisher.getInstance()
        conn_db = ConnectionDB.getInstance()
        try:
            logging.info("Start verification")
            email = body.decode("utf-8")
            client_id = props.correlation_id
            logging.info(f"Email: {email}, client_id: {client_id}")
            conn = conn_db.get(client_id)

            packet = {'commands': {'write': True}}
            if email == 'none':
                packet['type'] = 'disconnect' if conn.verified() else 'connack'
                packet['code'] = const.NOT_AUTHORIZED
                packet['commands']['disconnect'] = True
            else:
                packet['type'] = 'auth' if conn.verified() else 'connack'
                packet['code'] = const.SUCCESS
                packet['commands']['read'] = True
                packet['properties'] = {}
                if conn.verified():
                    packet['properties']['authentication_method'] = 'OAuth2.0'
                elif conn.get_random_id():
                    packet['properties']['assigned_client_identifier'] = conn.get_id()

            socket, reply_queue = conn.get_socket()
            body = json.dumps(packet)
            ch.basic_publish(
                exchange='',
                routing_key=reply_queue,
                properties=pika.BasicProperties(
                    correlation_id=socket,),
                body=body)

            if email == 'none': return

            conn.set_email(email)
            conn_db.update_email(conn)
            packet = {
                'email': email,
                'clean_start': conn.get_clean_start()}
            if conn.verified():
                # Check subscription authorization
                pass
            else:
                pass
                
            publisher.publish(conn.get_id(), packet)
        except Exception as e:
            logging.error(e)
        
    def run(self):
        self.thread.start()
        logging.info("Started Thread for OAuthListener")

    def is_alive(self):
        return self.thread.is_alive()