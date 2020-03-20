import threading
import pika
import logging
import json

import const

from session_proxy import SesisonProxy
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
        session_proxy = SessionProxy.getInstance()
        conn_db = ConnectionDB.getInstance()
        try:
            logging.info("Start verification")
            email = body.decode("utf-8")
            client_id = props.correlation_id
            logging.info(f"Email: {email}, client_id: {client_id}")
            conn = conn_db.get(client_id)

            packet = {'commands': {'write': True}, 'properties': {}}
            if email == 'none':
                packet['type'] = 'disconnect' if conn.verified() else 'connack'
                packet['code'] = const.NOT_AUTHORIZED
                packet['commands']['disconnect'] = True
            else:
                packet['code'] = const.SUCCESS
                packet['commands']['read'] = True
                if conn.verified():
                    packet['type'] = 'auth'
                    packet['properties']['authentication_method'] = 'OAuth2.0'
                    if conn.get_email() != email:
                        session_proxy.publish('reauth', conn.get_id(), email)
                else:
                    packet['type'] = 'connack'

                    session = session_proxy.get(conn.get_id())
                    if session and not conn.get_clean_start():
                        packet['session_present'] = True
                        if session.get('email') != email:
                            session_proxy.reauth(conn.get_id(), email)
                    else:
                        packet['session_present'] = False
                        session_proxy.add(conn.get_id(), email)

                    if conn.get_random_id():
                        packet['properties']['assigned_client_identifier'] = conn.get_id()

            if email:
                conn.set_email(email)
                conn_db.update_email(conn)

            socket, reply_queue = conn.get_socket()
            body = json.dumps(packet)
            ch.basic_publish(
                exchange='',
                routing_key=reply_queue,
                properties=pika.BasicProperties(
                    correlation_id=socket,),
                body=body)

        except Exception as e:
            logging.error(e)
        
    def run(self):
        self.thread.start()
        logging.info("Started Thread for OAuthListener")

    def is_alive(self):
        return self.thread.is_alive()