import json
import logging
import threading

import pika
from session import Session
from sesison_db import SessionDB

class GatewayListener:

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
        if not session or packet.get('clean_start'):
            email = packet.get('email')
            session_db.add(Session((cid, email)))
            return

        if 'email' in packet:
            # Check subs authorization
            return

        ptype = packet.get('type')
        if ptype in ['subscribe, un']


        if packet.get('disconnect'):
            # logging.info(f"Disconnecting client {socket}")
            self.conn_db.delete_by_socket(socket, reply_queue)
            return

        conn = self.conn_db.get_by_socket(socket, reply_queue)
        logging.info(f"Connection info: id={conn.get_id()}, email={conn.get_email()}")
        router = request_router.RequestRouter(conn, packet)
        packet = router.route()

        if not packet:
            if not conn.get_random_id():
                other_conn = self.conn_db.get(conn.get_id())
                if other_conn:
                    other_socket, other_reply_queue = other_conn.get_socket()
                    packet = {
                        'type': 'disconnect',
                        'code': const.SESSION_TAKEN_OVER,
                        'commands' : {
                            'write': True,
                            'disconnect': True}}
                    body = json.dumps(packet, cls=bytescoder.BytesEncoder)
                    ch.basic_publish(
                        exchange='',
                        routing_key=other_reply_queue,
                        properties=pika.BasicProperties(
                            correlation_id=other_socket),
                        body=body)
                    self.conn_db.delete(conn.get_id())

            self.conn_db.add(conn)
            packet = router.authenticating()

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