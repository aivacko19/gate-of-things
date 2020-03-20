import json
import logging
import threading

import pika

from packet import Packet
from packet_db import PacketDB
import const

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
        pack_db = PacketDB.getInstance()
        # logging.info(f"Received: {body}")
        packet = json.loads(body, object_hook=bytescoder.as_bytes)

        reply_queue = props.reply_to
        cid = props.correlation_id
        pid = packet.get('id')
        ptype = packet.get('type')

        pack = pack_db.get(cid, pid)

        if ptype in ['subscribe', 'unsubscribe']:
            pack = pack_db.get(cid, pid)
            if pack:
                new_packet = {
                    'type': 'suback' if ptype == 'subscribe' else 'unsuback',
                    'topics': list()
                }
                for t in packet.get('topics'):
                    new_packet['topics'].append({
                        'code': const.PACKET_IDENTIFIER_IN_USE})
                body = json.dumps(new_packet, cls=bytescoder.BytesEncoder)
            else:
                pack_db.add(cid, pid, ptype)

            ch.basic_publish(
                exchange='',
                routing_key='',
                properties=pika.BasicProperties(
                    correlation_id=cid),
                body=body)
            return
        
    def run(self):
        # self.thread.start()
        self.channel.start_consuming()

    def is_alive(self):
        return self.thread.is_alive()