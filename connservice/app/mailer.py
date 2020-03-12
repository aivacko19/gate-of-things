import json
import logging

import bytescoder
import router

class


class Sender:
    def __init__(self, channel, deliver_queue, client_addr):
        self.channel = channel
        self.deliver_queue = deliver_queue
        self.client_addr = client_addr

    def send(self, packet):
        packet['addr'] = self.client_addr
        body = json.dumps(packet, cls=bytescoder.BytesEncoder).encode('utf-8')
        self.channel.basic_publish(exchange='', routing_key=self.deliver_queue, body=body)
        logging.info("Sent back to queue", self.deliver_queue)

def receive(ch, method, props, body):
    # logging.info("Received: ", body)
    packet = json.loads(body, object_hook=bytescoder.as_bytes)

    reply_queue = props.reply_to
    socket = props.correlation_id

    if packet.get('disconnect'):
        # logging.info(f"Disconnecting client {socket}")
        connections.delete_by_socket(socket)
        return

    conn = connections.get_by_socket(socket)
    ptype = packet.get('type')
    if not conn:
        if ptype not in ['connect', 'pingreq']:
            
    elif not conn.verified



    sender = Sender(ch, reply_queue, socket)
    router = Router(sender, packet, packet['addr'])
    packet = router.route()

    body = json.dumps(packet, cls=bytescoder.BytesEncoder)
    ch.basic_publish(
        exchange='',
        routing_key=reply_queue,
        properties=pika.BasicProperties(
            correlation_id=socket)
        body=body)


def send(ch, packet):
    if 'addr' not in packet:
        loggin.error("Malformed packet. Key 'addr' missing")
        return

    if 'response_queue_name' not in packet:
        loggin.error("Malformed packet. Key 'response_queue_name' missing")
        return

    response_queue_name = packet['response_queue_name']
    del packet['response_queue_name']
    body = json.dumps(packet, cls=bytescoder.BytesEncoder).encode('utf-8')
    ch.basic_publish(exchange='', routing_key=response_queue_name, body=body)
    logging.info("Sent back to queue", response_queue_name)