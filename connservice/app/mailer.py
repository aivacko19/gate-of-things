import json
import logging

import bytescoder
import router

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

def receive(ch, method, properties, body):
    body_string = body.decode('utf-8')
    logging.info("Received: ", body)
    packet = json.loads(body, object_hook=bytescoder.as_bytes)

    if 'addr' not in packet:
        loggin.error("Malformed packet. Key 'addr' missing")
        return

    if 'response_queue_name' not in packet:
        loggin.error("Malformed packet. Key 'response_queue_name' missing")
        return

    sender = Sender(ch, packet[response_queue_name], packet[addr])
    router.route(sender, packet)

def send(ch, packet):
    if 'addr' not in packet:
        loggin.error("Malformed packet. Key 'addr' missing")
        return

    if 'response_queue_name' not in packet:
        loggin.error("Malformed packet. Key 'response_queue_name' missing")
        return

    response_queue_name = packet['response_queue_name']
    body = json.dumps(packet, cls=bytescoder.BytesEncoder).encode('utf-8')
    ch.basic_publish(exchange='', routing_key=response_queue_name, body=body)
    logging.info("Sent back to queue", response_queue_name)