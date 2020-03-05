#!/usr/bin/env python3

import pika
import json
import bytescoder

connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
channel = connection.channel()
channel.queue_declare(queue='connection_service')

def callback(ch, method, properties, body):
    body = body.decode('utf-8')
    print('Recieved:', body)
    packet = json.loads(body, object_hook=bytescoder.as_bytes)

    addr = packet['addr']
    print('Client:', addr)
    response_queue_name = packet['response_queue_name']
    packet = {'addr': addr, 'write': False, 'disconnect': True}

    body = json.dumps(packet, cls=bytescoder.BytesEncoder).encode('utf-8')
    channel.basic_publish(exchange='', routing_key=response_queue_name, body=body)
    print(" [x] Sent back to queue", response_queue_name)

channel.basic_consume(queue='connection_service', on_message_callback=callback, auto_ack=True)

print(' [*] Waiting for messages. To exit press CTRL+C')
channel.start_consuming()