#!/usr/bin/env python3

import pika
import json
import sys
import os
import time

import bytescoder

# if len(sys.argv) != 2:
#     print("usage:", sys.argv[0], "<rabbitmq_host>")
#     sys.exit(1)

RABBITMQ_HOSTNAME = os.environ.get('RABBITMQ_HOSTNAME')
if not RABBITMQ_HOSTNAME:
    sys.exit(1)

connection = None
for i in range(10):
    try:
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOSTNAME))
    except pika.exceptions.AMQPConnectionError:
        print(f"Connection attempt number {i} failed")
        time.sleep(5)
    if connection:
        break
if not connection:
    sys.exit(1)



channel = connection.channel()
channel.queue_declare(queue='connection_service')

def callback(ch, method, properties, body):
    body = body.decode('utf-8')
    print('Recieved:', body)
    packet = json.loads(body, object_hook=bytescoder.as_bytes)

    if 'disconnect' in packet:
        if packet['disconnect']:
            return 

    addr = packet['addr']
    print('Client:', addr)
    response_queue_name = packet['response_queue_name']
    packet['write'] = True
    # packet = {'addr': addr, 'write': False, 'disconnect': True}

    body = json.dumps(packet, cls=bytescoder.BytesEncoder).encode('utf-8')
    channel.basic_publish(exchange='', routing_key=response_queue_name, body=body)
    print(" [x] Sent back to queue", response_queue_name)

channel.basic_consume(queue='connection_service', on_message_callback=callback, auto_ack=True)

print(' [*] Waiting for messages. To exit press CTRL+C')
channel.start_consuming()