#!/usr/bin/env python3

import pika
import json
import libserver

connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
channel = connection.channel()
channel.queue_declare(queue='microservice')

def callback(ch, method, properties, body):
  print('Recieved:', body)
  body = json.loads(body)
  fd = body['fd']
  addr = body['addr']
  request = body['request']
  jsonheader = body['jsonheader']
  print('request:', request)
  obj = libserver.Message(fd, addr)
  obj.request = request
  obj.jsonheader = jsonheader
  obj.create_response()
  body = {'fd': fd, 'addr': addr, 'response': obj.get_send_buffer().hex()}
  body = json.dumps(body)
  channel.basic_publish(exchange='', routing_key='api-gateway', body=body)
  print(" [x] Sent 'Hello World!'")

channel.basic_consume(queue='microservice', on_message_callback=callback, auto_ack=True)

print(' [*] Waiting for messages. To exit press CTRL+C')
channel.start_consuming()