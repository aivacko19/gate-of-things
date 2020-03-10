import pika
import threading
import json

connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
channel = connection.channel()


def callback(ch, method, properties, body):
    body_string = body.decode('utf-8')
    print(body_string)

channel.queue_declare(queue='auth-service')
channel.queue_declare(queue='response-queue')
channel.basic_consume(queue='response-queue', on_message_callback=callback, auto_ack=True)


thread = threading.Thread(
        target=channel.start_consuming,
        daemon=True)
thread.start()

while True:

    body = input("Insert Packet: ")
    print(body)
    packet = json.loads(body)
    packet['response_queue_name'] = 'response-queue'
    body = json.dumps(packet).encode('utf-8')
    channel.basic_publish(exchange='', routing_key='auth-service', body=body)