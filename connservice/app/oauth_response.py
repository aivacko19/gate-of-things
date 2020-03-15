import threading
import pika

import const

class OAuthListener:

    def __init__(self, rabbitmq, listener, connection_db):
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(
                host=rabbitmq,
                connection_attempts=10,
                retry_delay=5,))
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue=listener)
        self.channel.basic_consume(
            queue=listener,
            on_message_callback=self.on_response,
            auto_ack=True)
        self.thread = threading.Thread(
            target=self.channel.start_consuming,
            daemon=True)
        self.conn_db = connection_db

    def on_response(self, ch, method, props, body):
        email = body
        client_id = props.correlation_id
        conn = self.conn_db.get(client_id)

        packet = {'commands': {'write': True}}
        if email == 'none':
            packet['type'] = 'disconnect' if conn.verified() else 'connack'
            packet['code'] = const.NOT_AUTHORIZED
            packet['commands']['disconnect'] = True
        else:
            packet['type'] = 'auth' if conn.verified() else 'connack'
            packet['code'] = const.SUCCESS
            packet['commands']['read'] = True    
            if conn.verified() and conn.get_random_id():
                packet['properties'] = {
                    'assigned_client_identifier': conn.get_id()}


        body = json.dumps(packet)
        self.channel.basic_publish(
            exchange='',
            routing_key=conn.reply_queue,
            properties=pika.BasicProperties(
                correlation_id=conn.socket,),
            body=user_reference)

        if email == 'none': return

        if conn.verified():
            # Set new email and check subscription authorization
            pass
        else:
            conn.set_email(email)
            # Create new Session
        
    def run(self):
        self.thread.start()

    def is_alive(self):
        return self.thread.is_alive()