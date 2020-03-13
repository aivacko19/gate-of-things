import json
import logging

import bytescoder
import router

class GatewayListener:

    def __init__(self, rabbitmq, listener):
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(
                host=rabbitmq,
                connection_attempts=10,
                retry_delay=5,))
        self.channel = connection.channel()
        self.channel.queue_declare(queue=listener)
        self.channel.basic_consume(
            queue=listener,
            on_message_callback=self.on_response,
            auto_ack=True)
        self.thread = threading.Thread(
            target=self.channel.start_consuming,
            deamon=True)

    def on_response(self, ch, method, props, body):
        # logging.info(f"Received: {body}")
        packet = json.loads(body, object_hook=bytescoder.as_bytes)

        reply_queue = props.reply_to
        socket = props.correlation_id

        if packet.get('disconnect'):
            # logging.info(f"Disconnecting client {socket}")
            connections.delete_by_socket(socket)
            return

        conn = connections.get_by_socket(socket)
        router = Router(conn, socket, reply_queue, packet)
        packet = router.route()

        if not packet:
            # Create connection
            connections.new(packet, socket, reply_queue)
            packet = router.authenticating()

        body = json.dumps(packet, cls=bytescoder.BytesEncoder)
        ch.basic_publish(
            exchange='',
            routing_key=reply_queue,
            properties=pika.BasicProperties(
                correlation_id=socket)
            body=body)
        
    def run(self):
        self.thread.start()

    def is_alive(self):
        return self.thread.is_alive()