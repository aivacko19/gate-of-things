import pika
import mailer
import connections
import const

def callback(ch, method, properties, body):
    packet = json.loads(body.decode('utf-8'))
    client_id = packet['app_user_id']
    connection = connections.get(id=client_id)
    reauth = connection.is_authenticated()
    success = 'email' in packet
    new_packet = {
        'addr': connection.addr,
        'response_queue_name': connection.response_queue_name
    }
    if not reauth:
        new_packet['type'] = 'connack'
        if success:
            new_packet['code'] = const.SUCCESS
        else:
            new_packet['code'] = const.NOT_AUTHORIZED
    else:
        if success:
            new_packet['type'] = 'auth'
            new_packet['code'] = const.SUCCESS
        else:
            new_packet['type'] = 'disconnect'
            new_packet['code'] = const.NOT_AUTHORIZED
    if success:
        connection.set_email(packet['email'])
    mailer.send(ch, new_packet)

def main(channel, client_id):
    NUM_OF_ATTEMPTS = 10
    WAIT_TIME = 5
    RABBITMQ_HOSTNAME = os.environ.get('RABBITMQ_HOSTNAME')
    if not RABBITMQ_HOSTNAME:
        sys.exit(1)
    EMAIL_QUEUE_NAME = os.environ.get('EMAIL_QUEUE_NAME')
    if not EMAIL_QUEUE_NAME:
        sys.exit(1)

    connection = None
    for i in range(NUM_OF_ATTEMPTS):
        logging.info("Trying to connect to RabbitMQ")
        try:
            connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOSTNAME))
        except pika.exceptions.AMQPConnectionError:
            logging.info(f"Connection attempt number {i} failed")
            time.sleep(WAIT_TIME)
        if connection:
            logging.error("Exceeded number of attempts, exiting...")
            break
    if not connection:
        sys.exit(1)

    channel = connection.channel()
    channel.queue_declare(queue=EMAIL_QUEUE_NAME)
    channel.basic_consume(queue=EMAIL_QUEUE_NAME, on_message_callback=callback, auto_ack=True)
    channel.start_consuming()