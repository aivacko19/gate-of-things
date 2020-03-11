import pika
import queue

request_uri_queue = queue.Queue()

def callback(ch, method, properties, body):
    packet = json.loads(body.decode('utf-8'))
    request_uri_queue.put(packet)

def get_request_uri(channel, client_id):
    response_queue_name = 'connection_service_' + client_id
    channel.queue_declare(queue=response_queue_name)

    packet = {
        'app_user_id': client_id,
        'response_queue_name': response_queue_name
    }
    body = json.dumps(packet).encode('utf-8')
    auth_queue = os.environ.get('AUTH_QUEUE_NAME')
    channel.basic_publish(exchange='', routing_key=auth_queue, body=body)

    channel.basic_get(queue=response_queue_name, callback=callback, auto_ack=True)
    packet = request_uri_queue.get(block=True, timeout=20)
    return packet['redirect_uri']
