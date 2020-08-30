import os
import sys
import time
import pika
import json
import base64
import logging
import functools
import threading
import traceback

class BytesEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, bytes):
            return {'__bytes__': True, '__value__': base64.b64encode(obj).decode('utf-8')}
        return json.JSONEncoder.default(self, obj)

def as_bytes(dct):
    if '__bytes__' in dct:
        return base64.b64decode(dct['__value__'].encode('utf-8'))
    return dct

LOG_FORMAT = ('%(levelname) -10s %(asctime)s %(name)s %(funcName)s %(lineno)d: %(message)s')
LOGGER = logging.getLogger(__name__)

AMQP_URL = os.environ.get('AMQP_URL')
if not AMQP_URL:
    raise Exception('Evironment variable AMQP_URL not defined')

CONNECTION_PARAMETERS = pika.ConnectionParameters(
    host=AMQP_URL,
    connection_attempts=10,
    retry_delay=5,
    heartbeat=0,)

RPC_TIMEOUT_IN_SECONDS = 4

class AbstractService(threading.Thread):

    def __init__(self, queue=None):
        self._connection = None
        self._channel = None
        self._queue = queue
        self._closing = False
        self._consumer_tag = None
        self._consuming = False
        self._prefetch_count = 1
        self._lock = threading.Lock()
        self._lock.acquire()
        self.actions = {}
        threading.Thread.__init__(self)

    # Connect to AMQT server                         // abstract_service.py
    def connect(self):

        # Establish connection
        self._connection = pika.BlockingConnection(CONNECTION_PARAMETERS)
        self._channel = self._connection.channel()

        if self._queue:
            # Declare shared service queue
            self._channel.queue_declare(queue=self._queue)
        else:
            # Declare exclusive anonymous queue
            result = self._channel.queue_declare(queue='', exclusive=True)
            self._queue = result.method.queue
        LOGGER.info('Connected to AMQT Server. Queue name: %s', self._queue)

        # Enable publishing 
        self._lock.release()

    # Receiving requests manually
    def consume(self, blocking=True):
        method, properties, body = None, None, None

        if blocking:
            for methodB, propertiesB, bodyB in self._channel.consume(self._queue):
                method = methodB
                properties = propertiesB
                body = bodyB
                break
        else:
            method, properties, body = self._channel.basic_get(self._queue)

        if not body: return None, None

        # Decode request
        request_string = body.decode('utf-8')
        request = json.loads(request_string, object_hook=as_bytes)
        return request, properties.correlation_id

    # Overriding thread method run() executing when thread starts                       // abstract_service.py
    def run(self):

        # Connect to AMQP server
        self.connect()

        # Get request from AMQP queue, blocking with 10s timeout
        for method, properties, body in self._channel.consume(self._queue, inactivity_timeout=10):

            # Process request if arrived
            if bool(method or properties or body):

                # Decode request
                request_string = body.decode('utf-8')
                request = json.loads(request_string, object_hook=as_bytes)
                LOGGER.info('Received request: %s', request_string)

                # Extract command from request, get service action from self.actions (overriden by child class)
                command = request.get('command')
                action = self.actions.get(command)
                if not action: continue
                del request['command']

                # Executing actions safely, methods defined by child class
                try:
                    self.prepare_action(request, properties)
                    response = action(request, properties)
                    self.reply_to_sender(response, properties)
                except Exception as e:
                    LOGGER.error(traceback.format_exc())

                # Sending message acknowledgement to RabbitMQ server
                self._channel.basic_ack(method.delivery_tag)

            # Cancel connection and stop thread if interrupted
            if self._closing: 
                self._channel.cancel()
                break
         
    # Publish request to service queue                         // abstract_service.py
    def publish(self, request, queue, correlation_id=None, reply_queue=None):
        if self.dummy_messenger:
            self.dummy_messenger.publish(request, queue, correlation_id)
            return

        # Block if not connected to AMQP server
        self._lock.acquire()
        self._lock.release()

        # Prepare request
        request_string = json.dumps(request, cls=BytesEncoder)
        body = request_string.encode('utf-8')
        properties = pika.BasicProperties(
            content_type='text/json', 
            reply_to=reply_queue or self._queue,
            correlation_id=correlation_id,)
        LOGGER.info('Publishing on queue: %s, request: %s', queue, request_string)

        # Create method object from RabbitMQ basic publish with ready parameters
        publish_method = functools.partial(self._channel.basic_publish,
            exchange='',
            routing_key=queue,
            body=body,
            properties=properties)

        if self.is_alive() and threading.currentThread().getName() != self.getName():
            # Execute method from another thread
            self._connection.add_callback_threadsafe(publish_method)
        else:
            # Execute method directly
            publish_method()

    # Remote procedure call to service                   // abstract_service.py
    def rpc(self, request, queue, correlation_id=None):
        if self.dummy_messenger:
            return self.dummy_messenger.rpc(request, queue, correlation_id)

        # Creating a temporary queue for receiving result of request
        result = self._channel.queue_declare(queue='', exclusive=True)
        temp_queue = result.method.queue

        # Publish request to service with temporary queue to respond to 
        self.publish(request, queue, correlation_id, temp_queue)

        # Poll temporary queue for response until timeout expires
        body = None
        for i in range(RPC_TIMEOUT_IN_SECONDS * 5):
            time.sleep(0.2)
            method, properties, body = self._channel.basic_get(temp_queue)
            if body: break
        if not body:
            raise Exception("Timeout expired. Service %s not available" % queue)

        # Decode response
        response_string = body.decode('utf-8')
        response = json.loads(response_string, object_hook=as_bytes)
        LOGGER.info('Received request: %s', response_string)
        return response

    def prepare_action(self, request, properties):
        pass

    def reply_to_sender(self, response, properties):
        if response:
            self.publish(request=response, 
                         queue=properties.reply_to, 
                         correlation_id=properties.correlation_id,)
 
    def close(self):
        self._closing = True