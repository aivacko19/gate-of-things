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

class AmqpAgent(threading.Thread):

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
        threading.Thread.__init__(self)

    def connect(self):
        self._connection = pika.BlockingConnection(CONNECTION_PARAMETERS)
        self._channel = self._connection.channel()

        if self._queue:
            self._channel.queue_declare(queue=self._queue)
        else:
            result = self._channel.queue_declare(queue='', exclusive=True)
            self._queue = result.method.queue

        LOGGER.info('Connected to AMQT Server. Queue name: %s', self._queue)
        self._lock.release()

    def consume(self, blocking=True):
        obj_bin = None
        props = None
        if blocking:
            for method, properties, body in self._channel.consume(self._queue):
                obj_bin = body
                props = properties
                break
        else:
            method, properties, body = self._channel.basic_get(self._queue)
            obj_bin = body
            props = properties

        if not obj_bin: return None, None

        obj_str = obj_bin.decode('utf-8')
        obj = json.loads(obj_str, object_hook=as_bytes)
        return obj, props.correlation_id

    def run(self):

        self._is_active = True
        self.connect()

        for method, properties, body in self._channel.consume(self._queue, 
                                                              inactivity_timeout=10):
            
            if not (method or properties or body): 
                if self._closing: 
                    self._channel.cancel()
                    break
                
                continue

            obj_str = body.decode('utf-8')
            obj = json.loads(obj_str, object_hook=as_bytes)

            LOGGER.info('Received message: %s', obj_str)

            try:
                self.main(obj, properties)
            except Exception as e:
                LOGGER.error(traceback.format_exc())

            self._channel.basic_ack(method.delivery_tag)

            if self._closing: break
         
    def publish(self, obj, queue, correlation_id=None):

        self._lock.acquire()
        self._lock.release()

        obj_str = json.dumps(obj, cls=BytesEncoder)
        body = obj_str.encode('utf-8')

        LOGGER.info('Publishing on queue: %s, message: %s', queue, obj_str)

        properties = pika.BasicProperties(
            content_type='text/json', 
            reply_to=self._queue,
            correlation_id=correlation_id,)

        publish_method = functools.partial(
            self._channel.basic_publish,
            exchange='',
            routing_key=queue,
            body=body,
            properties=properties)

        if self.is_alive() and threading.currentThread().getName() != self.getName():
            self._connection.add_callback_threadsafe(publish_method)
        else:
            publish_method()

    def rpc(self, obj, queue, correlation_id=None):
        result = self._channel.queue_declare(queue='', exclusive=True)
        temp_queue = result.method.queue

        self._lock.acquire()
        self._lock.release()

        obj_str = json.dumps(obj, cls=BytesEncoder)
        body = obj_str.encode('utf-8')

        LOGGER.info('Publishing on queue: %s, message: %s', queue, obj_str)

        properties = pika.BasicProperties(
            content_type='text/json', 
            reply_to=temp_queue,
            correlation_id=correlation_id,)

        publish_method = functools.partial(
            self._channel.basic_publish,
            exchange='',
            routing_key=queue,
            body=body,
            properties=properties)

        if self.is_alive() and threading.currentThread().getName() != self.getName():
            self._connection.add_callback_threadsafe(publish_method)
        else:
            publish_method()

        body = None

        for i in range(20):
            time.sleep(0.2)
            method, properties, body = self._channel.basic_get(temp_queue)
            if body:
                break
        if not body:
            raise Exception("Timeout expired. Service %s not available" % queue)

        obj_str = body.decode('utf-8')
        obj = json.loads(obj_str, object_hook=as_bytes)

        LOGGER.info('Received message: %s', obj_str)

        return obj

    def main(self, request, properties):
        pass

    def close(self):
        self._closing = True