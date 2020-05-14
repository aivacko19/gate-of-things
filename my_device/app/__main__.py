#!/usr/bin/env python3

import time
import socket
import json
import sys
import threading
import os
import logging
from urllib import parse
from base64 import b64decode
from base64 import b64encode
import nacl.encoding
import nacl.signing
import nacl.exceptions
from cryptography.hazmat.primitives.asymmetric import ed25519

from protocols import mqtt as protocol




def reading(server_socket, ac):

    while True:
        stream = protocol.new_stream()
        while stream.empty() or stream.still_loading():
            data = server_socket.recv(4096)
            if not data: raise RuntimeError("Server closed.")
            print(data)
            stream.append(data)
            stream.load()

        packet = protocol.parser.read(stream)
        payload = packet['payload'].decode('utf-8')
        ctrl_packet = json.loads(payload)
        if 'power' in ctrl_packet:
            ac['power'] = ctrl_packet.get('power')
        if 'temperature' in ctrl_packet:
            ac['temperature'] = ctrl_packet.get('temperature')

        print(packet)


DEVICE_NAME = 'my-device'
URI = '192.168.99.100'
TOKEN_FORMAT = "SharedAccessSignature sig=%s&se=%s&skn=%s&sr=%s"
PRIVATE_KEY = "dtcMIuk6BBAu1oI7heg26kvoiLoSMVYGoRGrrOVq09HGIkgOERc9sbJ9I7Wf8dhzskn5MDbFizPb+0lm6WN5Nw=="
PUBLIC_KEY = "xiJIDhEXPbGyfSO1n/HYc7JJ+TA2xYsz2/tJZuljeTc="

if len(sys.argv) != 3:
    print("usage:", sys.argv[0], "<host_addres>", "<host_port>")
    sys.exit(1)

with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
    sock.connect_ex((sys.argv[1], int(sys.argv[2])))

    resource_uri = URI + '/devices/' + DEVICE_NAME
    url_encoded_resource_uri = parse.quote_plus(resource_uri)
    policy = 'ed25519'
    expiry = str(int(time.time()) + 60)
    message = '%s\n%s' % (url_encoded_resource_uri, expiry)
    private_key = ed25519.Ed25519PrivateKey.from_private_bytes(b64decode(PRIVATE_KEY)[:32])
    message_bin = message.encode('utf-8')
    print(message_bin)
    signature_bin = private_key.sign(message_bin)
    print(signature_bin)
    signature = b64encode(signature_bin).decode('utf-8')
    print(signature)
    token = (TOKEN_FORMAT % (signature, expiry, policy, url_encoded_resource_uri)).encode('utf-8')

    packet = {}
    packet['type'] = 'connect'
    packet['clean_start'] = False
    packet['keep_alive'] = 10
    packet['client_id'] = DEVICE_NAME
    packet['properties'] = {}
    packet['properties']['authentication_method'] = 'SignatureValidation'
    packet['username'] = resource_uri
    packet['password'] = token

    stream = protocol.compose(packet)
    while not stream.empty():
        data = stream.output(4096)
        print(data)
        sent = sock.send(data)
        stream.update(sent)


    data = sock.recv(4096)
    if not data: raise RuntimeError("Server closed.")
    print(data)

    ac = {
        'power': False,
        'temperature': 30
    }

    thread = threading.Thread(target=reading, args=(sock, ac), daemon=True)
    thread.start()

    curr_temp = 15

    while True:

        time.sleep(10)

        target_temp = 30
        if ac['power']:
            target_temp = ac['temperature']

        if curr_temp < target_temp:
            curr_temp = curr_temp + 1
        elif curr_temp > target_temp:
            curr_temp = curr_temp - 1

        payload = "current temperature: %s" % curr_temp

        packet = {
            'type': 'publish',
            'dup': False,
            'qos': 0,
            'retain': False,
            'topic': 'device/%s' % DEVICE_NAME,
            'properties': {},
            'payload': payload.encode('utf-8'),
        }

        stream = protocol.compose(packet)
        while not stream.empty():
            data = stream.output(4096)
            print(data)
            sent = sock.send(data)
            stream.update(sent)

sys.exit(0)

