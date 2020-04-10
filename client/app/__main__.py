#!/usr/bin/env python3

import time
import socket
import json
import sys
import threading

from protocols import mqtt as protocol

# if len(sys.argv) != 2:
#     print("usage:", sys.argv[0], "<rabbitmq_host>")
#     sys.exit(1)

with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
    sock.connect_ex(('192.168.99.100', 1887))
    sock.settimeout(15)

    while True:
        body = input("Insert Packet: ")
        if body == 'exit': break
        if body != 'pass':
            packet = json.loads(body) #, object_hook=bytescoder.as_bytes)
            stream = protocol.compose(packet)

            while not stream.empty():
                data = stream.output(4096)
                print(data)
                sent = sock.send(data)
                stream.update(sent)

        stream = protocol.new_stream()
        while stream.empty() or stream.still_loading():
            data = sock.recv(4096)
            if not data: raise RuntimeError("Server closed.")
            print(data)
            stream.append(data)
            stream.load()

        packet = protocol.parser.read(stream)
        print(packet)

            

