import time
import socket
import json
import sys

from protocols import mqtt as protocol

def write(socket, stream):
    try:
        # Should be ready to write
        sent = socket.send(stream.output(4096))
    except BlockingIOError:
        # Resource temporarily unavailable (errno EWOULDBLOCK)
        pass
    else:
        stream.update(sent)

def read(socket, stream):
    try:
        # Should be ready to read
        data = socket.recv(4096)
    except BlockingIOError:
        # Resource temporarily unavailable (errno EWOULDBLOCK)
        pass
    else:
        if data:
            stream.append(data)
            stream.load()
        else:
            raise RuntimeError("Peer closed.")
while True:

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect_ex(('192.168.99.100', protocol.port))

    body = input("Insert Packet: ")
    print(body)
    packet = json.loads(body)
    while body != "exit":
        stream = protocol.compose(packet)
        print(stream.output(4096))
        while True:
            write(sock, stream)
            if stream.empty():
                break

        while True:
            read(sock, stream)
            if not stream.still_loading():
                break
        print(stream.output(4096))
        packet = protocol.parse(stream)
        body = json.dumps(packet)
        print(body)
        body = input("Insert Packet: ")
        packet = json.loads(body)

    sock.close()

