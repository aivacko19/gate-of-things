import time
import socket
import json

from protocols.mqtt.protocol import Protocol

protocol = Protocol()


def write(socket, stream):
    try:
        # Should be ready to write
        sent = socket.send(stream.output(4096))
    except BlockingIOError:
        # Resource temporarily unavailable (errno EWOULDBLOCK)
        pass
    else:
        stream.update(sent)

while True:
    body = input("Insert Packet: ")
    packet = json.loads(body)

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect_ex(('localhost', protocol.get_port()))

    stream = protocol.create_stream()
    protocol.write(packet, stream)
    print(stream.output(4096))
    while True:
        write(sock, stream)
        if stream.empty():
            break

    time.sleep(10)

    sock.close()

