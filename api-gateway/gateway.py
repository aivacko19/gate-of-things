#!/usr/bin/env python3

import socket
import selectors

class Selector(selectors.DefaultSelector()):
    def __init__(self):
        super().__init__()

    def register_listener(self, socket):
        socket.setblocking(False)
        self.register(socket, selectors.EVENT_READ, data=None)

    def register_client(self, socket, stream, mask):
        try:
            self.get_key(socket)
            self.modify(socket, mask, data=stream)
        except KeyError as e:
            socket.setblocking(False)
            self.register(socket, mask, data=stream)

    def register_read(self, socket, stream):
        self.register_client(socket, stream, selectors.EVENT_READ)

    def register_write(self, socket, stream):
        self.register_client(socket, stream, selectors.EVENT_WRITE)

class Gateway():
    def __init__(self, host, protocol, request_queue, response_queue):
        self.host = host
        self.protocol = protocol
        self.request_queue = request_queue
        self.response_queue = response_queue
        self.listener = None
        self.selector = None
        self.connections = {}

    def close(self):
        if len(self.connections) > 0
            for addr, client in self.connections:
                self.unregister(client)
            self.connections = {}
        if self.selector is not None:
            if self.listener is not None:
                self.unregister(self.listener)
            self.selector.close()
            self.selector = None
        if self.listener is not None:
            self.listener.close()
            self.listener = None

    def register_read(self, socket):
        stream = self.protocol.create_stream()
        self.selector.register_read(socket, stream)

    def register_write(self, socket, packet):
        stream = protocol.create_stream()
        self.protocol.write(packet, stream)
        self.selector.register_write(client, stream)

    def unregister(self, socket, close=True):
    try:
        self.selector.unregister(socket)
        if close:
            socket.close()
    except OSError as e:
        print(
            f"error: socket.close() exception for",
            f"{socket.getpeername()}: {repr(e)}",
        )
    except Exception as e:
        print(
            f"error: selector.unregister() exception for",
            f"{socket.getpeername()}: {repr(e)}",
        )

    def get_socket(self, addr):
        return self.connections[addr]

    def put_socket(self, socket):
        addr = self.sock2addr(socket)
        self.connections[addr] = client

    def socket_alive(self, addr):
        if type(addr) is not str:
            addr = self.sock2addr(addr)
        return self.connections.has_key(addr)

    def kill_socket(self, addr):
        del self.connections[addr]

    def sock2addr(self, socket):
        pair = socket.getpeername()
        return f"{pair[0]}:{str(pair[1])}"

    def start_listening():
        listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        listener.bind((self.host, self.protocol.get_port()))
        listener.listen()
        selector = Selector()
        selector.register_listener(listener)

        try:
            while True:
                events = self.selector.select(timeout=0)
                for key, mask in events:

                    if key.data is None:
                        client, addr = listener.accept()
                        self.put_socket(client)
                        self.register_read(client)

                    else:
                        client = key.fileobj
                        stream = key.data

                        if mask is selectors.EVENT_READ:
                            read(client, stream)
                            if stream.is_loading():
                                continue

                            self.unregister(client, close=False)

                            packet = self.protocol.read(stream)
                            packet['addr'] = self.sock2addr(client)
                            self.request_queue.put(packet)

                        elif mask is selectors.EVENT_WRITE:
                            write(client, stream)
                            if not stream.empty():
                                continue

                            if not self.socket_alive(client):
                                self.unregister(selector, client)
                            else:
                                self.register_read(client)

                if not self.response_queue.empty():
                    packet = self.response_queue.get()
                    disconnect_flag = packet.has_key['disconnect']
                    if disconnect_flag:
                        disconnect_flag = packet['disconnect']
                    write_flag = packet.has_key['write']
                    if write_flag:
                        write_flag = packet['write']

                    addr = packet['addr']
                    if not self.socket_alive(addr):
                        if not disconnect_flag:
                            packet = {'type': 0, 'addr': addr}
                            self.request_queue.put(packet)
                        continue
                    client = self.get_socket(addr)
                    if disconnect_flag:
                        self.kill_socket(addr)

                    if write_flag:
                        self.register_write(client, packet)
                    elif not disconnect_flag:
                        self.register_read(client)
                    else:
                        self.unregister(selector, client)

        except KeyboardInterrupt:
            print("caught keyboard interrupt, exiting")
        finally:
            self.close()

def read(socket, stream):
    try:
        # Should be ready to read
        data = socket.recv(4096)
    except BlockingIOError:
        # Resource temporarily unavailable (errno EWOULDBLOCK)
        pass
    else:
        if data:
            stream.load(data)
        else:
            raise RuntimeError("Peer closed.")

def write(socket, stream):
    try:
        # Should be ready to write
        sent = socket.send(stream.output(4096), 4096)
    except BlockingIOError:
        # Resource temporarily unavailable (errno EWOULDBLOCK)
        pass
    else:
        stream.update(sent)