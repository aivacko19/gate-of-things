#!/usr/bin/env python3

import socket
import selectors
import logging

class Gateway():
    def __init__(self, host, protocol, request_queue, response_queue):
        self.host = host
        self.protocol = protocol
        self.request_queue = request_queue
        self.response_queue = response_queue
        self.stop_flag = False
        self.listener = None
        self.selector = None
        self.connections = {}

    def close(self):
        if len(self.connections) > 0:
            for client in self.connections.values():
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

    def register_listener(self):
        self.listener.setblocking(False)
        self.selector.register(self.listener, selectors.EVENT_READ, data=None)

    def register_client(self, socket, stream, mask):
        try:
            self.selector.get_key(socket)
            self.selector.modify(socket, mask, data=stream)
        except KeyError as e:
            socket.setblocking(False)
            self.selector.register(socket, mask, data=stream)

    def register_read(self, socket):
        stream = self.protocol.new_stream()
        self.register_client(socket, stream, selectors.EVENT_READ)

    def register_write(self, socket, packet):
        stream = self.protocol.compose(packet)
        self.register_client(socket, stream, selectors.EVENT_WRITE)

    def unregister(self, socket, close=True):
        try:
            try:
                self.selector.get_key(socket)
                self.selector.unregister(socket)
            except KeyError:
                a = 2 + 3
            finally:
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
        self.connections[addr] = socket

    def socket_alive(self, addr):
        if not isinstance(addr, str):
            addr = self.sock2addr(addr)
        return addr in self.connections

    def kill_socket(self, addr):
        if not isinstance(addr, str):
            addr = self.sock2addr(addr)
        del self.connections[addr]

    def sock2addr(self, socket):
        pair = socket.getpeername()
        return f"{pair[0]}:{str(pair[1])}"

    def start_listening(self):
        self.listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.listener.bind((self.host, self.protocol.port))
        self.listener.listen()
        self.selector = selectors.DefaultSelector()
        self.register_listener()

        try:
            while True:
                events = self.selector.select(timeout=0)
                for key, mask in events:

                    if key.data is None:
                        client, addr = self.listener.accept()
                        self.put_socket(client)
                        self.register_read(client)
                        logging.info(f'Got connection from {self.sock2addr(client)}')

                    else:
                        client = key.fileobj
                        stream = key.data

                        if mask is selectors.EVENT_READ:
                            logging.info(f'Reading from {self.sock2addr(client)}')
                            try:
                                self.read(client, stream)
                            except:
                                self.kill_socket(client)

                            if not self.socket_alive(client):
                                packet = {'addr': self.sock2addr(client),
                                          'disconnect': True}
                                self.request_queue.put(packet)
                                self.unregister(client)
                                continue

                            if self.protocol.busy(stream):
                                continue

                            self.unregister(client, close=False)

                            logging.info(f'Parsing {self.sock2addr(client)}')
                            packet = self.protocol.parse(stream)
                            packet['addr'] = self.sock2addr(client)
                            self.request_queue.put(packet)

                        elif mask is selectors.EVENT_WRITE:
                            try:
                                self.write(client, stream)
                            except:
                                self.kill_socket(client)

                            if not self.socket_alive(client):
                                packet = {'addr': self.sock2addr(client),
                                          'disconnect': True}
                                self.request_queue.put(packet)
                                self.unregister(client)
                                continue

                            if not self.protocol.empty(stream):
                                continue

                            if not self.socket_alive(client):
                                self.unregister(selector, client)
                            else:
                                self.register_read(client)

                if not self.response_queue.empty():
                    packet = self.response_queue.get()
                    read_flag = 'read' in packet
                    if read_flag:
                        read_flag = packet['read']
                    write_flag = 'write' in packet
                    if write_flag:
                        write_flag = packet['write']
                    wait_flag = 'wait' in packet
                    if wait_flag:
                        wait_flag = packet['wait']

                    addr = packet['addr']
                    if not self.socket_alive(addr):
                        if read_flag:
                            packet = {'addr': addr,
                                      'disconnect': True}
                            self.request_queue.put(packet)
                        continue
                    client = self.get_socket(addr)

                    if write_flag:
                        del packet['addr']
                        del packet['write']
                        if 'wait' in packet:
                            del packet['wait']
                        if 'read' in packet:
                            if not read_flag and not wait_flag:
                                self.kill_socket(addr)
                            del packet['read']
                        self.register_write(client, packet)
                    elif read_flag:
                        self.register_read(client)
                    elif not wait_flag:
                        self.kill_socket(addr)
                        self.unregister(client)
                if self.stop_flag:
                    break

        except KeyboardInterrupt:
            print("caught keyboard interrupt, exiting")
        finally:
            self.close()

    def read(self, socket, stream):
        try:
            # Should be ready to read
            data = socket.recv(4096)
        except BlockingIOError:
            # Resource temporarily unavailable (errno EWOULDBLOCK)
            pass
        else:
            if data:
                self.protocol.write(stream, data)
            else:
                raise RuntimeError("Peer closed.")

    def write(self, socket, stream):
        try:
            # Should be ready to write
            data = self.protocol.peek(stream, 4096)
            sent = socket.send(data)
        except BlockingIOError:
            # Resource temporarily unavailable (errno EWOULDBLOCK)
            pass
        else:
            self.protocol.position(stream, sent)

    def stop(self):
        self.stop_flag = True