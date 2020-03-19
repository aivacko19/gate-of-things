#!/usr/bin/env python3

import socket
import selectors
import logging

class Gateway():
    def __init__(self, host, protocol, mail):
        self.host = host
        self.protocol = protocol
        self.mail = mail
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

    def sock2addr(self, socket):
        pair = socket.getpeername()
        return f"{pair[0]}:{str(pair[1])}"

    def disconnect_user(self, user_reference):
        packet = {'disconnect': True}
        self.mail.put(user_reference, packet)
        if user_reference in self.connections:
            del self.connections[user_reference]

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
                        user_reference = self.sock2addr(client)

                        if mask is selectors.EVENT_READ:
                            logging.info(f'Reading from {user_reference}')

                            try:
                                data = client.recv(4096)
                            except BlockingIOError:
                                # Resource temporarily unavailable (errno EWOULDBLOCK)
                                continue
                            if not data:
                                self.disconnect_user(user_reference)
                                self.unregister(client)
                                continue

                            while data:
                                self.protocol.append(stream, data)
                                data = self.protocol.load_packet(stream)
                                if self.protocol.still_loading(stream):
                                    break

                                logging.info(f'Parsing {user_reference}')
                                packet, error = self.protocol.parse(stream)
                                if error:
                                    self.disconnect_user(user_reference)
                                    self.register_write(client, packet)
                                    break

                                self.mail.put(user_reference, packet)

                            if (self.socket_alive(client) 
                                and not self.protocol.still_loading(stream)):
                                self.unregister(client, close=False)

                        elif mask is selectors.EVENT_WRITE:
                            data = self.protocol.peek(stream, 4096)
                            try:
                                sent = client.send(data)
                            except BlockingIOError:
                                # Resource temporarily unavailable (errno EWOULDBLOCK)
                                continue
                            
                            self.protocol.position(stream, sent)
                            if not self.protocol.empty(stream):
                                continue

                            if self.socket_alive(client):
                                self.register_read(client)
                            else:
                                self.unregister(client)


                addr, packet = self.mail.get()
                if packet:
                    commands = packet['commands']
                    del packet['commands']

                    if not self.socket_alive(addr):
                        if not commands.get('disconnect'):
                            self.disconnect_user(addr)
                        continue

                    client = self.get_socket(addr)
                    if commands.get('disconnect'):
                        self.disconnect_user(addr)

                    if commands.get('write'):
                        self.register_write(client, packet)
                    elif commands.get('disconnect'):
                        self.unregister(client, close=True)
                    elif commands.get('read'):
                        self.register_read(client)
                    else:
                        self.unregister(client, close=False)

                if self.stop_flag or not self.mail.is_alive():
                    break

        except KeyboardInterrupt:
            print("caught keyboard interrupt, exiting")
        finally:
            self.close()

    def stop(self):
        self.stop_flag = True