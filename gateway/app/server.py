#!/usr/bin/env python3

import socket
import ssl
import selectors
import logging
import os 

LOGGER = logging.getLogger(__name__)

class Server:
    def __init__(self, host, port, safe_port):
        self.host = host
        self.port = port
        self.safe_port = safe_port
        self.stop_flag = False
        self.listener = None
        self.listener_safe = None
        self.selector = None

    def register_listener(self, listener):
        listener.setblocking(False)
        self.selector.register(listener, selectors.EVENT_READ, data=None)

    def register_client(self, socket, data, mask):
        try:
            self.selector.get_key(socket)
            self.selector.modify(socket, mask, data=data)
        except KeyError as e:
            socket.setblocking(False)
            self.selector.register(socket, mask, data=data)

    def register_read(self, socket, data):
        self.register_client(socket, data, selectors.EVENT_READ)

    def register_write(self, socket, data):
        self.register_client(socket, data, selectors.EVENT_WRITE)

    def unregister(self, socket):
        try:
            self.selector.get_key(socket)
            self.selector.unregister(socket)
        except KeyError:
            addr = socket.getpeername()
            LOGGER.error('Client %s:%s: %s', addr[0], addr[1], 'Socket not registered.')
        except Exception as e:
            addr = socket.getpeername()
            LOGGER.error('Client %s:%s: %s', addr[0], addr[1], e)

    def safe_close(self, socket):
        try:
            socket.close()
        except OSError as e:
            addr = socket.getpeername()
            LOGGER.error('Client %s:%s: %s', addr[0], addr[1], e)

    def close(self):
        if self.selector is not None:
            if self.listener is not None:
                self.unregister(self.listener)
            if self.listener_safe is not None:
                self.unregister(self.listener_safe)
            self.selector.close()
            self.selector = None
        if self.listener is not None:
            self.listener.close()
            self.listener = None
        if self.listener_safe is not None:
            self.listener_safe.close()
            self.listener_safe = None

    def get_empty_buffer(self):
        pass

    def get_data(self, buff):
        pass

    def set_position(self, buff):
        pass

    def is_empty(self, buff):
        pass

    def on_start(self):
        pass

    def on_connect(self, socket):
        pass

    def on_disconnect(self, socket):
        pass

    def on_read(self, socket, data, buff):
        pass

    def on_write(self, socket):
        pass

    def on_close(self):
        pass

    def in_loop_action(self):
        pass

    def disconnect(self, client):
        self.on_disconnect(client)
        self.unregister(client)
        self.safe_close(client)


    def start(self):
        context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
        context.load_cert_chain(certfile='cert.pem', keyfile='key.pem')

        self.selector = selectors.DefaultSelector()

        listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        listener.bind((self.host, self.port))
        listener.listen()
        self.register_listener(listener)
        self.listener = listener

        listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        listener.bind((self.host, self.safe_port))
        listener.listen()
        self.register_listener(listener)
        self.listener_safe = listener

        self.on_start()

        try:

            # Server loop                                                            // server.py
            while True:
                # Processing events from sockets
                events = self.selector.select(timeout=0)
                for key, mask in events: 
                    # Accept new connection from client
                    if key.data is None:
                        listener = key.fileobj
                        client, addr = listener.accept()
                        if listener == self.listener_safe: 
                            client = context.wrap_socket(client, server_side=True)
                        self.register_client(client, self.get_empty_buffer(), selectors.EVENT_READ)
                        self.on_connect(client)
                    else:
                        client, package_buffer = key.fileobj, key.data
                        # Read package from client
                        if mask is selectors.EVENT_READ:
                            try:
                                data = client.recv(4096)
                            except (BlockingIOError, ConnectionResetError):
                                continue
                            if not data: self.disconnect(client)
                            else: self.on_read(client, data, package_buffer)
                        # Write package to client
                        elif mask is selectors.EVENT_WRITE:
                            data = self.get_data(package_buffer, 4096)
                            try:
                                sent = client.send(data)
                            except BlockingIOError:
                                continue
                            self.set_position(package_buffer, sent)
                            if self.is_empty(package_buffer): self.on_write(client)
                # Additional processing in each iteration implemented by child class
                self.in_loop_action() 

                if self.stop_flag: break

        except KeyboardInterrupt:
            LOGGER.info('Caught keyboard interrupt, exiting...')
        finally:
            self.on_close()
            self.close()

    def stop(self):
        self.stop_flag = True