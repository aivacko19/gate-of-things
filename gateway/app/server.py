#!/usr/bin/env python3

import socket
import selectors
import logging
import os 

LOGGER = logging.getLogger(__name__)

class Server:
    def __init__(self, host):
        self.host = host
        self.stop_flag = False
        self.listener = None
        self.selector = None

    def register_listener(self):
        self.listener.setblocking(False)
        self.selector.register(self.listener, selectors.EVENT_READ, data=None)

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
            LOGGER.error('Client %s:%s: %s', socket.getpeername(), 'Socket not registered.')
        except Exception as e:
            LOGGER.error('Client %s:%s: %s', socket.getpeername(), e)

    def safe_close(self, socket):
        try:
            socket.close()
        except OSError as e:
            LOGGER.error('Client %s:%s: %s', socket.getpeername(), e)

    def close(self):
        if self.selector is not None:
            if self.listener is not None:
                self.unregister(self.listener)
            self.selector.close()
            self.selector = None
        if self.listener is not None:
            self.listener.close()
            self.listener = None

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

    def loop(self):
        pass

    def start(self):
        self.listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.listener.bind((self.host, self.protocol.port))
        self.listener.listen()
        self.selector = selectors.DefaultSelector()
        self.register_listener()
        self.on_start()

        try:
            while True:
                events = self.selector.select(timeout=0)
                for key, mask in events:

                    if key.data is None:
                        client, addr = self.listener.accept()
                        LOGGER.info('Client %s:%s - connecting', addr)
                        buff = self.get_empty_buffer()
                        self.register_client(socket, buff, selectors.EVENT_READ)
                        self.on_connect(socket)

                    else:
                        client = key.fileobj
                        buff = key.data

                        if mask is selectors.EVENT_READ:
                            LOGGER.info('Client %s:%s - reading', client.getpeername())

                            try:
                                data = client.recv(4096)
                            except BlockingIOError:
                                # Resource temporarily unavailable (errno EWOULDBLOCK)
                                continue
                            if not data:
                                LOGGER.info('Client %s:%s - disconnecting', client.getpeername())
                                self.on_disconnect(client)
                                self.unregister(client)
                                self.safe_close(client)
                                continue

                            self.on_read(client, data, buff)

                        elif mask is selectors.EVENT_WRITE:
                            LOGGER.info('Client %s:%s - writing', client.getpeername())

                            data = self.get_data(buff, 4096)

                            try:
                                sent = client.send(data)
                            except BlockingIOError:
                                # Resource temporarily unavailable (errno EWOULDBLOCK)
                                continue

                            self.set_position(buff, sent)

                            if not self.is_empty(buff):
                                continue
                            
                            self.on_write(client)

                self.loop()

                if self.stop_flag:
                    break

        except KeyboardInterrupt:
            LOGGER.info('Caught keyboard interrupt, exiting...')
        finally:
            self.on_close()
            self.close()

    def stop(self):
        self.stop_flag = True