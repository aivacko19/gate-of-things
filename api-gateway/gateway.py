#!/usr/bin/env python3

import socket
import selectors

class Selector(selectors.DefaultSelector()):
    def __init__(self):
        super().__init__()

    def register_listener(self, socket):
        socket.setblocking(False)
        self.register(socket, selectors.EVENT_READ, data=None)

    def register_client(self, socket, message, mask):
        try:
            self.get_key(socket)
            self.modify(socket, mask, data=message)
        except KeyError as e:
            socket.setblocking(False)
            self.register(socket, mask, data=message)

    def register_read(self, socket, message):
        self.register_client(socket, message, selectors.EVENT_READ)

    def register_write(self, socket, message):
        self.register_client(socket, message, selectors.EVENT_WRITE)

def start_listening(host, protocol, request_queue, response_queue):

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        listener.bind((host, protocol.get_port()))
        listener.listen()

        with Selector() as selector:

            selector.register_listener(listener)
            connections = {}  

            try:
                while True:
                    events = selector.select(timeout=0)
                    for key, mask in events:

                        if key.data is None:
                            client, addr = listener.accept()
                            fd = client.fileno()
                            connections[fd] = client
                            message = protocol.create(fd, addr)
                            selector.register_read(client, message)

                        else:
                            client = key.fileobj
                            message = key.data

                            if mask is selectors.EVENT_READ:
                                finished = read(client, message)
                                if finished:
                                    if message.disconnect:
                                        selector.register_write(client, message)
                                    else:
                                        try:
                                            selector.unregister(client)
                                        except Exception as e:
                                            print(
                                                f"error: selector.unregister() exception for",
                                                f"{client.getpeername()}: {repr(e)}",
                                            )  
                                        request_queue.put(message.request)

                            elif mask is selectors.EVENT_WRITE:
                                finished = write(client, message)
                                if finished:
                                    fd = client.fileno()
                                    if message.disconnect:
                                        try:
                                            selector.unregister(client)
                                        except Exception as e:
                                            print(
                                                f"error: selector.unregister() exception for",
                                                f"{client.getpeername()}: {repr(e)}",
                                            ) 
                                        del connections[fd]
                                        try:
                                            client.close()
                                        except OSError as e:
                                            print(
                                                f"error: socket.close() exception for",
                                                f"{client.getpeername()}: {repr(e)}",
                                            )
                                    else:
                                        message = protocol.create(fd, client.getpeername())
                                        selectors.register_read(client, message)

                    if not response_queue.empty():
                        response = response_queue.get()
                        message = protocol.create(response)
                        client = connections[message.fd]
                        register_write(client, message)

            except KeyboardInterrupt:
                print("caught keyboard interrupt, exiting")
            finally:
                for fd, client in connections:
                    try:
                        selector.unregister(client)
                    except Exception as e:
                        print(
                            f"error: selector.unregister() exception for",
                            f"{client.getpeername()}: {repr(e)}",
                        ) 
                    try:
                        client.close()
                    except OSError as e:
                        print(
                            f"error: socket.close() exception for",
                            f"{client.getpeername()}: {repr(e)}",
                        )

def read(socket, message):
    try:
        # Should be ready to read
        data = socket.recv(4096)
    except BlockingIOError:
        # Resource temporarily unavailable (errno EWOULDBLOCK)
        pass
    else:
        if data:
            message.read(data)
        else:
            raise RuntimeError("Peer closed.")

def write(socket, message):
    data = message.get_buffer()
    if data:
        try:
            # Should be ready to write
            sent = sock.send(buf)
        except BlockingIOError:
            # Resource temporarily unavailable (errno EWOULDBLOCK)
            pass
        else:
            message.update_buffer(sent)