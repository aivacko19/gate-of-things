#!/usr/bin/env python3

import socket
import selectors

class Selector(selectors.DefaultSelector()):
    def __init__(self, protocol):
        self.protocol = protocol
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

    def register_new(self, socket):
        stream = self.protocol.create_stream()
        self.register_read(socket, stream)

def start_listening(host, protocol, request_queue, response_queue):

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        listener.bind((host, protocol.get_port()))
        listener.listen()

        with Selector(protocol) as selector:
            selector.register_listener(listener)
            connections = {}  

            try:
                while True:
                    events = selector.select(timeout=0)
                    for key, mask in events:

                        if key.data is None:
                            client, addr = listener.accept()
                            connections[client.fileno()] = client
                            selector.register_new(client)

                        else:
                            client = key.fileobj
                            stream = key.data # message = key.data

                            if mask is selectors.EVENT_READ:
                                read(client, stream)
                                if stream.is_loading():
                                    continue

                                unregister(selector, client, close=False)

                                message = protocol.create_message()
                                message.read(stream)

                                action = message.get_action()
                                if action == 'wait':
                                    request = message.get_request()
                                    request['fd'] = client.fileno()
                                    request_queue.put(request)
                                elif action in ['write', 'disconnect']:
                                    stream = protocol.create_stream()
                                    message.write(stream)
                                    self.register_write(client, stream)
                                    if action == 'disconnect':
                                        del connections[client.fileno()]
                                else:
                                    self.register_new(client)

                            elif mask is selectors.EVENT_WRITE:
                                write(client, stream) # finished = write(client, message)
                                if not stream.empty():
                                    continue

                                if not connections.has_key(client.fileno()):
                                    unregister(selector, client)
                                else:
                                    selector.register_new(client)

                    if not response_queue.empty():
                        response = response_queue.get()

                        fd = response['fd']
                        if not connections.has_key[fd]:
                            continue
                        client = connections[fd]

                        message = protocol.create_message()
                        message.load_response(response)

                        action = message.get_action()
                        if action == 'wait':
                            request = message.get_request()
                            request['fd'] = client.fileno()
                            request_queue.put(request)
                        elif action in ['write', 'disconnect']:
                            stream = protocol.create_stream()
                            message.write(stream)
                            self.register_write(client, stream)
                            if action == 'disconnect':
                                del connections[client.fileno()]
                        else:
                            self.register_new(client)

            except KeyboardInterrupt:
                print("caught keyboard interrupt, exiting")
            finally:
                for fd, client in connections:
                    unregister(selector, client)

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

def unregister(selector, socket, close=True):
    try:
        selector.unregister(socket)
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