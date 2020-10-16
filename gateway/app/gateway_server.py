#!/usr/bin/env python3

import logging

import server

LOGGER = logging.getLogger(__name__)

class GatewayServer(server.Server):
    def __init__(self, host, protocol, mailer):
        self.protocol = protocol
        self.mailer = mailer
        self.connections = {}
        server.Server.__init__(self, host, protocol.port, protocol.port_safe)

    def get_empty_buffer(self):
        return self.protocol.new_stream()

    def get_data(self, buff, size):
        return self.protocol.peek(buff, size)

    def set_position(self, buff, position):
        self.protocol.position(buff, position)

    def is_empty(self, buff):
        return self.protocol.empty(buff)

    def on_start(self):
        pass

    def on_connect(self, socket):
        self.connections[socket.fileno()] = socket

    def on_disconnect(self, socket):
        file_descriptor = socket.fileno()
        self.mailer.publish_disconnect(file_descriptor)
        if file_descriptor in self.connections:
            del self.connections[file_descriptor]

    def on_read(self, socket, data, buff):
        disconnect = False
        file_descriptor = socket.fileno()
        while data:
            self.protocol.append(buff, data)
            data = self.protocol.load_packet(buff)

            if self.protocol.still_loading(buff): return
            LOGGER.info('Client %s - packet loaded', file_descriptor)

            packet, error = self.protocol.parse(buff)
            if error:
                LOGGER.info('Client %s - packet error, disconnecting', file_descriptor)
                disconnect = True
                buff = self.protocol.compose(packet)
                break

            self.mailer.publish_request(packet, file_descriptor)

        if disconnect:
            self.on_disconnect(socket)
            self.register_write(socket, buff)
        else:
            self.unregister(socket)

    def on_write(self, socket):
        if socket.fileno() in self.connections:
            buff = self.protocol.new_stream()
            self.register_read(socket, buff)
        else:
            self.unregister(socket)
            self.safe_close(socket)

    # Process response from Router                       // gateway_server.py
    def in_loop_action(self):
        # Get packet from mailer service
        packet, fd, state = self.mailer.get()
        if not (packet or fd or state): return

        # Delete file descriptor if client socket not valid or disconnected 
        client_socket = self.connections.get(fd)
        socket_invalid = False
        try:
            client_socket.getpeername()
        except (OSError, AttributeError):
            socket_invalid = True
        if socket_invalid or state == 'disconnect': 
            self.mailer.publish_disconnect(fd)
            if fd in self.connections: del self.connections[fd]
            if socket_invalid: return

        # Send package to client, after writing disconnect, listen or ignore
        if packet:
            package_buffer = self.protocol.compose(packet)
            self.register_write(client_socket, package_buffer)
        # Disconnect client socket
        elif state == 'disconnect':
            self.unregister(client_socket)
            self.safe_close(client_socket)
        # Listen on client socket
        elif state == 'read':
            package_buffer = self.protocol.new_stream()
            self.register_read(client_socket, package_buffer)
        # Ignore client
        else: self.unregister(client_socket)

    def on_close(self, packet):
        if len(self.connections) > 0:
            for client in self.connections.values():
                self.unregister(client)
            self.connections = {}

    # def get_socket(self, addr):
    #     return self.connections.get(addr)

    # def put_socket(self, socket):
    #     self.connections[socket.fileno()] = socket

    # def socket_alive(self, socket):
    #     return socket.fileno() in self.connections

    # def sock2addr(self, socket):
    #     pair = socket.getpeername()
    #     return f"{pair[0]}:{str(pair[1])}"
