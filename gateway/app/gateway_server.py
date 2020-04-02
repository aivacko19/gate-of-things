#!/usr/bin/env python3

import socket
import selectors
import logging
import os

import server

LOGGER = logging.getLogger(__name__)

env = {
    'ROUTING_SERVICE': None
}

for key in env:
    service = os.environ.get(key)
    if not service:
        raise Exception('Environment variable %s not defined', key)
    env[key] = service

class GatewayServer(server.Server):
    def __init__(self, host, protocol, agent):
        self.protocol = protocol
        self.agent = agent
        self.connections = {}
        server.Server.__init__(self, host)

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
        self.put_socket(socket)

    def on_disconnect(self, socket):
        user_reference = socket if isinstance(socket, str) else self.sock2addr(socket)
        packet = {'command': 'disconnect'}
        self.agent.publish(
            obj=packet,
            queue=env['ROUTING_SERVICE'],
            correlation_id=user_reference)
        if user_reference in self.connections:
            del self.connections[user_reference]

    def on_read(self, socket, data, buff):
        disconnect = False
        while data:
            self.protocol.append(buff, data)
            data = self.protocol.load_packet(buff)

            if self.protocol.still_loading(buff):
                return
            addr = socket.getpeername()
            LOGGER.info('Client %s:%s - packet loaded', addr[0], addr[1])

            packet, error = self.protocol.parse(buff)
            if error:
                addr = socket.getpeername()
                LOGGER.info('Client %s:%s - packet error, disconnecting', addr[0], addr[1])
                disconnect = True
                buff = self.protocol.compose(packet)
                break

            packet['command'] = 'process'
            self.agent.publish(
                obj=packet,
                queue=env['ROUTING_SERVICE'],
                correlation_id=self.sock2addr(socket))

        if disconnect:
            self.on_disconnect(socket)
            self.register_write(socket, buff)
        else:
            self.unregister(socket)

    def on_write(self, socket):
        if self.socket_alive(socket):
            buff = self.protocol.new_stream()
            self.register_read(socket, buff)
        else:
            self.unregister(socket)
            self.safe_close(socket)

    def loop(self):
        packet, addr = self.agent.get()
        if not packet:
            return

        command = packet.get('command')
        del packet['command']

        client = self.get_socket(addr)
        if not client:
            self.on_disconnect(addr)
            return

        if command == 'disconnect':
            self.on_disconnect(addr)

        if packet:
            buff = self.protocol.compose(packet)
            self.register_write(client, buff)
        elif command == 'disconnect':
            self.unregister(client)
            self.safe_close(client)
        elif command == 'read':
            buff = self.protocol.new_stream()
            self.register_read(client, buff)
        else:
            self.unregister(client)

    def on_close(self, packet):
        if len(self.connections) > 0:
            for client in self.connections.values():
                self.unregister(client)
            self.connections = {}

    def get_socket(self, addr):
        return self.connections.get(addr, None)

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
