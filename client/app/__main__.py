#!/usr/bin/env python3

import time
import socket
import json
import sys
import threading

from protocols import mqtt as protocol


def reading(server_socket):

    while True:
        stream = protocol.new_stream()
        while stream.empty() or stream.still_loading():
            data = server_socket.recv(4096)
            if not data: raise RuntimeError("Server closed.")
            print(data)
            stream.append(data)
            stream.load()

        packet = protocol.parser.read(stream)
        print(packet)


LIST_OF_COMMANDS = ['connect', 'ping', 'subscribe', 'unsubscribe', 'publish']

if len(sys.argv) != 3:
    print("usage:", sys.argv[0], "<host_addres>", "<host_port>")
    sys.exit(1)

with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
    sock.connect_ex((sys.argv[1], int(sys.argv[2])))

    thread = threading.Thread(target=reading, args=(sock,), daemon=True)
    thread.start()

    while True:
        command = input(">>>> ")

        if command == 'exit':
            break

        if command not in LIST_OF_COMMANDS:
            print("\n")
            print("command not recognized")
            print("list of commands:")
            for command in LIST_OF_COMMANDS:
                print("\t", command)
            continue

        packet = {}
        ready = False

        if command == 'connect':
            packet['type'] = 'connect'
            packet['clean_start'] = False
            packet['keep_alive'] = 10
            client_id = input("\tclient id: ")
            packet['client_id'] = client_id
            packet['properties'] = {
                'authentication_method': 'OAuth2.0'
            }
            ready = True

        elif command == 'ping':
            packet['type'] = 'pingreq'
            ready = True

        elif command == 'subscribe':
            packet['type'] = 'subscribe'
            packet['id'] = 1234
            subscription_id = input("\tsubscription id: ")
            subscription_id = int(subscription_id)
            if subscription_id > 0:
                packet['subscription_id'] = subscription_id
            packet['topics'] = list()
            while True:
                topic = {}
                topic_filter = input("\ttopic (enter if none): ")
                if topic_filter == '':
                    break
                topic['filter'] = topic_filter
                ready = True
                max_qos = input("\tmax qos: ")
                max_qos = int(max_qos)
                if max_qos not in range(3):
                    max_qos = 0
                topic['max_qos'] = max_qos
                packet['topics'].append(topic)

        elif command == 'unsubscribe':
            packet['type'] = 'unsubscribe'
            packet['id'] = 1234
            packet['topics'] = list()
            while True:
                topic = {}
                topic_filter = input("\ttopic (enter if none): ")
                if topic_filter == '':
                    break
                topic['filter'] = topic_filter
                ready = True
                packet['topics'].append(topic)
            
        elif command == 'publish':
            packet['type'] = 'publish'
            packet['qos'] = 0
            packet['dup'] = False
            packet['retain'] = False
            topic = input('\ttopic: ')
            packet['topic'] = topic
            payload = input('\tmessage: ')
            packet['payload'] = payload.encode('utf-8')
            ready = True


        if not ready:
            print("package not sent")
            continue

        stream = protocol.compose(packet)
        while not stream.empty():
            data = stream.output(4096)
            print(data)
            sent = sock.send(data)
            stream.update(sent)

sys.exit(0)


with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
    sock.connect_ex(('192.168.99.100', 1887))
    sock.settimeout(15)

    while True:
        body = input("Insert Packet: ")
        if body == 'exit': break
        if body != 'pass':
            packet = json.loads(body) #, object_hook=bytescoder.as_bytes)
            stream = protocol.compose(packet)

            while not stream.empty():
                data = stream.output(4096)
                print(data)
                sent = sock.send(data)
                stream.update(sent)

        stream = protocol.new_stream()
        while stream.empty() or stream.still_loading():
            data = sock.recv(4096)
            if not data: raise RuntimeError("Server closed.")
            print(data)
            stream.append(data)
            stream.load()

        packet = protocol.parser.read(stream)
        print(packet)

            


