import sys
import os
import logging
import json

import pika

class SessionPublisher:

    __instance = None

    @staticmethod
    def initInstance(rabbitmq, recepient):
        SessionPublisher.__instance = SessionPublisher(rabbitmq, recepient)

    @staticmethod
    def getInstance():
        return SessionPublisher.__instance

    def __init__(self, rabbitmq, recepient):
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(
                host=rabbitmq,
                connection_attempts=10,
                retry_delay=5,
                heartbeat=0,))

        self.channel = self.connection.channel()
        self.recepient = recepient
        logging.info(f"Created SessionPublisher publishing on queue {recepient}")

    def publish(self, cid, packet):
        logging.info(f"Publishing Packet to Session for {cid}")
        body = json.dumps(packet)
        self.channel.basic_publish(
            exchange='',
            routing_key=self.recepient,
            properties=pika.BasicProperties(
                correlation_id=cid),
            body=body)