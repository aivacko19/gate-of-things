import sys
import os
import logging

import pika

class AuthenticationPublisher:

    __instance = None

    @staticmethod
    def initInstance(rabbitmq, recepient):
        AuthenticationPublisher.__instance = AuthenticationPublisher(rabbitmq, recepient)

    @staticmethod
    def getInstance():
        return AuthenticationPublisher.__instance

    def __init__(self, rabbitmq, recepient):
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(
                host=rabbitmq,
                connection_attempts=10,
                retry_delay=5,
                heartbeat=0,))

        self.channel = self.connection.channel()
        self.recepient = recepient
        logging.info(f"Created AuthenticationPublisher publishing on queue {recepient}")

    def publish(self, user_reference, email='none'):
        logging.info(f"Publishing Email {email} for {user_reference}")
        self.channel.basic_publish(
            exchange='',
            routing_key=self.recepient,
            properties=pika.BasicProperties(
                correlation_id=user_reference),
            body=email)