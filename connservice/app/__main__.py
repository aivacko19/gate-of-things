#!/usr/bin/env python3

import sys
import os
import pika
import time
import logging
import threading

import gateway_listener
import oauth_response
import connection_db
import request_uri_client


logging.basicConfig(level=logging.INFO)


DB_NAME = os.environ.get('DB_NAME')
DB_USER = os.environ.get('DB_USER')
DB_PASS = os.environ.get('DB_PASS')
DB_HOST = os.environ.get('DB_HOST')

db = connection_db.ConnectionDB(DB_NAME, DB_USER, DB_PASS, DB_HOST)

RABBITMQ = os.environ.get('RABBITMQ', 'localhost')
GATEWAY_QUEUE = os.environ.get('GATEWAY_QUEUE', 'conn-service-auth')
AUTH_QUEUE = os.environ.get('AUTH_QUEUE', 'conn-service-gate')
AUTH_REMOTE = os.environ.get('AUTH_REMOTE', 'auth-service')

request_uri_client.RequestUriRpcClient.initInstance(RABBITMQ, AUTH_REMOTE)
oauth_service = oauth_response.OAuthListener(RABBITMQ, AUTH_QUEUE, db)
gateway_service = gateway_listener.GatewayListener(RABBITMQ, GATEWAY_QUEUE, db)

oauth_service.run()
logging.info(f"Running oauth verification service {oauth_service.is_alive()}")
gateway_service.run()