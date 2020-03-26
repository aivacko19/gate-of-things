#!/usr/bin/env python3

import os
import logging

from oauth_uri_service import OAuthUriService
from oauth_userinfo_service import app as userinfo_service

LOGGER = logging.getLogger(__name__)
LOG_FORMAT = ('%(levelname) -10s %(asctime)s %(name)s %(funcName)s %(lineno)d: %(message)s')
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)

env = {
    'OAUTH_URI_SERVICE': None,
    'REDIRECT_URI': None
}

for key in env:
    service = os.environ.get(key)
    if not service:
        raise Exception('Environment variable %s not defined', key)
    env[key] = service

HOST = os.environ.get("HOST", "localhost")

uri_service = OAuthUriService(env['OAUTH_URI_SERVICE'], env['REDIRECT_URI'])
uri_service.start()
userinfo_service.run(host=HOST, ssl_context="adhoc")
uri_server.close()