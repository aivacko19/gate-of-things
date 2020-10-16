import os

import pytest

from app import web_app
from app import service
from . import dummy_db
from . import dummy_messenger

ADMIN = 'ADMIN'
OAUTH = 'OAUTH'
DEVICE = 'DEVICE'
ACCESS_CONTROL = 'ACCESS_CONTROL'
SUBSCRIPTION = 'SUBSCRIPTION'
AMQP_URL = 'AMQP_URL'

os.environ['QUEUE'] = ADMIN
os.environ['OAUTH_SERVICE'] = OAUTH
os.environ['DEVICE_SERVICE'] = DEVICE
os.environ['ACCESS_CONTROL_SERVICE'] = ACCESS_CONTROL
os.environ['SUBSCRIPTION_SERVICE'] = SUBSCRIPTION
os.environ['AMQP_URL'] = AMQP_URL

@pytest.fixture
def client():
    db = dummy_db.Database()
    messenger = dummy_messenger.Messenger()
    my_service = service.Service('administrator', db, messenger)
    web_app.init(db, my_service)

def test_login(client):

    rv = client.get('/login')
    assert b'www.google.com' in rv.data
