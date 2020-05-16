
import os
import json
import logging

from flask import Flask, render_template, session, redirect, url_for, request
import requests
from flask_bootstrap import Bootstrap

import service
import db


import amqp_helper
from providers import google as provider

LOGGER = logging.getLogger(__name__)

app = Flask(__name__)
Bootstrap(app)
app.secret_key = os.environ.get("SECRET_KEY") or os.urandom(24)

env = {
    'MANAGER_SERVICE': None,
    'OAUTH_URI_SERVICE': None,
}

for key in env:
    env_var = os.environ.get(key)
    if not env_var:
        raise Exception('Environment variable %s not defined', key)
    env[key] = env_var

DB_NAME = os.environ.get('DB_NAME', 'mydb')
DB_USER = os.environ.get('DB_USER', 'root')
DB_PASS = os.environ.get('DB_PASS', 'root')
DB_HOST = os.environ.get('DB_HOST', '192.168.99.100')
mydb = db.DB(DB_NAME, DB_USER, DB_PASS, DB_HOST)

my_service = service.Service(env['MANAGER_SERVICE'], mydb)
my_service.connect()
my_service.start()

@app.route("/")
def index():
    failed_to_login = False
    if 'username' not in session:
        if 'temp_id' not in session:
            temp_id = mydb.new()
            session['temp_id'] = temp_id
        else:
            result = mydb.get(session['temp_id'])
            username = result.get('username')
            failed_to_login = result.get('failed')
            if username:
                session['username'] = username
                mydb.delete(session['temp_id'])
                del session['temp_id']

    if 'username' not in session:
        return render_template('login.html', failed=failed_to_login)



    return render_template('hello.html')

@app.route("/login/")
def login():
    my_request = {
        'oauth_request': True,
        'redirect_url': request.url_root,
        'queue': env['MANAGER_SERVICE'],}
    response = my_service.rpc(
        obj=my_request,
        queue=env['OAUTH_URI_SERVICE'],
        correlation_id=str(session['temp_id']))

    return redirect(response.get('uri'))




