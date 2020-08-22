
import os
import json
import logging

from flask import Flask, render_template, session, redirect, url_for, request
import requests
from flask_bootstrap import Bootstrap
import base64

import psycopg2

import service
from db import Database

import amqp_helper
from providers import google as provider

LOGGER = logging.getLogger(__name__)

PAGE_SIZE = 30

app = Flask(__name__)
Bootstrap(app)
app.secret_key = os.environ.get("SECRET_KEY") or os.urandom(24)

env = {
    'QUEUE': None,
    'OAUTH_URI_SERVICE': None,
    'DEVICE_SERVICE': None,
    'ACCESS_CONTROL_SERVICE': None,
    'SUBSCRIPTION_SERVICE': None,
}

for key in env:
    env_var = os.environ.get(key)
    if not env_var:
        raise Exception('Environment variable %s not defined', key)
    env[key] = env_var

key = 'DSN'
dsn = os.environ.get(key, None)
if not dsn:
    raise Exception('Environment variable %s not defined', key)

db = Database(dsn)

key = 'AUDIT_LOG_DB'
audit_log_db = os.environ.get(key, None)
if not audit_log_db:
    raise Exception('Environment variable %s not defined', key)


my_agent = amqp_helper.AmqpAgent()
my_agent.connect()

@app.route("/")
def index():
    # Attempt to fetch username
    failed_to_login = False
    if 'username' not in session:
        if 'temp_id' not in session:
            # Create temporary id for guest
            temp_id = db.new()
            session['temp_id'] = temp_id
        else:
            # Check if OAuth service verified the user
            result = db.get(session['temp_id'])
            username = result.get('username')
            failed_to_login = result.get('failed')
            if username:
                session['username'] = username
                db.delete(session['temp_id'])
                del session['temp_id']

    if 'username' not in session:
        return render_template('login.html', failed=failed_to_login)

    if 'devices' not in session:
        # Get names of devices owned
        my_request = {
            'command': 'get_owned_resources',
            'user': session['username'],}
        response = rpc(my_request, 'ACCESS_CONTROL_SERVICE')

        # Get device properties 
        my_request = {
            'command': 'get_devices_by_name',
            'names': session['resources']
        }
        response = rpc(my_request, 'DEVICE_SERVICE')
        session['devices'] = response['devices']

    devices = session['devices']
    return render_template('devices.html', devices=devices)

@app.route("/login/")
def login():
    if 'username' in session:
        return redirect(url_for('index'))

    my_request = {
        'command': 'oauth_request',
        'redirect_url': request.url_root,
        'queue': env['QUEUE'],}
    response = my_agent.rpc(
        obj=my_request,
        queue=env['OAUTH_URI_SERVICE'],
        correlation_id=str(session['temp_id']))

    return redirect(response.get('uri'))

@app.route("/logout/")
def logout():
    session.pop('username', None)
    session.pop('devices', None)
    return redirect(url_for('index'))

@app.route("/<device_name>/")
def details(device_name):
    device = get_device(device_name)
    if device is None:
        return redirect(url_for('index'))

    return render_template('details.html', device=device)

@app.route("/<device_name>/delete/")
def delete_device(device_name):
    device = get_device(device_name)
    if device is None:
        return redirect(url_for('index'))


    my_request = {
        'command': 'delete_device',
        'name': device_name
    }
    response = rpc(my_request, 'DEVICE_SERVICE')

    del session['devices'][device_name]

    return render_template('devices.html', devices=session['devices'])

@app.route("/<device_name>/change_key/", methods=['POST', 'GET'])
def change_key(device_name):
    device = get_device(device_name)
    if device is None:
        return redirect(url_for('index'))

    if request.method == 'GET':
        return render_template('change_key.html', device=device)
    
    new_key = request.form['new_key']
    decoded_key = base64.b64decode(new_key)
    if len(decoded_key) != 32:
        return render_template('change_key.html', device=device, error=True)

    my_request = {
        'command': 'change_key',
        'name': device_name,
        'key': new_key
    }
    response = rpc(my_request, 'DEVICE_SERVICE')

    session['devices'][device_name]['key'] = new_key

    return render_template('details.html', device=device)



@app.route("/new/", methods=['POST', 'GET'])
def new_device():
    if 'username' not in session:
        return redirect(url_for('index'))

    if request.method == "GET":
        return render_template('new_device.html')

    name = request.form['name']
    key = request.form['key']

    error_name = False
    error_key = False

    my_request = {
        'command': 'get',
        'name': name
    }
    response = rpc(my_request, 'DEVICE_SERVICE')
    device = response.get('device')

    if device is not None:
        error_name = True

    decoded_key = base64.b64decode(key)
    if len(decoded_key) != 32:
        error_key = True

    if error_name or error_key:
        return render_template('new_device.html', device=device, 
            error_key=error_key, error_name=error_name)

    my_request = {
        'command': 'add',
        'name': name,
        'owner': session['username'],
        'key': key
    }
    response = rpc(my_request, 'DEVICE_SERVICE')

    device = {
        'id': response['id'],
        'name': name,
        'owner': session['username'],
        'key': key
    }

    session['devices'][name] = device

    email = '%s@device' % name
    resource = 'device/%s' % name
    resource_ctrl = '%s/ctrl' % resource

    # Enable device to publish
    response = {
        'user': email,
        'resource': resource,
        'write': True,
    }
    self.create_policy(response, props)

    # Enable device to be controled
    response = {
        'user': email,
        'resource': resource_ctrl,
        'read': True,
    }
    self.create_policy(response, props)

    # Enable owner to control
    response = {
        'user': owner,
        'resource': resource_ctrl,
        'write': True,
        'owner': True
    }
    self.create_policy(response, props)

    LOGGER.info(session['devices'])

    return render_template('details.html', device=device)

@app.route("/<device_name>/policies/")
def policies(device_name):
    device = get_device(device_name)
    if device is None:
        return redirect(url_for('index'))

    resource = 'device/%s' % device_name

    if 'policies' not in device:
        my_request = {
            'command': 'get_resource',
            'resource': resource
        }
        response = rpc(my_request, 'ACCESS_CONTROL_SERVICE')
        policies = response.get('policies')
        for user, policy in policies.items():
            if not policy['read']:
                del policies[user]
            policy['write'] = False
        my_request = {
            'command': 'get_resource',
            'resource': resource + '/ctrl'
        }
        response = rpc(my_request, 'ACCESS_CONTROL_SERVICE')
        write_policies = response.get('policies')
        for user, policy in write_policies.items():
            if not policy['write']:
                continue
            if user in policies:
                policies[user]['write'] = True
            else:
                policy['read'] = False
                policy['access_time'] = 0
                policies[user] = policy

        session['devices'][device_name]['policies'] = policies

    policies = session['devices'][device_name]['policies']

    return render_template('policies.html', device=device, policies=policies)

@app.route("/<device_name>/policies/<user>/delete/")
def delete_policy(device_name, user):
    device = get_device(device_name)
    if device is None:
        return redirect(url_for('index'))

    resource = 'device/%s' % device_name

    if 'policies' not in device:
        my_request = {
            'command': 'get_resource',
            'resource': resource
        }
        response = rpc(my_request, 'ACCESS_CONTROL_SERVICE')
        session['devices'][device_name]['policies'] = response.get('policies')

    policies = session['devices'][device_name]['policies']

    if user in policies:
        policy = session['devices'][device_name]['policies'][user]
        read = policy['read']
        write = policy['write']
        own = policy['own']
        if own:
            return

        del session['devices'][device_name]['policies'][user]

        if read:
            my_request = {
                'command': 'delete_policy',
                'resource': resource,
                'user': user
            }
            publish(my_request, 'ACCESS_CONTROL_SERVICE')
        if write:
            my_request = {
                'command': 'delete_policy',
                'resource': resource + '/ctrl',
                'user': user
            }
            publish(my_request, 'ACCESS_CONTROL_SERVICE')


    return redirect(url_for('policies', device_name=device_name))

@app.route("/<device_name>/policies/new_policy/", methods=["POST", "GET"])
def new_policy(device_name):
    device = get_device(device_name)
    if device is None:
        return redirect(url_for('index'))

    resource = 'device/%s' % device_name

    if 'policies' not in device:
        my_request = {
            'command': 'get_resource',
            'resource': resource
        }
        response = rpc(my_request, 'ACCESS_CONTROL_SERVICE')
        session['devices'][device_name]['policies'] = response.get('policies')

    policies = session['devices'][device_name]['policies']

    if request.method == 'GET':
        return render_template('new_policy.html', device=device)

    user = request.form.get('user')
    read = 'read' in request.form and request.form.get('read') == 'on'
    write = 'write' in request.form and request.form.get('write') == 'on'
    own = 'own' in request.form and request.form.get('own') == 'on'
    access_time = int(request.form.get('access_time'))

    if user in policies:
        return render_template('new_policy.html', device=device, error_name=True)

    if read:
        my_request = {
            'command': 'add_policy',
            'resource': resource,
            'user': user,
            'read': read,
            'write': False,
            'own': own,
            'access_time': access_time
        }
        publish(my_request, "ACCESS_CONTROL_SERVICE")
    if write:
        my_request = {
            'command': 'add_policy',
            'resource': resource + '/ctrl',
            'user': user,
            'read': False,
            'write': write,
            'access_time': 0
        }
        publish(my_request, "ACCESS_CONTROL_SERVICE")


    session['devices'][device_name]['policies'][user] = {
        'user': user,
        'read': read,
        'write': write,
        'own': own,
        'access_time': access_time,
    }

    return redirect(url_for('policies', device_name=device_name))

@app.route('/<device_name>/subscriptions/')
def subscriptions(device_name):
    device = get_device(device_name)
    if device is None:
        return redirect(url_for('index'))

    topic = 'device/%s' % device_name

    my_request = {
        'command': 'get_subscriptions',
        'topic': topic,
    }
    response = rpc(my_request, "SUBSCRIPTION_SERVICE")
    subscriptions = response['subscriptions']

    return render_template('subscriptions.html', device=device, subscriptions=subscriptions)

@app.route('/<device_name>/subscriptions/<user>/delete_subscription/')
def delete_subscription(device_name, user):
    device = get_device(device_name)
    if device is None:
        return redirect(url_for('index'))

    topic = 'device/%s' % device_name

    my_request = {
        'command': 'delete_subscription',
        'topic': topic,
        'email': user
    }
    response = rpc(my_request, "SUBSCRIPTION_SERVICE")

    return redirect(url_for('subscriptions', device_name=device_name))

@app.route('/<device_name>/log')
def log(device_name):
    device = get_device(device_name)
    if device is None:
        return redirect(url_for('index'))

    page = int(request.args.get('page', '1'))

    limit = PAGE_SIZE
    offset = (page - 1) * PAGE_SIZE

    resource = 'device/%s' % device_name
    user = session['username'].replace('@', '_at_').replace('.', '_dot_')
    dsn = 'postgres://%s:123@%s' % (user, audit_log_db)
    query = 'SELECT * FROM get_audit_logs(%s, %s, %s)'

    logs = list()
    with psycopg2.connect(dsn) as conn:
        conn.autocommit = True
        cursor = conn.cursor()
        cursor.execute(query, (resource, limit, offset))
        result = cursor.fetchall()
        LOGGER.info(result)
        for row in result:
            log = {
                'user': row[0],
                'action': row[1],
                'access_time': row[2]
            }
            logs.append(log)

    return render_template('log.html', device=device, logs=logs)

def get_device(device_name):
    if 'username' not in session:
        return None

    username = session['username']

    # Check own rights
    my_request = {
        'command': 'get_own_access',
        'user': username,
        'resource': device_name}
    response = rpc(my_request, 'ACCESS_CONTROL_SERVICE')
    if not response['own_access']:
        return None

    devices = session.get('devices')
    if device_name not in devices:
        return None

    return devices[device_name]

def rpc(request, service):
    return my_agent.rpc(
        obj=request,
        queue=env[service])

def publish(request, service):
    return my_agent.publish(
        obj=request,
        queue=env[service])

def create_policy(self, request, props):
    request['command'] = 'add_policy'
    self.publish(
        obj=request,
        queue=env['ACCESS_CONTROL_SERVICE'],
        correlation_id=props.correlation_id,)



