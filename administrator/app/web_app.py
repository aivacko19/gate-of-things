
import os
import json
import logging
import base64

from flask import Flask, render_template, session, redirect, url_for, request
from flask_bootstrap import Bootstrap
import psycopg2

import abstract_service
import service
from db import Database


LOGGER = logging.getLogger(__name__)

PAGE_SIZE = 30

app = Flask(__name__)
Bootstrap(app)
app.secret_key = os.environ.get("SECRET_KEY") or os.urandom(24)

env = {
    'QUEUE': None,
    'OAUTH_SERVICE': None,
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
my_agent = abstract_service.AbstractService()
my_agent.connect()


key = 'AUDIT_LOG_DB'
AUDIT_LOG_DB = os.environ.get(key, None)
if not AUDIT_LOG_DB:
    raise Exception('Environment variable %s not defined', key)


class Util:
    def __init__(self, db, service):
        self.db = db
        self.service = service

util = Util(db, my_agent)

def init(db, service):
    util = Util(db, service)

@app.route("/")
def index():
    # Attempt to fetch username
    failed_to_login = False
    if 'username' not in session:
        if 'temp_id' not in session:
            # Create temporary id for guest
            temp_id = util.db.new()
            session['temp_id'] = temp_id
        else:
            # Check if OAuth service verified the user
            result = util.db.get(session['temp_id'])
            username = result.get('username')
            failed_to_login = result.get('failed')
            if username:
                session['username'] = username
                util.db.delete(session['temp_id'])
                del session['temp_id']

    if 'username' not in session:
        return render_template('login.html', failed=failed_to_login)

    if 'devices' not in session:
        # Get names of devices owned
        response = rpc('ACCESS_CONTROL_SERVICE', {'command': 'get_owned_resources',
                                                  'user': session['username'],})
        # Get device properties 
        response = rpc('DEVICE_SERVICE', {'command': 'get_devices_by_name',
                                          'names': response['resources'],})
        session['devices'] = response['devices']

    devices = session['devices']
    return render_template('devices.html', devices=devices)

@app.route("/login/")
def login():
    if 'username' in session: return redirect(url_for('index'))

    my_request = {'command': 'get_uri',
                  'redirect_url': request.url_root,
                  'queue': env['QUEUE'],}
    response = util.service.rpc(request=my_request,
                            queue=env['OAUTH_SERVICE'],
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
    response = rpc('DEVICE_SERVICE', my_request)

    my_request = {
        'command': 'delete_resource',
        'resource': device_name
    }
    response = publish('ACCESS_CONTROL_SERVICE', my_request)

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
    response = rpc('DEVICE_SERVICE', my_request)

    session['devices'][device_name]['key'] = new_key

    return render_template('details.html', device=device)


# Registering new device                                                        // web_app.py
@app.route("/new/", methods=['POST', 'GET'])
def new_device():
    if 'username' not in session: return redirect(url_for('index'))
    if request.method == "GET": return render_template('new_device.html')

    name, key, error_name, error_key = request.form['name'], request.form['key'], False, False
    device = {'name': name, 'key': key,}

    response = rpc('DEVICE_SERVICE', {'command': 'get', 'name': name})
    if response.get('device'): error_name = True
    if len(base64.b64decode(key)) != 32: error_key = True
    if error_name or error_key: 
        return render_template('new_device.html', error_key=error_key, error_name=error_name)

    response = rpc('DEVICE_SERVICE', {'command': 'add', 'device': device})
    device['id'] = response['id']
    session['devices'][name] = device
    publish('ACCESS_CONTROL_SERVICE', {'command': 'add_policy',
                                       'user': session['username'],
                                       'resource': name,
                                       'read': True, 'write': True, 'own': True,})
    return render_template('details.html', device=device)

@app.route("/<device_name>/policies/")
def policies(device_name):
    device = get_device(device_name)
    if device is None:
        return redirect(url_for('index'))

    if 'policies' not in device:
        my_request = {
            'command': 'get_resource',
            'resource': device_name
        }
        response = rpc('ACCESS_CONTROL_SERVICE', my_request)
        policies = response.get('policies')

        session['devices'][device_name]['policies'] = policies

    policies = session['devices'][device_name]['policies']

    return render_template('policies.html', device=device, policies=policies)

@app.route("/<device_name>/policies/<user>/delete/")
def delete_policy(device_name, user):
    device = get_device(device_name)
    if device is None:
        return redirect(url_for('index'))

    if 'policies' not in device:
        my_request = {
            'command': 'get_resource',
            'resource': device_name
        }
        response = rpc('ACCESS_CONTROL_SERVICE', my_request)
        session['devices'][device_name]['policies'] = response.get('policies')

    policies = session['devices'][device_name]['policies']

    if user in policies:
        policy = session['devices'][device_name]['policies'][user]
        if not policy['own']:
            del session['devices'][device_name]['policies'][user]

            my_request = {
                'command': 'delete_policy',
                'resource': device_name,
                'user': user
            }
            publish('ACCESS_CONTROL_SERVICE', my_request)

    return redirect(url_for('policies', device_name=device_name))

@app.route("/<device_name>/policies/new_policy/", methods=["POST", "GET"])
def new_policy(device_name):
    device = get_device(device_name)
    if device is None:
        return redirect(url_for('index'))

    if 'policies' not in device:
        my_request = {
            'command': 'get_resource',
            'resource': device_name
        }
        response = rpc('ACCESS_CONTROL_SERVICE', my_request)
        session['devices'][device_name]['policies'] = response.get('policies')

    policies = session['devices'][device_name]['policies']

    if request.method == 'GET':
        return render_template('new_policy.html', device=device)

    user = request.form.get('user')
    read = 'read' in request.form and request.form.get('read') == 'on'
    write = 'write' in request.form and request.form.get('write') == 'on'
    own = 'own' in request.form and request.form.get('own') == 'on'
    access_time_string = request.form.get('access_time', '')
    access_time = int(access_time_string) if access_time_string.isnumeric() else 0

    if user in policies:
        return render_template('new_policy.html', device=device, error_name=True)

    my_request = {
        'command': 'add_policy',
        'resource': device_name,
        'user': user,
        'read': read,
        'write': write,
        'own': own,
        'access_time': access_time,
    }
    publish("ACCESS_CONTROL_SERVICE", my_request)

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
    response = rpc("SUBSCRIPTION_SERVICE", my_request)
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
    response = rpc("SUBSCRIPTION_SERVICE", my_request)

    return redirect(url_for('subscriptions', device_name=device_name))

# Retrieving logs from Audit Log DB                        // web_app.py
@app.route('/<device_name>/log')
def log(device_name):
    device = get_device(device_name)
    if device is None: return redirect(url_for('index'))

    page = int(request.args.get('page', '1'))
    offset = (page - 1) * PAGE_SIZE
    user = session['username'].replace('@', '_at_').replace('.', '_dot_')
    dsn = 'postgres://%s:123@%s' % (user, AUDIT_LOG_DB)
    query = 'SELECT * FROM get_audit_logs(%s, %s, %s)'

    logs = list()
    with psycopg2.connect(dsn) as connection:
        connection.autocommit = True
        cursor = connection.cursor()
        cursor.execute(query, (device_name, PAGE_SIZE, offset))
        for row in cursor.fetchall():
            logs.append({'user': row[0],
                         'action': row[1],
                         'success': row[2],
                         'access_time': row[3]})

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
    response = rpc('ACCESS_CONTROL_SERVICE', my_request)
    if not response['own_access']:
        return None

    devices = session.get('devices')
    if device_name not in devices:
        return None

    return devices[device_name]

def rpc(service, request):
    return util.service.rpc(
        request=request,
        queue=env[service])

def publish(service, request):
    return util.service.publish(
        request=request,
        queue=env[service])