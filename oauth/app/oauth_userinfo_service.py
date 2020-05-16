
import os
import json

import flask
import requests

import amqp_helper
from providers import google as provider

app = flask.Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY") or os.urandom(24)

my_agent = amqp_helper.AmqpAgent()
my_agent.connect()

env = {
    'ROUTING_SERVICE': None
}

for key in env:
    service = os.environ.get(key)
    if not service:
        raise Exception('Environment variable %s not defined', key)
    env[key] = service

@app.route("/")
def index():

    # Get Code
    code = flask.request.args.get("code")
    state_str = flask.request.args.get("state")
    state = json.loads(state_str)
    user_reference = state.get('user_reference')

    # Get Token with Code
    provider_cfg = provider.get_cfg()
    token_endpoint = provider_cfg["token_endpoint"]
    token_url, headers, body = provider.client.prepare_token_request(
        token_endpoint,
        authorization_response=flask.request.url,
        redirect_url=flask.request.base_url,
        code=code
    )
    token_response = requests.post(
        token_url,
        headers=headers,
        data=body,
        auth=(provider.CLIENT_ID, provider.CLIENT_SECRET),
    )
    provider.client.parse_request_body_response(json.dumps(token_response.json()))

    # Get User Info with Token
    userinfo_endpoint = provider_cfg["userinfo_endpoint"]
    uri, headers, body = provider.client.add_token(userinfo_endpoint)
    userinfo = requests.get(uri, headers=headers, data=body).json()

    # Prepare email and response
    if not userinfo.get("email_verified"):
        email = None
        response = ("User email not available or not verified by Google.", 400)
    else:
        email = userinfo["email"]
        if state.get('redirect_url'):
            response = flask.redirect(state.get('redirect_url'))
        else:
            response = (
                "<p>You logged in! You're a Legend! Email: {}</p>"
                "<p>Check your MQTT Connection</p>".format(email))

    request = {
        'command': 'verify',
        'email': email}
    my_agent.publish(
        obj=request, 
        queue=state.get('queue'), 
        correlation_id=user_reference,)

    return response