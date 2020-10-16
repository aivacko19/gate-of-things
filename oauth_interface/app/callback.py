
import os
import json

import flask
import requests

import abstract_service
from providers import google as provider

app = flask.Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY") or os.urandom(24)

my_agent = abstract_service.AbstractService()
my_agent.connect()

@app.route("/")#                                                   // callback.py
def index():
    # Get code and state from OAuth server notification
    code = flask.request.args.get("code")
    state_str = flask.request.args.get("state")
    state = json.loads(state_str)

    # Request token from OAuth server using code
    provider_cfg = provider.get_cfg()
    token_endpoint = provider_cfg["token_endpoint"]
    token_url, headers, body = provider.client.prepare_token_request(
        token_endpoint,
        authorization_response=flask.request.url,
        redirect_url=flask.request.base_url,
        code=code,)
    token_response = requests.post(token_url, headers=headers, data=body,
        auth=(provider.CLIENT_ID, provider.CLIENT_SECRET),)
    provider.client.parse_request_body_response(json.dumps(token_response.json()))

    # Request user info from OAuth server using token and extract email
    userinfo_endpoint = provider_cfg["userinfo_endpoint"]
    uri, headers, body = provider.client.add_token(userinfo_endpoint)
    userinfo = requests.get(uri, headers=headers, data=body).json()

    email_verified = userinfo.get('email_verified')
    email = userinfo.get('email')

    # Prepare response
    if not email_verified:
        response = ("User email not available or not verified by Google.", 400)
    else:
        if state.get('redirect_url'):
            response = flask.redirect(state.get('redirect_url'))
        else:
            response = ("<p>You logged in! You're a Legend! Email: {}</p>"
                        "<p>Check your MQTT Connection</p>".format(email))

    request = {'command': 'verify',
               'email': email if email_verified else None}
    my_agent.publish(request=request, 
                     queue=state.get('queue'), 
                     correlation_id=state.get('user_reference'),)

    return response