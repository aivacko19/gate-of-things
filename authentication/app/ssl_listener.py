import json
import os

import flask
import requests

from providers import google as provider
from auth_publisher import publisher

app = flask.Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY") or os.urandom(24)

@app.route("/")
def index():

    # Get Code
    code = flask.request.args.get("code")
    user_reference = flask.request.args.get("state")

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
    uri, headers, body = client.add_token(userinfo_endpoint)
    userinfo = requests.get(uri, headers=headers, data=body).json()

    if not userinfo.get("email_verified"):
        publisher.publish(user_reference)
        return "User email not available or not verified by Google.", 400

    email = userinfo["email"]
    publisher.publish(user_reference, email)
    
    return (
        "<p>You're logged in! Email: {}</p>"
        "<p>Check your MQTT Connection</p>".format(email)
    )