import json
import os
import sys
import time
import threading

# Third-party libraries
from flask import Flask, redirect, request, url_for
from oauthlib import oauth2
import requests
import pika

NUM_OF_ATTEMPTS = 10
WAIT_TIME = 5
RABBITMQ_HOSTNAME = os.environ.get('RABBITMQ_HOSTNAME', "localhost")
if not RABBITMQ_HOSTNAME:
    sys.exit(1)
QUEUE_NAME = os.environ.get('QUEUE_NAME', "auth-service")
if not QUEUE_NAME:
    sys.exit(1)

URI_CALLBACK = os.environ.get("URI_CALLBACK", "https://127.0.0.1:5000/")
HOST = os.environ.get("HOST", "localhost")

GOOGLE_CLIENT_ID = None
GOOGLE_CLIENT_SECRET = None
with open("client_secret.json") as json_file:
    data = json.load(json_file)
    GOOGLE_CLIENT_ID = data.get("client_id")
    GOOGLE_CLIENT_SECRET = data.get("client_secret")
if not GOOGLE_CLIENT_ID or not GOOGLE_CLIENT_SECRET:
    print("Google Client Secret not found, exiting...")
    sys.exit(1)
GOOGLE_DISCOVERY_URL = (
    "https://accounts.google.com/.well-known/openid-configuration"
)

connection = None
for i in range(NUM_OF_ATTEMPTS):
    print("Trying to connect to RabbitMQ")
    try:
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOSTNAME))
    except pika.exceptions.AMQPConnectionError:
        print(f"Connection attempt number {i} failed")
        time.sleep(WAIT_TIME)
    if connection:
        break
if not connection:
    print("Exceeded number of attempts, exiting...")
    sys.exit(1)
channel = connection.channel()

def send_auth_request(ch, method, properties, body):
    body_string = body.decode('utf-8')
    packet = json.loads(body)
    client_id = packet['app_user_id']

    # Find out what URL to hit for Google login
    google_provider_cfg = get_google_provider_cfg()
    authorization_endpoint = google_provider_cfg["authorization_endpoint"]

    # Use library to construct the request for Google login and provide
    # scopes that let you retrieve user's profile from Google
    request_uri = client.prepare_request_uri(
        authorization_endpoint,
        redirect_uri=URI_CALLBACK,
        scope=["openid", "email", "profile"],
        state=body_string
    )

    packet['request_uri'] = request_uri
    response_queue_name = packet['response_queue_name']
    del packet['response_queue_name']
    body = json.dumps(packet).encode('utf-8')
    ch.basic_publish(exchange='', routing_key=response_queue_name, body=body)

# Flask app setup
app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY") or os.urandom(24)

# OAuth 2 client setup
client = oauth2.WebApplicationClient(GOOGLE_CLIENT_ID)

def get_google_provider_cfg():
    return requests.get(GOOGLE_DISCOVERY_URL).json()

@app.route("/")
def index():
    # Get authorization code Google sent back to you
    code = request.args.get("code")
    body_string = request.args.get("state")
    packet = json.loads(body_string)

    # Find out what URL to hit to get tokens that allow you to ask for
    # things on behalf of a user
    google_provider_cfg = get_google_provider_cfg()
    token_endpoint = google_provider_cfg["token_endpoint"]

    # Prepare and send a request to get tokens! Yay tokens!
    token_url, headers, body = client.prepare_token_request(
        token_endpoint,
        authorization_response=request.url,
        redirect_url=request.base_url,
        code=code
    )
    token_response = requests.post(
        token_url,
        headers=headers,
        data=body,
        auth=(GOOGLE_CLIENT_ID, GOOGLE_CLIENT_SECRET),
    )

    # Parse the tokens!
    client.parse_request_body_response(json.dumps(token_response.json()))

    # Now that you have tokens (yay) let's find and hit the URL
    # from Google that gives you the user's profile information,
    # including their Google profile image and email
    userinfo_endpoint = google_provider_cfg["userinfo_endpoint"]
    uri, headers, body = client.add_token(userinfo_endpoint)
    userinfo_response = requests.get(uri, headers=headers, data=body)

    # You want to make sure their email is verified.
    # The user authenticated with Google, authorized your
    # app, and now you've verified their email through Google!
    if userinfo_response.json().get("email_verified"):
        unique_id = userinfo_response.json()["sub"]
        users_email = userinfo_response.json()["email"]
        picture = userinfo_response.json()["picture"]
        users_name = userinfo_response.json()["given_name"]
    else:
        packet['disconnect'] = True
        response_queue_name = packet['response_queue_name']
        del packet['response_queue_name']
        body = json.dumps(packet).encode('utf-8')
        channel.basic_publish(exchange='', routing_key=response_queue_name, body=body)
        return "User email not available or not verified by Google.", 400

    packet['email'] = users_email
    response_queue_name = packet['response_queue_name']
    del packet['response_queue_name']
    body = json.dumps(packet).encode('utf-8')
    channel.basic_publish(exchange='', routing_key=response_queue_name, body=body)

    # Send user back to homepage
    return (
        "<p>Hello, {}! You're logged in! Email: {} You're client ID: {}</p>"
        "<div><p>Google Profile Picture:</p>"
        '<img src="{}" alt="Google profile pic"></img></div>'.format(
            users_name, users_email, packet['app_user_id'], picture
        )
    )

if __name__ == "__main__":
    channel.queue_declare(queue=QUEUE_NAME)
    channel.basic_consume(queue=QUEUE_NAME, on_message_callback=send_auth_request, auto_ack=True)
    thread = threading.Thread(
        target=channel.start_consuming,
        daemon=True)
    thread.start()
    app.run(host=HOST, ssl_context="adhoc")