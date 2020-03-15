from oauthlib import oauth2
import json

CLIENT_ID = None
CLIENT_SECRET = None
with open("google_client_secret.json") as json_file:
    data = json.load(json_file)
    CLIENT_ID = data.get("client_id")
    CLIENT_SECRET = data.get("client_secret")
if not CLIENT_ID or not CLIENT_SECRET:
    print("Google Client Secret not found, exiting...")
    sys.exit(1)
GOOGLE_DISCOVERY_URL = (
    "https://accounts.google.com/.well-known/openid-configuration"
)

client = oauth2.WebApplicationClient(CLIENT_ID)

def get_cfg():
    return requests.get(GOOGLE_DISCOVERY_URL).json()
