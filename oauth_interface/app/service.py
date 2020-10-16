
from providers import google as provider 
import abstract_service
import json

CONTINUE_AUTHENTICATION = 0x18

class Service(abstract_service.AbstractService):

    def __init__(self, queue, redirect_uri, dummy_messenger=None):
        self.redirect_uri = redirect_uri
        self.dummy_messenger = dummy_messenger
        abstract_service.AbstractService.__init__(self, queue)
        self.actions = {'get_uri': self.get_uri,}

    # Retrieve URL for the Indentity provider based on the client id
    def get_uri(self, request, props):

        provider_cfg = provider.get_cfg()
        authorization_endpoint = provider_cfg["authorization_endpoint"]

        # Saving the callback queue, redirect url if redirected, and client id
        state = {'queue': request.get('queue'),
                 'redirect_url': request.get('redirect_url'),
                 'user_reference': props.correlation_id,}
        state_str = json.dumps(state)

        request_uri = provider.client.prepare_request_uri(
            authorization_endpoint,
            redirect_uri=self.redirect_uri,
            scope=["openid", "email", "profile"],
            state=state_str,)

        return {'command': 'oauth_uri', 'uri': request_uri}

