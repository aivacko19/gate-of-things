
from providers import google as provider 
import amqp_helper
import json

CONTINUE_AUTHENTICATION = 0x18

class Service(amqp_helper.AmqpAgent):

    def __init__(self, queue, redirect_uri):
        self.redirect_uri = redirect_uri
        amqp_helper.AmqpAgent.__init__(self, queue)

    def main(self, request, props):
        if not request.get('oauth_request'): return

        provider_cfg = provider.get_cfg()
        authorization_endpoint = provider_cfg["authorization_endpoint"]

        state = {
            'queue': request.get('queue'),
            'redirect_url': request.get('redirect_url'),
            'user_reference': props.correlation_id,}
        state_str = json.dumps(state)

        request_uri = provider.client.prepare_request_uri(
            authorization_endpoint,
            redirect_uri=self.redirect_uri,
            scope=["openid", "email", "profile"],
            state=state_str,)

        response = {
            'command': 'oauth_uri',
            'uri': request_uri}
        self.publish(
            obj=response, 
            queue=props.reply_to, 
            correlation_id=props.correlation_id,)