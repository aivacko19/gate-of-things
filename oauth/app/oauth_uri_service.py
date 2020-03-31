
from providers import google as provider 
import amqp_helper

CONTINUE_AUTHENTICATION = 0x18

class OAuthUriService(amqp_helper.AmqpAgent):

    def __init__(self, queue, redirect_uri):
        self.redirect_uri = redirect_uri
        amqp_helper.AmqpAgent.__init__(self, queue)

    def main(self, request, props):
        if not request.get('oauth_request'): return

        provider_cfg = provider.get_cfg()
        authorization_endpoint = provider_cfg["authorization_endpoint"]

        request_uri = provider.client.prepare_request_uri(
            authorization_endpoint,
            redirect_uri=self.redirect_uri,
            scope=["openid", "email", "profile"],
            state=props.correlation_id,)

        response = {
            'command': 'oauth_uri',
            'uri': request_uri}
        self.publish(
            obj=response, 
            queue=props.reply_to, 
            correlation_id=props.correlation_id,)