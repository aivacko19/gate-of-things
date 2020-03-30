
import logging

import amqp_helper

LOGGER = logging.getLogger(__name__)

SUCCESS = 0x00
NO_SUBSCRIPTION_EXISTED = 0x11
UNSPECIFIED_ERROR = 0x80
IMPLEMENTATION_SPECIFIC_ERROR = 0x83
NOT_AUTHORIZED = 0x87
TOPIC_FILTER_INVALID = 0x8F
PACKET_IDENTIFIER_IN_USE = 0x91
QUOTA_EXCEEDED = 0x97
SHARED_SUBSCRIPTION_NOT_AVAILABLE = 0x9E
SUBSCRIPTION_IDENTIFIERS_NOT_AVAILABLE = 0xA1
WILDCARD_SUBSCRIPTIONS_NOT_AVAILABLE = 0xA2

class SubscriptionService(amqp_helper.AmqpAgent):

    def __init__(self, queue, db):
        self.db = db
        amqp_helper.AmqpAgent.__init__(self, queue)

    def main(self, request, props):
        session = self.db.get(props.correlation_id)
        command = request.get('type')
        codes = list()

        ids = self.db.get_packet_ids(session)
        if request.get('id') in ids:
            LOGGER.info('ERROR: %s: Packet identifier in use', props.correlation_id)
            for topic in request.get('topics'):
                codes.append({'code': PACKET_IDENTIFIER_IN_USE})

        elif command == 'subscribe':
            for topic in request.get('topics'):

                topic_filter = topic.get('filter')
                LOGGER.info('%s: Subscribing on topic filter %s',
                            props.correlation_id, topic.get('filter'))

                email = session.get_email()
                authorized = True
                # Authorize (email, topic_filter)

                if not authorized:
                    codes.append({'code': NOT_AUTHORIZED})
                    continue

                topic['sub_id'] = request.get('properties').get('subscription_identifier') or 0
                topic['session_id'] = session.get_id()
                self.db.add_sub(session, topic)
                codes.append({'code': topic.get('max_qos')})

        elif command == 'unsubscribe':
            for topic in request.get('topics'):

                LOGGER.info('%s: Unsubscribing from topic filter %s',
                            props.correlation_id, topic.get('filter'))
                sub = self.db.get_sub(session, topic.get('filter'))

                if not sub:
                    codse.append({'code': NO_SUBSCRIPTION_EXISTED})
                    continue

                self.db.delete_sub(sub)
                codes.append({'code': SUCCESS})

        if codes:
            response = {
                'type': 'suback' if request.get('type') =='subscribe' else 'unsuback',
                'id': request.get('id'),
                'topics': codes,
                'service': True,
                'commands': {'write': True, 'read': True}
            }
            self.publish(
                obj=response,
                queue=props.reply_to,
                correlation_id=props.correlation_id)

        