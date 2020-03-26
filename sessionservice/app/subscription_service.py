
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
        response = {}

        if command == 'subscribe':
            LOGGER.info('Executing command subscribe')
            email = session.get_email()
            sub_id = request.get('properties').get('subscription_identifier') or 0

            topics = list()
            for topic in request.get('topics'):
                topic['sub_id'] = sub_id
                topic_filter = topic.get('filter')

                authorized = True
                # Authorize (email, topic_filter)

                if not authorized:
                    topics.append({'code': NOT_AUTHORIZED})
                    continue

                self.db.add_sub(session, topic)
                topics.append({'code': topic.get('max_qos')})

            response['type'] = 'suback'
            response['id'] = request.get('id')
            response['topics'] = topics
            response['commands'] = {'write': True, 'read': True}

        elif command == 'unsubscribe':
            topics = list()
            for topic in request.get('topics'):
                sub = self.db.get_sub(session, topic.get('filter'))
                if not sub:
                    topics.append({'code': NO_SUBSCRIPTION_EXISTED})
                    continue

                self.db.delete_sub(session, sub)
                topics.append({'code': SUCCESS})

            response['type'] = 'unsuback'
            response['id'] = request.get('id')
            response['topics'] = topics
            response['commands'] = {'write': True, 'read': True}

        if response:
            response['service'] = True
            self.publish(
                obj=response,
                queue=props.reply_to,
                correlation_id=props.correlation_id)