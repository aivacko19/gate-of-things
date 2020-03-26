
import logging

import amqp_helper

LOGGER = logging.getLogger(__name__)

class SessionService(amqp_helper.AmqpAgent):

    def __init__(self, queue, db):
        self.db = db
        amqp_helper.AmqpAgent.__init__(self, queue)

    def main(self, request, props):
        LOGGER.info('Fetching session with id %s', props.correlation_id)
        session = self.db.get(props.correlation_id)
        command = request.get('command')
        response = {}

        if command == 'get':
            LOGGER.info('Received command get')
            response['session'] = True
            if session:
                response['email'] = session.get_email()

        elif command == 'add':
            if session:
                self.db.delete(session)
            self.db.add(props.correlation_id, request.get('email'))

        elif command == 'reauth':
            email = request.get('email')
            for sub in session.get_subs():
                topic_filter = sub.get_topic_filter()

                authorized = True
                # Authorize (email, topic_filter)

                if not authorized:
                    self.db.delete_sub(session, sub)
            session.set_email(email)
            self.db.update(session)

        LOGGER.info('Responding with %s', response)
        if response:
            response['service'] = True
            self.publish(
                obj=response,
                queue=props.reply_to,
                correlation_id=props.correlation_id)