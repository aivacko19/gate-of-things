
import amqp_helper

class Service(amqp_helper.AmqpAgent):

    def __init__(self, queue, db):
        self.db = db
        amqp_helper.AmqpAgent.__init__(self, queue)

    def main(self, request, props):

        command = request.get('command')
        del request['command']

        if command == 'verify':
            email = request.get('email')
            temp_id = int(props.correlation_id)
            if email:
                self.db.set_username(temp_id, email)
            else:
                self.db.set_failed(temp_id)