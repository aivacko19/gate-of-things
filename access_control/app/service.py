
import os
import logging

import amqp_helper

LOGGER = logging.getLogger(__name__)

env = {
}

for key in env:
    service = os.environ.get(key)
    if not service:
        raise Exception('Environment variable %s not defined', key)
    env[key] = service

class AccessControlService(amqp_helper.AmqpAgent):

    def __init__(self, queue, db):
        self.db = db
        amqp_helper.AmqpAgent.__init__(self, queue)

    def main(self, request, props):

        command = request.get('command')
        response = {}

        if command == 'add_policy':
            user = request.get('user')
            resource = request.get('resource')
            read = request.get('read')
            write = request.get('write')
            self.db.add(user, resource, read, write)

        elif command == 'update_policy':
            user = request.get('user')
            resource = request.get('resource')
            read = request.get('read')
            write = request.get('write')
            self.db.update(user, resource, read, write)

        elif command == 'delete_policy':
            user = request.get('user')
            resource = request.get('resource')
            self.db.delete(user, resource)

        elif command == 'delete_resource':
            resource = request.get('resource')
            self.db.delete_resource(resource)

        elif command == 'get_read_access':
            user = request.get('user')
            resource = request.get('resource')
            read_access = self.db.can_read(user, resource)
            response = {'read_access': read_access,}

        elif command == 'get_write_access':
            user = request.get('user')
            resource = request.get('resource')
            write_access = self.db.can_write(user, resource)
            response = {'write_access': write_access,}

        elif command == 'get_resource':
            resource = request.get('resource')
            response = self.db.get_resource(resource)

        if response:
            self.publish(
                obj=response,
                queue=props.reply_to,
                correlation_id=props.correlation_id)



   







