
import os
import logging

from . import abstract_service

LOGGER = logging.getLogger(__name__)

env = {
    'LOGGER_SERVICE': None
}

for key in env:
    service = os.environ.get(key)
    if not service:
        raise Exception('Environment variable %s not defined', key)
    env[key] = service

class Service(abstract_service.AbstractService):

    def __init__(self, queue, db, dummy_messenger=None):
        self.db = db
        abstract_service.AbstractService.__init__(self, queue)
        self.dummy_messenger = dummy_messenger
        self.actions = {
            'add_policy': self.add_policy,
            'update_policy': self.update_policy,
            'delete_policy': self.delete_policy,
            'delete_resource': self.delete_resource,
            'get_read_access': self.get_read_access,
            'get_write_access': self.get_write_access,
            'get_own_access': self.get_own_access,
            'get_resource': self.get_resource,
            'get_owned_resources': self.get_owned_resources,}

    # Add a new policy
    def add_policy(self, request, props):
        user = request.get('user')
        resource = request.get('resource')
        read = request.get('read')
        write = request.get('write')
        own = request.get('own', False)
        access_time = request.get('access_time', 0)

        # Save previous ownership status
        prev_own = self.db.owns(user, resource,)

        added = self.db.add(user, resource, read, write, own, access_time)

        # Add access to logs (or remove)
        if added and own != prev_own:
            logger_message = {
                'owner': user,
                'resource': resource,}
            logger_message['command'] = 'add_ownership' if own else 'remove_ownership'
            self.publish(
                request=logger_message,
                queue=env['LOGGER_SERVICE'],)

    # Update a policy 
    def update_policy(self, request, props):
        user = request.get('user')
        resource = request.get('resource')
        read = request.get('read')
        write = request.get('write')
        own = request.get('own', False)

        # Save previous ownership status
        prev_own = self.db.owns(user, resource,)

        updated = self.db.update(user, resource, read, write, own)

        # Update access to logs if ownership status changed
        if updated and own != prev_own:
            logger_message = {
                'owner': user,
                'resource': resource,}
            logger_message['command'] = 'add_ownership' if own else 'remove_ownership'
            self.publish(
                request=logger_message,
                queue=env['LOGGER_SERVICE'],)

    # Delete policy 
    def delete_policy(self, request, props):
        user = request.get('user')
        resource = request.get('resource')

        # Save previous ownership status
        prev_own = self.db.owns(user, resource,)

        deleted = self.db.delete(user, resource)

        # Revoke access to logs
        if updated and prev_own:
            logger_message = {
                'command': 'remove_ownership',
                'owner': user,
                'resource': resource,}
            self.publish(
                request=logger_message,
                queue=env['LOGGER_SERVICE'],)

    # Delete all policies for one resource
    def delete_resource(self, request, props):
        resource = request.get('resource')
        policies = self.db.get_resource(resource)
        for user, policy in policies.items():
            LOGGER.info(policy)
            if policy.get('own'):
                logger_message = {
                    'command': 'remove_ownership',
                    'owner': user,
                    'resource': resource,}
                self.publish(
                    request=logger_message,
                    queue=env['LOGGER_SERVICE'],)
        self.db.delete_resource(resource)

    # Get subscribe rights
    def get_read_access(self, request, props):
        user = request.get('user')
        resource = request.get('resource')
        read_access, access_time = self.db.can_read(user, resource)
        return {'read_access': read_access, 'access_time': access_time}

    # Get publish rights
    def get_write_access(self, request, props):
        user = request.get('user')
        resource = request.get('resource')
        write_access = self.db.can_write(user, resource)
        return {'write_access': write_access,}

    # Get ownership rights
    def get_own_access(self, request, props):
        user = request.get('user')
        resource = request.get('resource')
        own_access = self.db.owns(user, resource)
        return {'own_access': own_access,}

    # Get policies for one resource
    def get_resource(self, request, props):
        resource = request.get('resource')
        policies = self.db.get_resource(resource)
        return {'policies': policies,}

    # Get owned resources
    def get_owned_resources(self, request, props):
        user = request.get('user')
        resources = self.db.get_owned_resources(user)
        return {'resources': resources}



   








