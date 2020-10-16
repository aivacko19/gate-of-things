class Database:

    def __init__(self):
        self.db = {}

    def _check(self, user, resource):
        if resource not in self.db: return False
        if user not in self.db.get(resource): return False
        return True

    def add(self, user, resource, read=False, write=False, own=False, access_time=0):
        if self._check(user, resource):
            return self.update(user, resource, read, write, own, access_time)

        if resource not in self.db: self.db[resource] = {}
        self.db[resource][user] = {'read': read, 'write': write, 'own': own, 'access_time': access_time}
        return True

    def get_resource(self, resource):
        return self.db.get(resource, {})

    def can_read(self, user, resource):
        if not self._check(user, resource): return False, 0
        return self.db.get(resource).get(user).get('read'), self.db.get(resource).get(user).get('access_time')

    def can_write(self, user, resource):
        if not self._check(user, resource): return False
        return self.db.get(resource).get(user).get('write')

    def owns(self, user, resource):
        if not self._check(user, resource): return False
        return self.db.get(resource).get(user).get('own')

    def update(self, user, resource, read=False, write=False, own=False, access_time=0):
        if not self._check(user, resource):
            return self.add(user, resource, read, write, own, access_time)

        self.db[resource][user] = {'read': read, 'write': write, 'own': own, 'access_time': access_time}
        return True

    def delete(self, user, resource):
        if not self._check(user, resource): return False
        del self.db[resource][user]
        return True

    def delete_resource(self, resource):
        if resource not in self.db: return False
        del self.db[resource]
        return True

    def get_owned_resources(self, user):
        resources = []
        for resource in self.db:
            if user in resource: resources.append(resource)
        return resources