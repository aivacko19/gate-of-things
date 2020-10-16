class Database:

    def __init__(self):
        self.db = {}

    def insert(self, name, owner, key, disabled=False):
        if self.db.get(name): return None
        self.db[name] = {'id': 1, 'name': name, 'owner': owner, 'key': key, 'disabled': disabled}
        return 1

    def select(self, name):
        return self.db.get(name)

    def select_by_owner(self, owner):
        devices = {}
        for device in self.db.values():
            if device.get('owner') == owner:
                devices[device.get('name')] = device
        return devices

    def update_key(self, name, key):
        if not self.db.get(name): return False
        self.db[name]['key'] = key
        return True

    def update_disabled(self, name, disabled):
        if not self.db.get(name): return False
        self.db[name]['disabled'] = disabled
        return True

    def delete(self, name):
        if not self.db.get(name): return False
        del self.db[name]
        return True