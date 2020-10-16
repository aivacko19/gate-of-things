import random

class Database:

    def __init__(self):
        self.db = {}

    def new(self):
        my_id = random.randit(1, 1000)
        self.db[my_id] = {'username': None, 'failed': False}
        return my_id

    def set_username(self, cid, username):
        self.db[cid]['username'] = username

    def set_failed(self, cid):
        self.db[cid]['failed'] = True

    def get(self, cid):
        return self.db.get(cid)

    def delete(self, cid):
        if not self.db.get(cid): return
        del self.db[cid]