import json

class Database:

    def __init__(self):
        self.db = {}
        self.quota = {}

    def add(self, cid, time_received, message):
        quota = self.dec_quota(cid)
        if quota < 0: return -1

        if not self.db.get(cid): 
            self.db[cid] = {}
        pid = 1
        for existing_pid in self.db.get(cid):
            if existing_pid > pid: break
            pid += 1
        message['id'] = pid
        self.db[cid][pid] = {'time_received': time_received, 'message': message, 'received': False}
        return pid

    def get(self, cid, pid):
        if not self.db.get(cid): return None, None
        row = self.db.get(cid).get(pid)
        if not row: return None, None

        return row.get('message'), row.get('time_received')

    def delete(self, cid, pid):
        if not self.db.get(cid): return False
        if not self.db.get(cid).get(pid): return False
        del self.db[cid][pid]
        self.inc_quota(cid)
        return True

    def set_received(self, cid, pid):
        if not self.db.get(cid): return False
        if not self.db.get(cid).get(pid): return False
        self.db[cid][pid]['received'] = True
        return True

    def get_quota(self, cid):
        if not self.quota.get(cid): return None
        return self.quota.get(cid).get('send_quota')

    def set_quota(self, cid, quota):
        self.quota[cid] = {'receive_max': quota, 'send_quota': quota}

    def inc_quota(self, cid):
        if not self.quota.get(cid): return
        row = self.quota.get(cid)
        quota = min(row.get('receive_max'), row.get('send_quota') + 1)
        self.quota[cid]['send_quota'] = quota

    def dec_quota(self, cid):
        if not self.quota.get(cid): return -1
        row = self.quota.get(cid)
        quota = row.get('send_quota') - 1
        if quota >= 0:
            self.quota[cid]['send_quota'] = quota
        return quota