
class Session:

    def __init__(self, db_row):
        self.id = db_row[0]
        self.email = db_row[5]
        self.subs = list()

    def get_db_row(self):
        return (self.id,
                self.email,)

    def get_id(self):
        return self.id

    def get_email(self):
        return self.email

    def set_id(self, cid):
        self.id = cid

    def set_email(self, email):
        self.email = email

    def add_sub(self, sub):
        self.subs.append(sub)

    def get_subs(self):
        return self.subs

class Subscription:

    def __init__(self, db_row=None, sub_id=None, packet=None):
        if db_row:
            self.topic_filter = db_row[0]
            self.sub_id = db_row[1]
            self.max_qos = db_row[2]
            self.no_local = db_row[3]
            self.retain_as_published = db_row[4]
            self.retain_handling = db_row[5]
        else:
            self.topic_filter = packet.get("filter")
            self.sub_id = sub_id
            self.max_qos = packet.get('max_qos')
            self.no_local = packet.get('no_local')
            self.retain_as_published = packet.get('retain_as_published')
            self.retain_handling = packet.get('retain_handling')

    def get_topic_filter(self):
        return self.topic_filter

    def get_sub_id(self):
        return self.sub_id
        
    def get_max_qos(self):
        return self.max_qos
        
    def get_no_local(self):
        return self.no_local
        
    def get_retain_as_published(self):
        return self.retain_as_published
        
    def get_retain_handling(self):
        return self.retain_handling




