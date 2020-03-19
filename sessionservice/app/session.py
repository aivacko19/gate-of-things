
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



