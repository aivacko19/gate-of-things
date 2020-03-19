
class Packet:

    def __init__(self, db_row):
        self.id = db_row[0]
        self.server_owned = db_row[1]
        self.client_id = db_row[2]
        self.packet_id = db_row[3]
        self.type = db_row[4]

    def get_db_row(self):
        return (self.id,
                self.server_owned,
                self.client_id,
                self.packet_id,
                self.type,)

    def get_id(self):
        return self.id

    def get_server_owned(self):
        return self.server_owned

    def get_client_id(self):
        return self.client_id

    def get_packet_id(self):
        return self.packet_id

    def get_type(self):
        return self.type

    def set_id(self, cid):
        self.id = cid

    def set_email(self, email):
        self.email = email

    def add_sub(self, sub):
        self.subs.append(sub)



