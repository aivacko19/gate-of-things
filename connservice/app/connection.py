import random

CHARS = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

class Connection:

    def __init__(self, db_row):
        self.id = db_row[0]
        self.random_id = db_row[1]
        self.socket = db_row[2]
        self.reply_queue = db_row[3]
        self.clean_start = db_row[4]
        self.method = db_row[5]
        self.email = db_row[6]

    def verified(self):
        return self.email is not None

    def connected(self):
        return self.id is not None

    def get_db_row(self):
        return (self.id,
                self.random_id,
                self.socket,
                self.reply_queue,
                self.clean_start,
                self.method,
                self.email,)

    def get_id(self):
        return self.id

    def get_random_id(self):
        return self.random_id

    def get_clean_start(self):
        cs = self.clean_start
        self.clean_start = False
        return cs

    def get_method(self):
        return self.method

    def get_socket(self):
        return self.socket, self.reply_queue

    def get_email(self):
        return self.email

    def set_id(self, cid):
        self.id = cid

    def set_random_id(self, random_id):
        self.random_id = random_id

    def set_clean_start(self, clean_start):
        self.clean_start = clean_start

    def set_method(self, method):
        self.method = method

    def set_email(self, email):
        self.email = email

    def generate_id(self, length):
        self.id = ""
        for i in range(length):
            self.id += CHARS[random.randrange(len(CHARS))]
        self.random_id = True




