import random

CHARS = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

class Connection:

    def __init__(self, db_row):
        self.id = db_row[0]
        self.random_id = db_row[1]
        self.socket = db_row[2]
        self.reply_queue = db_row[3]
        self.method = db_row[4]
        self.email = db_row[5]

    def verified(self):
        return self.email is not None

    def connected(self):
        return self.id is not None

    def get_db_row(self):
        return (self.id,
                self.random_id,
                self.socket,
                self.reply_queue,
                self.method,
                self.email,)

    def get_id(self):
        return self.id

    def get_random_id(self):
        return self.random_id

    def get_method(self):
        return self.method

    def get_socket(self):
        return self.socket, self.reply_queue

    def set_random_id(self, random_id):
        self.random_id = random_id

    def set_method(self, method):
        self.method = method

    def set_email(self, email):
        self.email = email

    def generate_id(self, length):
        self.id = ""
        for i in range(length):
            self.id += CHARS[random.randrange(len(CHARS))]
        self.random_id = True




