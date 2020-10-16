import random

CHARS = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

ID_INDEX = 0
RANDOM_ID_INDEX = 1
SOCKET_INDEX = 2
REPLY_QUEUE_INDEX = 3
CLEAN_START_INDEX = 4
METHOD_INDEX = 5
EMAIL_INDEX = 6

class Database:

    def __init__(self):
        self.db = {}

    def delete(self, cid):
        if not self.db.get(cid): return False
        del self.db[cid]
        return True

    def delete_by_socket(self, socket, reply_queue):
        cid = None
        for key, connection in self.db,items():
            if connection[SOCKET_INDEX]== socket:
                if connection[REPLY_QUEUE_INDEX] == reply_queue:
                    cid = key
        if not cid: return False
        del self.db[cid]
        return True

    def get(self, cid):
        if not self.db.get(cid): return None
        return Connection(self.db.get(cid))

    def get_by_socket(self, socket, reply_queue):
        result = None
        for key, connection in self.db,items():
            if connection[SOCKET_INDEX] == socket:
                if connection[REPLY_QUEUE_INDEX] == reply_queue:
                    result = connection
        if not result:
            result = (None, False, socket, reply_queue, False, None, None)
        return Connection(result)

    def add(self, conn):
        if conn.random_id:
            while True:
                conn.generate_id(23)
                if not self.db.get(conn.get_id()): break

        self.db[conn.get_id()] = conn.get_db_row()
        return conn.get_id()

    def update(self, conn):
        if not self.db.get(conn.get_id()): return False
        self.db[conn.get_id()][EMAIL_INDEX] = conn.get_email()
        return True

class Connection:

    def __init__(self, db_row):
        self.id = db_row[ID_INDEX]
        self.random_id = db_row[RANDOM_ID_INDEX]
        self.socket = db_row[SOCKET_INDEX]
        self.reply_queue = db_row[REPLY_QUEUE_INDEX]
        self.clean_start = db_row[CLEAN_START_INDEX]
        self.method = db_row[METHOD_INDEX]
        self.email = db_row[EMAIL_INDEX]

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

    def set_parameters(self, cid, random_id, clean_start, method):
        self.id = cid
        self.random_id = random_id
        self.clean_start = clean_start
        self.method = method

    def set_email(self, email):
        self.email = email

    def generate_id(self, length):
        self.id = ""
        for i in range(length):
            self.id += CHARS[random.randrange(len(CHARS))]
        self.random_id = True
        