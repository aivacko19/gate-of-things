import psycopg2
from psycopg2 import OperationalError
import os
import random

CHARS = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

CREATE_TABLE = """
    CREATE TABLE IF NOT EXISTS connection (
        id VARCHAR(23) PRIMARY KEY,
        random_id BOOLEAN DEFAULT FALSE,
        socket VARCHAR(40),
        reply_queue VARCHAR(40),
        clean_start BOOLEAN DEFAULT FALSE,
        method VARCHAR(100),
        email VARCHAR(100) NULL,
        CONSTRAINT index_socket_reply UNIQUE (socket, reply_queue)
    )
"""

DELETE = """
    DELETE FROM connection C
    WHERE C.id = %s
"""

DELETE_BY_SOCKET = """
    DELETE FROM connection C
    WHERE C.socket = %s AND C.reply_queue = %s
"""

SELECT = """
    SELECT * FROM connection C
    WHERE C.id = %s
"""

SELECT_BY_SOCKET = """
    SELECT * FROM connection C
    WHERE C.socket = %s AND C.reply_queue = %s
"""

insert_keys = ['id', 'random_id', 'socket', 'reply_queue', 'clean_start', 'method', 'email']
insert_values = ", ".join(['%s'] * len(insert_keys))
INSERT = f"""
    INSERT INTO connection ({", ".join(insert_keys)})
    VALUES ({insert_values})
"""

UPDATE = """
    UPDATE connection
    SET email = %s
    WHERE id = %s
"""

class Database:

    def __init__(self, dsn):
        self.connection = psycopg2.connect(dsn)
        self.connection.autocommit = True
        cursor = self.connection.cursor()
        cursor.execute(CREATE_TABLE)

    def delete(self, cid):
        cursor = self.connection.cursor()
        cursor.execute(DELETE, (cid,))

    def delete_by_socket(self, socket, reply_queue):
        cursor = self.connection.cursor()
        cursor.execute(DELETE_BY_SOCKET, (socket, reply_queue))

    def get(self, cid):
        cursor = self.connection.cursor()
        cursor.execute(SELECT, (cid,))
        result = cursor.fetchone()
        if not result: return None
        return Connection(result)

    def get_by_socket(self, socket, reply_queue):
        cursor = self.connection.cursor()
        cursor.execute(SELECT_BY_SOCKET, (socket, reply_queue))
        result = cursor.fetchone()
        if not result:
            result = (None, False, socket, reply_queue, False, None, None)
        return Connection(result)

    def add(self, conn):
        if conn.random_id:
            while True:
                conn.generate_id(23)
                cursor = self.connection.cursor()
                cursor.execute(SELECT, (conn.id,))
                if not cursor.fetchone(): break

        cursor = self.connection.cursor()
        cursor.execute(INSERT, conn.get_db_row())
        return conn.get_id()

    def update(self, conn):
        cursor = self.connection.cursor()
        cursor.execute(UPDATE, (conn.get_email(), conn.get_id()))

    def close(self):
        self.connection.close()

class Connection:

    def __init__(self, db_row):
        self.id = db_row[0]
        self.random_id = db_row[1]
        self.socket = db_row[2]
        self.reply_queue = db_row[3]
        self.clean_start = db_row[4]
        self.method = db_row[5]
        self.email = db_row[6]

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
        
if __name__ == '__main__':
    DB_NAME = os.environ.get('DB_NAME', 'mydb')
    DB_USER = os.environ.get('DB_USER', 'root')
    DB_PASS = os.environ.get('DB_PASS', 'root')
    DB_HOST = os.environ.get('DB_HOST', '192.168.99.100')
    db = ConnectionDB(DB_NAME, DB_USER, DB_PASS, DB_HOST)

    conn = db.get_by_socket("123", "432")
    # conn.set_method("OAuth2.0")
    # conn.set_id("clientjkl")
    # db.add(conn)
    db.delete(conn.get_id())

    
    # # db.get(conn.get_id())
    # db.add(conn)
    # conn = db.get_by_socket("123", "432")
    # print(conn.get_id())
    # db.delete(conn.get_id())

    db.close()