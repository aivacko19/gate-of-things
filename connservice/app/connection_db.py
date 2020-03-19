import psycopg2
from psycopg2 import OperationalError
import os
import connection


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

UPDATE_EMAIL = """
    UPDATE connection
    SET email = %s
    WHERE id = %s
"""

class ConnectionDB:

    __instance = None

    @staticmethod
    def getInstance():
        if not ConnectionDB.__instance:
            DB_NAME = os.environ.get('DB_NAME', 'mydb')
            DB_USER = os.environ.get('DB_USER', 'root')
            DB_PASS = os.environ.get('DB_PASS', 'root')
            DB_HOST = os.environ.get('DB_HOST', '192.168.99.100')
            ConnectionDB.__instance = ConnectionDB(DB_NAME, DB_USER, DB_PASS, DB_HOST)

        return ConnectionDB.__instance

    def __init__(self, db_name, db_user, db_password, db_host):
        self.connection = psycopg2.connect(
            database=db_name,
            user=db_user,
            password=db_password,
            host=db_host,)
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
        return connection.Connection(result)

    def get_by_socket(self, socket, reply_queue):
        cursor = self.connection.cursor()
        cursor.execute(SELECT_BY_SOCKET, (socket, reply_queue))
        result = cursor.fetchone()
        if not result:
            result = (None, False, socket, reply_queue, False, None, None)
        return connection.Connection(result)

    def add(self, conn):
        if conn.random_id:
            while True:
                conn.generate_id(23)
                cursor = self.connection.cursor()
                cursor.execute(SELECT, (conn.id,))
                if not cursor.fetchone(): break

        cursor = self.connection.cursor()
        cursor.execute(INSERT, conn.get_db_row())

    def update_email(self, conn):
        cursor = self.connection.cursor()
        cursor.execute(UPDATE_EMAIL, (conn.get_email(), conn.get_id()))

    def close(self):
        self.connection.close()



        
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