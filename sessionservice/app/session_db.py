import psycopg2
from psycopg2 import OperationalError
import os
from session import Session


CREATE_TABLE = """
    CREATE TABLE IF NOT EXISTS session (
        id VARCHAR(23) PRIMARY KEY,
        email VARCHAR(100)
    );
    CREATE TABLE IF NOT EXISTS subscription (
        id SERIAL PRIMARY KEY,
        session_id VARCHAR(23),
        topic_filter VARCHAR(40),
        subscription_id INTEGER,
        max_qos INTEGER CHECK (max_qos BETWEEN 0 AND 2),
        no_local BOOLEAN,
        retain_as_published BOOLEAN,
        retain_handling INTEGER CHECK (retain_handling BETWEEN 0 AND 2),
        UNIQUE (session_id, topic_filter)
        FOREIGN KEY (session_id) REFERENCES session ON DELETE CASCADE
    );
"""

DELETE = """
    DELETE FROM session
    WHERE id = %s
"""

SELECT = """
    SELECT * FROM session
    WHERE id = %s
"""

SELECT_SUBS = """
    SELECT topic_filter, subscription_id, max_qos, 
    no_local, retain_as_published, retain_handling 
    FROM subscription
    WHERE session_id = %s
"""

insert_keys = ['id', 'email']
insert_values = ", ".join(['%s'] * len(insert_keys))
INSERT = f"""
    INSERT INTO session ({", ".join(insert_keys)})
    VALUES ({insert_values})
"""

insert_keys = ['session_id', 'topic_filter', 'subscription_id',
    'max_qos', 'no_local', 'retain_as_published', 'retain_handling']
insert_values = ", ".join(['%s'] * len(insert_keys))
INSERT_SUB = f"""
    INSERT INTO subscription ({", ".join(insert_keys)})
    VALUES ({insert_values})
"""

UPDATE_EMAIL = """
    UPDATE session
    SET email = %s
    WHERE id = %s
"""

UPDATE_SUB = """
    UPDATE subscription
    SET subscription_filter = %s,
    max_qos = %s,
    no_local = %s,
    retain_as_published = %s,
    retain_handling = %s,
    WHERE session_id = %s
    AND topic_filter = %s
"""

class SessionDB:

    __instance = None

    @staticmethod
    def getInstance():
        if not SessionDB.__instance:
            DB_NAME = os.environ.get('DB_NAME', 'mydb')
            DB_USER = os.environ.get('DB_USER', 'root')
            DB_PASS = os.environ.get('DB_PASS', 'root')
            DB_HOST = os.environ.get('DB_HOST', '192.168.99.100')
            SessionDB.__instance = SessionDB(DB_NAME, DB_USER, DB_PASS, DB_HOST)

        return SessionDB.__instance

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

    def get(self, cid):
        cursor = self.connection.cursor()
        cursor.execute(SELECT, (cid,))
        result = cursor.fetchone()
        if not result: return None
        session = Session(result)
        cursor = self.connection.cursor()
        cursor.execute(SELECT_SUBS, (cid,))
        result = cursor.fetchall()
        for sub in result:
            session.add_sub({
                'topic_filter': sub[0],
                'subscription_id': sub[1],
                'max_qos': sub[2],
                'no_local': sub[3],
                'retain_as_published': sub[4],
                'retain_handling': sub[5]})
        return session


    def add(self, session):
        cursor = self.connection.cursor()
        cursor.execute(INSERT, session.get_db_row())

    def add_sub(self, session, sub):
        cursor = self.connection.cursor()
        cursor.execute(INSERT_SUB, (
            session.get_id(),
            sub['topic_filter'],
            sub['subscription_id'],
            sub['max_qos'],
            sub['no_local'],
            sub['retain_as_published'],
            sub['retain_handling'],))

    def update_email(self, session):
        cursor = self.connection.cursor()
        cursor.execute(UPDATE_EMAIL, (session.get_email(), session.get_id()))

    def update_sub(self, session, sub):
        cursor = self.connection.cursor()
        cursor.execute(UPDATE_SUB, (
            sub['subscription_id'],
            sub['max_qos'],
            sub['no_local'],
            sub['retain_as_published'],
            sub['retain_handling']),
            session.get_id(),
            sub['topic_filter'],)

    def close(self):
        self.connection.close()



        
if __name__ == '__main__':
    DB_NAME = os.environ.get('DB_NAME', 'mydb')
    DB_USER = os.environ.get('DB_USER', 'root')
    DB_PASS = os.environ.get('DB_PASS', 'root')
    DB_HOST = os.environ.get('DB_HOST', '192.168.99.100')
    db = SessionDB(DB_NAME, DB_USER, DB_PASS, DB_HOST)

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