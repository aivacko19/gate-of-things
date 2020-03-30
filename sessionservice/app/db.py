import psycopg2
from psycopg2 import OperationalError
import os
import logging

LOGGER = logging.getLogger(__name__)


CREATE_TABLE = """
    CREATE TABLE IF NOT EXISTS session (
        id SERIAL PRIMARY KEY,
        cid VARCHAR(23) UNIQUE,
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
        UNIQUE (session_id, topic_filter),
        FOREIGN KEY (session_id) REFERENCES session (cid) ON DELETE CASCADE
    );
    CREATE TABLE IF NOT EXISTS packet_identifier (
        session_id VARCHAR(23),
        packet_id INTEGER,
        FOREIGN KEY (session_id) REFERENCES session (cid) ON DELETE CASCADE
    )
"""

DELETE = """
    DELETE FROM session
    WHERE cid = %s
"""

DELETE_SUB = """
    DELETE FROM subscription
    WHERE session_id = %s
    AND topic_filter = %s
"""

SELECT = """
    SELECT * FROM session
    WHERE cid = %s
"""

SELECT_SUB = """
    SELECT topic_filter, subscription_id, max_qos, 
    no_local, retain_as_published, retain_handling, session_id
    FROM subscription
    WHERE session_id = %s
    AND topic_filter = %s
"""

SELECT_SUBS = """
    SELECT topic_filter, subscription_id, max_qos, 
    no_local, retain_as_published, retain_handling, session_id
    FROM subscription
    WHERE session_id = %s
"""

SELECT_MATCHING_SUBS = """
    SELECT topic_filter, subscription_id, max_qos, 
    no_local, retain_as_published, retain_handling, session_id
    FROM subscription
    WHERE topic_filter = %s
"""

SELECT_PACKET_IDS = """
    SELECT packet_id FROM packet_identifier
    WHERE session_id = %s
"""

INSERT_PACKET_ID = """
    INSERT INTO packet_identifier (session_id, packet_id)
    VALUES (%s, %s)
"""

DELETE_PACKET_ID = """
    DELETE FROM packet_identifier
    WHERE session_id = %s
    AND packet_id = %s
"""

insert_keys = ['cid', 'email']
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

UPDATE = """
    UPDATE session
    SET email = %s
    WHERE cid = %s
"""

UPDATE_SUB = """
    UPDATE subscription
    SET subscription_id = %s,
    max_qos = %s,
    no_local = %s,
    retain_as_published = %s,
    retain_handling = %s
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

    def delete(self, session):
        cursor = self.connection.cursor()
        cursor.execute(DELETE, (session.get_id(),))

    def delete_sub(self, sub):
        cursor = self.connection.cursor()
        cursor.execute(SELECT_SUB, (sub.get_session_id(), sub.get_topic_filter()))

    def get(self, cid):
        cursor = self.connection.cursor()
        LOGGER.info('Executing SELECT statement for table "session"')
        cursor.execute(SELECT, (cid,))
        result = cursor.fetchone()
        if not result: return None
        session = Session(result)
        cursor = self.connection.cursor()
        LOGGER.info('Executing SELECT statement for table "subscription')
        cursor.execute(SELECT_SUBS, (session.get_id(),))
        result = cursor.fetchall()
        for sub in result:
            session.add_sub(Subscription(sub))
        return session

    def get_sub(self, session, topic_filter):
        cursor = self.connection.cursor()
        cursor.execute(SELECT_SUB, (session.get_id(), topic_filter))
        result = cursor.fetchone()
        if not result: return None
        return Subscription(result)

    def get_matching_subs(self, topic):
        cursor = self.connection.cursor()
        cursor.execute(SELECT_MATCHING_SUBS, (topic,))
        result = cursor.fetchall()
        subs = list()
        for sub in result:
            subs.append(Subscription(sub))
        return subs


    def add(self, cid, email):
        cursor = self.connection.cursor()
        cursor.execute(INSERT, (cid, email))

    def add_sub(self, session, sub):
        exists = self.get_sub(session, sub['filter'])
        if exists:
            self.update_sub(sub)
        else:
            self.insert_sub(sub)

    def insert_sub(self, sub):
        cursor = self.connection.cursor()
        cursor.execute(INSERT_SUB, (
            sub.get('session_id'),
            sub.get('filter'),
            sub.get('sub_id'),
            sub.get('max_qos'),
            sub.get('no_local'),
            sub.get('retain_as_published'),
            sub.get('retain_handling'),))

    def update(self, session):
        cursor = self.connection.cursor()
        cursor.execute(UPDATE, (session.get_email(), session.get_id()))

    def update_sub(self, session, sub):
        cursor = self.connection.cursor()
        cursor.execute(UPDATE_SUB, (
            sub.get('sub_id'),
            sub.get('max_qos'),
            sub.get('no_local'),
            sub.get('retain_as_published'),
            sub.get('retain_handling'),
            sub.get('session_id'),
            sub.get('filter'),))

    def get_packet_ids(self, session):
        cursor = self.connection.cursor()
        cursor.execute(SELECT_PACKET_IDS, (session.get_id(),))
        result = cursor.fetchall()
        ids = list()
        for packet_id in result:
            ids.append(packet_id[0])
        return ids

    def add_packet_id(self, session_id, packet_id):
        cursor = self.connection.cursor()
        cursor.execute(INSERT_PACKET_ID, (session_id, packet_id))

    def delete_packet_id(self, session_id, packet_id):
        cursor = self.connection.cursor()
        cursor.execute(DELETE_PACKET_ID, (session_id, packet_id))

    def close(self):
        self.connection.close()


class Session:

    def __init__(self, db_row):
        self.id = db_row[1]
        self.email = db_row[2]
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

    def __init__(self, db_row):
        self.topic_filter = db_row[0]
        self.sub_id = db_row[1]
        self.max_qos = db_row[2]
        self.no_local = db_row[3]
        self.retain_as_published = db_row[4]
        self.retain_handling = db_row[5]
        self.session_id = db_row[6]

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

    def get_session_id(self):
        return self.session_id

        
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