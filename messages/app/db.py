import psycopg2
from psycopg2 import OperationalError
import os
import logging

LOGGER = logging.getLogger(__name__)


CREATE_TABLE = """
    CREATE TABLE IF NOT EXISTS connection(
        cid VARCHAR(23) PRIMARY KEY,
        receive_max INTEGER,
        send_quota INTEGER
    );
    CREATE TABLE IF NOT EXISTS message (
        id SERIAL PRIMARY KEY,
        cid VARCHAR(23) NOT NULL,
        topic VARCHAR(40) NOT NULL,
        pid INTEGER NOT NULL,
        payload BITEA NOT NULL,
        time_received INTEGER NOT NULL,
        received BOOLEAN DEFAULT FALSE,
        payload_format INTEGER,
        content_type VARCHAR,
        message_expiry INTEGER,
        topic_alias INTEGER,
        response_topic VARCHAR,
        correlation_data BITEA,
        subscription_id INTEGER,
        UNIQUE (cid, pid)
    );
    CREATE TABLE IF NOT EXISTS user_property (
        message_id INTEGER,
        order INTEGER,
        key VARCHAR,
        value VARCHAR,
        PRIMARY KEY (message_id, order),
        FOREIGN KEY (message_id) REFERENCES message ON DELETE CASCADE
    );
"""

DELETE = """
    DELETE FROM message
    WHERE cid = %s
    AND pid = %s
"""

SELECT = """
    SELECT * FROM message
    WHERE cid = %s
    AND pid = %s
"""

SELECT_USER_PROPERTY = """
    SELECT key, value FROM user_property
    WHERE message_id = %s
    ORDER BY order ASC
"""

SELECT_PIDS = """
    SELECT pid FROM message
    WHERE cid = %s
    ORDER BY pid ASC
"""

insert_keys = ['cid', 'topic', 'pid', 'payload', 'time_received', 'payload_format',
               'content_type', 'message_expiry', 'topic_alias', 'response_topic',
               'correlation_data', 'subscription_id']
insert_values = ", ".join(['%s'] * len(insert_keys))
INSERT = f"""
    INSERT INTO message ({", ".join(insert_keys)})
    VALUES ({insert_values})
"""

INSERT_USER_PROPERTY = """
    INSERT INTO user_property (message_id, order, key, value)
    VALUES (%s, %s, %s, %s)
"""

SET_RECEIVED = """
    UPDATE message
    SET received = TRUE
    WHERE cid = %s
    AND pid = %s
"""

SELECT_CONNECTION = """
    SELECT * FROM connection
    WHERE cid = %s
"""

INSERT_CONNECTION = """
    INSERT INTO connection (cid, receive_max, send_quota)
    VALUES (%s, %s, %s)
"""

UPDATE_CONNECTION = """
    UPDATE connection
    SET receive_max = %s,
    send_quota = %s
    WHERE cid = %s
"""

class MessageDB:

    def __init__(self, db_name, db_user, db_password, db_host):
        self.connection = psycopg2.connect(
            database=db_name,
            user=db_user,
            password=db_password,
            host=db_host,)
        self.connection.autocommit = True
        cursor = self.connection.cursor()
        cursor.execute(CREATE_TABLE)

    def add(cid, message):
        quota = self.dec_quota(cid)
        if quota < 0:
            return -1
        cursor = self.connection.cursor()
        cursor.execute(SELECT_PIDS, (cid,))
        results = cursor.fetchall()
        pid = 1
        for result in results:
            if result[0] > pid:
                break
            pid += 1
        cursor = self.connection.cursor()
        cursor.execute(INSERT, (
            cid,
            message.get('topic'),
            pid,
            message.get('payload'),
            message.get('received'),
            message.get('properties').get('payload_format_indicator'),
            message.get('properties').get('content_type'),
            message.get('properties').get('message_expiry'),
            message.get('properties').get('topic_alias'),
            message.get('properties').get('response_topic'),
            message.get('properties').get('correlation_data'),
            message.get('properties').get('subscription_id')))
        message_id = cursor.fetchone[0]
        user_properties = message.get('properties').get('user_property')
        for index, user_property in enumerate(user_properties):
            cursor = self.connection.cursor()
            cursor.execute(INSERT_USER_PROPERTY, 
                (message_id, index, user_property[0], user_property[1]))
        return pid

    def get(cid, pid):
        cursor = self.connection.cursor()
        cursor.execute(SELECT, (cid, pid))
        result = cursor.fetchone()
        if not result:
            return None

        message = {
            'topic': result[2],
            'id': result[3],
            'payload': result[4],
            'received': result[5],
            'properties': {'user_properties': list()}
        }

        if result[6]:
            message['properties']['payload_format_indicator'] = result[6]
        if result[7]:
            message['properties']['content_type'] = result[7]
        if result[8]:
            message['properties']['message_expiry'] = result[8]
        if result[9]:
            message['properties']['topic_alias'] = result[9]
        if result[10]:
            message['properties']['response_topic'] = result[10]
        if result[11]:
            message['properties']['correlation_data'] = result[11]
        if result[12]:
            message['properties']['subscription_identifier'] = result[12]
        message_id = result[0]
        cursor = self.connection.cursor()
        cursor.execute(SELECT_USER_PROPERTY, (message_id,))
        user_properties = cursor.fetchall()
        for user_property in user_properties:
            message['properties']['user_properties'].append((user_property[0], user_property[1]))
        return message

    def delete(cid, pid):
        cursor = self.connection.cursor()
        cursor.execute(DELETE, (cid, pid))
        if cursor.fetchone[0] > 0:
            self.inc_quota(cid)

    def set_received(cid, pid):
        cursor = self.connection.cursor()
        cursor.execute(SET_RECEIVED, (cid, pid))

    def get_quota(cid):
        cursor = self.connection.cursor()
        cursor.execute(SELECT_CONNECTION, (cid,))
        result = cursor.fetchone()
        if not result:
            return None
        return result[2]

    def set_quota(cid, quota):
        cursor = self.connection.cursor()
        cursor.execute(SELECT_CONNECTION, (cid,))
        result = cursor.fetchone()
        cursor = self.connection.cursor()
        if result:
            cursor.execute(UPDATE_CONNECTION, (quota, quota, cid))
        else:
            cursor.execute(INSERT_CONNECTION, (cid, quota, quota))

    def inc_quota(cid):
        cursor = self.connection.cursor()
        cursor.execute(SELECT_CONNECTION, (cid,))
        result = cursor.fetchone()
        quota = min(result[1], result[2] + 1)
        cursor = self.connection.cursor()
        cursor.execute(UPDATE_CONNECTION, (result[1], quota, cid))

    def dec_quota(cid):
        cursor = self.connection.cursor()
        cursor.execute(SELECT_CONNECTION, (cid,))
        result = cursor.fetchone()
        quota = result[2] - 1
        if quota >= 0:
            cursor = self.connection.cursor()
            cursor.execute(UPDATE_CONNECTION, (result[1], quota, cid))
        return quota