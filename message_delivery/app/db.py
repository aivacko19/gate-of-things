import psycopg2
from psycopg2 import OperationalError
import os
import logging
import json
import base64

LOGGER = logging.getLogger(__name__)

class BytesEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, bytes):
            return {'__bytes__': True, '__value__': base64.b64encode(obj).decode('utf-8')}
        return json.JSONEncoder.default(self, obj)

def as_bytes(dct):
    if '__bytes__' in dct:
        return base64.b64decode(dct['__value__'].encode('utf-8'))
    return dct


CREATE_TABLE = """
    CREATE TABLE IF NOT EXISTS quota(
        cid VARCHAR(23) PRIMARY KEY,
        receive_max INTEGER,
        send_quota INTEGER
    );
    CREATE TABLE IF NOT EXISTS message (
        cid VARCHAR(23) NOT NULL,
        pid INTEGER NOT NULL,
        time_received INTEGER NOT NULL,
        received BOOLEAN DEFAULT FALSE,
        message VARCHAR NOT NULL,
        UNIQUE (cid, pid)
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

SELECT_PIDS = """
    SELECT pid FROM message
    WHERE cid = %s
    ORDER BY pid ASC
"""

insert_keys = ['cid', 'pid', 'time_received', 'message']
insert_values = ", ".join(['%s'] * len(insert_keys))
INSERT = f"""
    INSERT INTO message ({", ".join(insert_keys)})
    VALUES ({insert_values})
"""

SET_RECEIVED = """
    UPDATE message
    SET received = TRUE
    WHERE cid = %s
    AND pid = %s
"""

SELECT_CONNECTION = """
    SELECT * FROM quota
    WHERE cid = %s
"""

INSERT_CONNECTION = """
    INSERT INTO quota (cid, receive_max, send_quota)
    VALUES (%s, %s, %s)
"""

UPDATE_CONNECTION = """
    UPDATE quota
    SET receive_max = %s,
    send_quota = %s
    WHERE cid = %s
"""

class Database:

    def __init__(self, dsn):
        self.connection = psycopg2.connect(dsn)
        self.connection.autocommit = True
        cursor = self.connection.cursor()
        cursor.execute(CREATE_TABLE)

    def add(self, cid, time_received, message):
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
        message['id'] = pid
        cursor = self.connection.cursor()
        cursor.execute(INSERT, (cid, pid, time_received,
                                json.dumps(message, cls=BytesEncoder)))
        return pid

    def get(self, cid, pid):
        cursor = self.connection.cursor()
        cursor.execute(SELECT, (cid, pid))
        result = cursor.fetchone()
        if not result:
            return None

        return json.loads(result[4], object_hook=as_bytes), result[2]

    def delete(self, cid, pid):
        cursor = self.connection.cursor()
        cursor.execute(DELETE, (cid, pid))
        if cursor.rowcount > 0:
            self.inc_quota(cid)

    def set_received(self, cid, pid):
        cursor = self.connection.cursor()
        cursor.execute(SET_RECEIVED, (cid, pid))

    def get_quota(self, cid):
        cursor = self.connection.cursor()
        cursor.execute(SELECT_CONNECTION, (cid,))
        result = cursor.fetchone()
        if not result:
            return None
        return result[2]

    def set_quota(self, cid, quota):
        cursor = self.connection.cursor()
        cursor.execute(SELECT_CONNECTION, (cid,))
        result = cursor.fetchone()
        cursor = self.connection.cursor()
        if result:
            cursor.execute(UPDATE_CONNECTION, (quota, quota, cid))
        else:
            cursor.execute(INSERT_CONNECTION, (cid, quota, quota))

    def inc_quota(self, cid):
        cursor = self.connection.cursor()
        cursor.execute(SELECT_CONNECTION, (cid,))
        result = cursor.fetchone()
        quota = min(result[1], result[2] + 1)
        cursor = self.connection.cursor()
        cursor.execute(UPDATE_CONNECTION, (result[1], quota, cid))

    def dec_quota(self, cid):
        cursor = self.connection.cursor()
        cursor.execute(SELECT_CONNECTION, (cid,))
        result = cursor.fetchone()
        quota = result[2] - 1
        if quota >= 0:
            cursor = self.connection.cursor()
            cursor.execute(UPDATE_CONNECTION, (result[1], quota, cid))
        return quota