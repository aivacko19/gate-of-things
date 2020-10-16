import psycopg2
from psycopg2 import OperationalError
import os
import logging

LOGGER = logging.getLogger(__name__)

ID_INDEX = 0
FAILED_INDEX = 1
USERNAME_INDEX = 2

CREATE_TABLE = """
    CREATE TABLE IF NOT EXISTS temporary_session (
        id SERIAL PRIMARY KEY,
        failed BOOLEAN,
        username VARCHAR(40)
    );
"""

insert_keys = ['failed', 'username']
insert_values = ", ".join(['%s'] * len(insert_keys))
INSERT = f"""
    INSERT INTO temporary_session ({", ".join(insert_keys)})
    VALUES ({insert_values})
    RETURNING id
"""

SELECT = """
    SELECT * FROM temporary_session
    WHERE id = %s
"""

DELETE = """
    DELETE FROM temporary_session
    WHERE id = %s
"""

UPDATE = """
    UPDATE temporary_session
    SET failed = %s, username = %s
    WHERE id = %s
"""

class Database:

    def __init__(self, dsn):
        self.connection = psycopg2.connect(dsn)
        self.connection.autocommit = True
        cursor = self.connection.cursor()
        cursor.execute(CREATE_TABLE)

    def new(self):
        cursor = self.connection.cursor()
        cursor.execute(INSERT, (False, None))
        result = cursor.fetchone()
        if result is None:
            return None

        return result[0]

    def set_username(self, cid, username):
        cursor = self.connection.cursor()
        cursor.execute(UPDATE, (False, username, cid))

    def set_failed(self, cid):
        cursor = self.connection.cursor()
        cursor.execute(UPDATE, (True, None, cid))

    def get(self, cid):
        cursor = self.connection.cursor()
        cursor.execute(SELECT, (cid,))
        result = cursor.fetchone()
        if result is None:
            return None

        return {
            'username': result[USERNAME_INDEX],
            'failed': result[FAILED_INDEX]}

    def delete(self, cid):
        cursor = self.connection.cursor()
        cursor.execute(DELETE, (cid,))
