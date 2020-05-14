import psycopg2
from psycopg2 import OperationalError
import os
import logging

LOGGER = logging.getLogger(__name__)

ID_INDEX = 0
USER_INDEX = 1
RESOURCE_INDEX = 2
ACTION_INDEX = 3
ACCESS_TIME_INDEX = 4

CREATE_TABLE = """
    CREATE TABLE IF NOT EXISTS log (
        id SERIAL PRIMARY KEY,
        client VARCHAR(40),
        resource VARCHAR(40),
        action VARCHAR(40),
        access_time TIMESTAMP
    );
"""

insert_keys = ['client', 'resource', 'action']
insert_values = ", ".join(['%s'] * len(insert_keys))
INSERT = f"""
    INSERT INTO log ({", ".join(insert_keys)}, access_time)
    VALUES ({insert_values}, CURRENT_TIMESTAMP)
"""

SELECT = """
    SELECT * FROM log
    WHERE resource = %s
"""

class DB:

    def __init__(self, db_name, db_user, db_password, db_host):
        self.connection = psycopg2.connect(
            database=db_name,
            user=db_user,
            password=db_password,
            host=db_host,)
        self.connection.autocommit = True
        cursor = self.connection.cursor()
        cursor.execute(CREATE_TABLE)

    def insert(self, user, resource, action):
        cursor = self.connection.cursor()
        cursor.execute(INSERT, (user, resource, action,))
        # result = cursor.fetchone()
        # if result is None:
        #     return None
        
        # return result[0]

    def select(self, resource):
        cursor = self.connection.cursor()
        cursor.execute(SELECT, (resource,))
        result = cursor.fetchall()
        if not result:
            return []

        logs = list()
        for row in result:

            log = {
                'id': row[ID_INDEX],
                'user': row[USER_INDEX],
                'resource': row[RESOURCE_INDEX],
                'action': row[ACTION_INDEX],
                'access_time': row[ACCESS_TIME_INDEX],
            }

            logs.append(log)

        return logs