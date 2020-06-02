import psycopg2
from psycopg2 import OperationalError
import os
import logging

LOGGER = logging.getLogger(__name__)

CREATE_TABLE = """
    CREATE TABLE IF NOT EXISTS policy (
        id SERIAL PRIMARY KEY,
        user_id VARCHAR(40) NOT NULL,
        resource VARCHAR(40) NOT NULL,
        read BOOLEAN DEFAULT FALSE,
        write BOOLEAN DEFAULT FALSE,
        UNIQUE (user_id, resource)
    );
"""

ID_INDEX = 0
USER_INDEX = 1
RESOURCE_INDEX = 2
READ_INDEX = 3
WRITE_INDEX = 4

insert_keys = ['user_id', 'resource', 'read', 'write']
insert_values = ", ".join(['%s'] * len(insert_keys))
INSERT = f"""
    INSERT INTO policy ({", ".join(insert_keys)})
    VALUES ({insert_values})
"""

SELECT = """
    SELECT * FROM policy
    WHERE user_id = %s
    AND resource = %s
"""

SELECT_RESOURCE = """
    SELECT * FROM policy
    WHERE resource = %s 
"""

UPDATE = """
    UPDATE policy
    SET read = %s
    AND write = %s
    WHERE user_id = %s
    AND resource = %s
"""

DELETE = """
    DELETE FROM policy
    WHERE user_id = %s
    AND resource = %s
"""

DELETE_RESOURCE = """
    DELETE FROM policy
    WHERE resource = %s
"""

class Database:

    def __init__(self, dsn):
        self.connection = psycopg2.connect(dsn)
        self.connection.autocommit = True
        cursor = self.connection.cursor()
        cursor.execute(CREATE_TABLE)

    def add(self, user, resource, read=False, write=False):
        cursor = self.connection.cursor()
        cursor.execute(SELECT, (user, resource))
        result = cursor.fetchone()
        if result:
            self.update(user, resource, read, write)
        else:
            cursor.execute(INSERT, (user, resource, read, write,))


    def get_resource(self, resource):
        cursor = self.connection.cursor()
        cursor.execute(SELECT_RESOURCE, (resource,))
        result = cursor.fetchall()
        if not result:
            return {}
            
        policies = {}
        for row in result:
            policy = {
                'user': row[USER_INDEX],
                'read': row[READ_INDEX],
                'write': row[WRITE_INDEX],}
            policies[row[USER_INDEX]] = policy
        return policies

    def can_read(self, user, resource):
        cursor = self.connection.cursor()
        cursor.execute(SELECT, (user, resource,))
        result = cursor.fetchone()
        if not result:
            return False
        return result[READ_INDEX]

    def can_write(self, user, resource):
        cursor = self.connection.cursor()
        cursor.execute(SELECT, (user, resource,))
        result = cursor.fetchone()
        if not result:
            return False
        return result[WRITE_INDEX]

    def update(self, user, resource, read=False, write=False):
        cursor = self.connection.cursor()
        cursor.execute(UPDATE, (read, write, user, resource,))

    def delete(self, user, resource):
        cursor = self.connection.cursor()
        cursor.execute(DELETE, (user, resource,))

    def delete_resource(self, resource):
        cursor = self.connection.cursor()
        cursor.execute(DELETE_RESOURCE, (resource,))
