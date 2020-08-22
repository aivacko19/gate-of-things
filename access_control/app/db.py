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
        own BOOLEAN DEFAULT FALSE,
        access_time INT DEFAULT 0
        UNIQUE (user_id, resource)
    );
"""

ID_INDEX = 0
USER_INDEX = 1
RESOURCE_INDEX = 2
READ_INDEX = 3
WRITE_INDEX = 4
OWN_INDEX = 5
ACCESS_TIME_INDEX = 6

insert_keys = ['user_id', 'resource', 'read', 'write', 'own', 'access_time']
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

SELECT_OWNED = """
    SELECT * FROM policy
    WHERE user = %s
    AND own = TRUE
"""

UPDATE = """
    UPDATE policy
    SET read = %s
    AND write = %s
    AND own = %s
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

    def add(self, user, resource, read=False, write=False, own=False, access_time=0):
        cursor = self.connection.cursor()
        cursor.execute(SELECT, (user, resource))
        result = cursor.fetchone()

        if result:
            return self.update(user, resource, read, write, own)
        
        cursor.execute(INSERT, (user, resource, read, write, own, access_time))
        return cursor.rowcount > 0


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
                'write': row[WRITE_INDEX],
                'own': row[OWN_INDEX],
                'access_time': row[ACCESS_TIME_INDEX]}
            policies[row[USER_INDEX]] = policy
        return policies

    def can_read(self, user, resource):
        cursor = self.connection.cursor()
        cursor.execute(SELECT, (user, resource,))
        result = cursor.fetchone()
        if not result:
            return False
        return result[READ_INDEX], result[ACCESS_TIME_INDEX]

    def can_write(self, user, resource):
        cursor = self.connection.cursor()
        cursor.execute(SELECT, (user, resource,))
        result = cursor.fetchone()
        if not result:
            return False
        return result[WRITE_INDEX]

    def owns(self, user, resource):
        cursor = self.connection.cursor()
        cursor.execute(SELECT, (user, resource,))
        result = cursor.fetchone()
        if not result:
            return False
        return result[OWN_INDEX]

    def update(self, user, resource, read=False, write=False, own=False):
        cursor = self.connection.cursor()
        cursor.execute(UPDATE, (read, write, own, user, resource,))

        return cursor.rowcount > 0

    def delete(self, user, resource):
        cursor = self.connection.cursor()
        cursor.execute(DELETE, (user, resource,))

        return cursor.rowcount > 0

    def delete_resource(self, resource):
        cursor = self.connection.cursor()
        cursor.execute(DELETE_RESOURCE, (resource,))

    def get_owned_resources(self, user):
        cursor = self.connection.cursor()
        cursor.execute(SELECT_OWNED, (user,))
        result = cursor.fetchall()
        resources = []
        for row in result:
            resources.append(row[RESOURCE_INDEX])
        return resources
