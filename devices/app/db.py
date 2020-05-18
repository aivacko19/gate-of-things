import psycopg2
from psycopg2 import OperationalError
import os
import logging

LOGGER = logging.getLogger(__name__)

ID_INDEX = 0
NAME_INDEX = 1
OWNER_INDEX = 2
KEY_INDEX = 3

CREATE_TABLE = """
    CREATE TABLE IF NOT EXISTS device (
        id SERIAL PRIMARY KEY,
        name VARCHAR(40) UNIQUE,
        owner VARCHAR(40) NOT NULL,
        key VARCHAR(400)
    );
"""

insert_keys = ['name', 'owner', 'key']
insert_values = ", ".join(['%s'] * len(insert_keys))
INSERT = f"""
    INSERT INTO device ({", ".join(insert_keys)})
    VALUES ({insert_values}) RETURNING id
"""

SELECT = """
    SELECT * FROM device
    WHERE name = %s
"""

SELECT_OWNER = """
    SELECT * FROM device
    WHERE owner = %s
"""

UPDATE = """
    UPDATE device
    SET key = %s
    WHERE name = %s
"""

DELETE = """
    DELETE FROM device
    WHERE name = %s
"""

class DeviceDB:

    def __init__(self, db_name, db_user, db_password, db_host):
        self.connection = psycopg2.connect(
            database=db_name,
            user=db_user,
            password=db_password,
            host=db_host,)
        self.connection.autocommit = True
        cursor = self.connection.cursor()
        cursor.execute(CREATE_TABLE)

    def insert(self, name, owner, key):
        cursor = self.connection.cursor()
        cursor.execute(INSERT, (name, owner, key))
        result = cursor.fetchone()
        if result is None:
            return None
        
        return result[0]

    def select(self, name):
        cursor = self.connection.cursor()
        cursor.execute(SELECT, (name,))
        result = cursor.fetchone()
        if result is None:
            return None

        device = {
            'id': result[ID_INDEX],
            'name': result[NAME_INDEX],
            'owner': result[OWNER_INDEX],
            'key': result[KEY_INDEX],
        }

        return device

    def select_by_owner(self, owner):
        cursor = self.connection.cursor()
        cursor.execute(SELECT_OWNER, (owner,))
        result = cursor.fetchall()

        if not result:
            return {}

        devices = {}
        for row in result:
            device = {
                'id': row[ID_INDEX],
                'name': row[NAME_INDEX],
                'owner': row[OWNER_INDEX],
                'key': row[KEY_INDEX],
            }
            devices[device['name']] = device

        return devices

    def update(self, name, key):
        cursor = self.connection.cursor()
        cursor.execute(UPDATE, (key, name,))
        
        return cursor.rowcount

    def delete(self, name):
        cursor = self.connection.cursor()
        cursor.execute(DELETE, (name,))

        return cursor.rowcount