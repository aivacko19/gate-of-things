import psycopg2
from psycopg2 import OperationalError
import os
import logging

LOGGER = logging.getLogger(__name__)

ID_INDEX = 0
USER_INDEX = 1
RESOURCE_INDEX = 2
ACTION_INDEX = 3
OWNER_INDEX = 4
ACCESS_TIME_INDEX = 5

CREATE_TABLE = """
    CREATE TABLE IF NOT EXISTS audit_log (
        id SERIAL PRIMARY KEY,
        client VARCHAR(40),
        resource VARCHAR(40),
        action VARCHAR(40),
        owner VARCHAR(40),
        access_time TIMESTAMP
    );
    CREATE OR REPLACE FUNCTION get_audit_logs (m_resource varchar(40), m_limit integer, m_offset integer) 
        RETURNS TABLE (
            m_client VARCHAR(40),
            m_action VARCHAR(40),
            m_access_time TIMESTAMP
    ) 
    AS $$
    BEGIN
        RETURN QUERY SELECT
            client,
            action,
            access_time
        FROM
            audit_log
        WHERE
            resource = m_resource
        AND 
            user = owner
        ORDER BY access_time DESC
        LIMIT m_limit
        OFFSET m_offset;
    END; $$ 

    LANGUAGE 'plpgsql';
"""

insert_keys = ['client', 'resource', 'action', 'owner']
insert_values = ", ".join(['%s'] * len(insert_keys))
INSERT = f"""
    INSERT INTO audit_log ({", ".join(insert_keys)}, access_time)
    VALUES ({insert_values}, CURRENT_TIMESTAMP)
    RETURNING id
"""

SELECT = """
    SELECT * FROM audit_log
    WHERE resource = %s
"""

SELECT_OWNER = """
    SELECT * FROM audit_log
    WHERE owner = %s
"""

CREATE_USER = """
    CREATE USER %s WITH PASSWORD '123';
"""

GRANT_EXECUTE = """
    GRANT EXECUTE ON FUNCTION get_audit_logs TO %s;
"""

GRANT_SELECT = """
    GRANT SELECT ON audit_log TO %s;
"""

class Database:

    def __init__(self, dsn):
        self.connection = psycopg2.connect(dsn)
        self.connection.autocommit = True
        cursor = self.connection.cursor()
        cursor.execute(CREATE_TABLE)

    def insert(self, user, resource, action, owner):
        owner = owner.replace('@', '_at_').replace('.', '_dot_')
        # cursor = self.connection.cursor()
        # cursor.execute(SELECT_OWNER, (owner,))
        # LOGGER.info(cursor.fetchall())
        cursor = self.connection.cursor()
        cursor.execute(INSERT, (user, resource, action, owner,))
        result = cursor.fetchone()
        if result is None:
            return None
        
        return result[0]

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
                'owner': row[OWNER_INDEX],
                'access_time': str(row[ACCESS_TIME_INDEX]),
            }

            logs.append(log)

        return logs

    def grant(self, owner):
        owner = owner.replace('@', '_at_').replace('.', '_dot_')
        cursor = self.connection.cursor()
        cursor.execute(CREATE_USER % owner)
        cursor = self.connection.cursor()
        cursor.execute(GRANT_SELECT % owner)
        cursor = self.connection.cursor()
        cursor.execute(GRANT_EXECUTE % owner)