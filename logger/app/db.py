import psycopg2
from psycopg2 import OperationalError
import os
import logging

LOGGER = logging.getLogger(__name__)

ID_INDEX = 0
USER_INDEX = 1
RESOURCE_INDEX = 2
ACTION_INDEX = 3
SUCCESS_INDEX = 4
ACCESS_TIME_INDEX = 5

CREATE_TABLE = """
    CREATE TABLE IF NOT EXISTS audit_log (
        id SERIAL PRIMARY KEY,
        client VARCHAR(40),
        resource VARCHAR(40),
        action VARCHAR(40),
        success VARCHAR(40),
        access_time TIMESTAMP
    );

    CREATE TABLE IF NOT EXISTS ownership (
        resource VARCHAR(40),
        owner VARCHAR(40),
        PRIMARY KEY (resource, owner)
    );

    CREATE OR REPLACE FUNCTION get_audit_logs (m_resource varchar(40), m_limit integer, m_offset integer) 
        RETURNS TABLE (
            m_client VARCHAR(40),
            m_action VARCHAR(40),
            m_success VARCHAR(40),
            m_access_time TIMESTAMP
    ) 
    AS $$
    BEGIN
        RETURN QUERY SELECT
            client,
            action,
            success,
            access_time
        FROM
            audit_log, ownership
        WHERE
            audit_log.resource = m_resource
        AND
            audit_log.resource = ownership.resource
        AND 
            user = ownership.owner
        ORDER BY access_time DESC
        LIMIT m_limit
        OFFSET m_offset;
    END; $$ 

    LANGUAGE 'plpgsql';
"""

insert_keys = ['client', 'resource', 'action', 'success']
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

insert_keys = ['owner', 'resource']
insert_values = ", ".join(['%s'] * len(insert_keys))
INSERT_OWNERSHIP = f"""
    INSERT INTO ownership ({", ".join(insert_keys)})
    VALUES ({insert_values})
"""

SELECT_OWNERSHIP = """
    SELECT * FROM ownership
    WHERE owner = %s
"""

DELETE_OWNERSHIP = """
    DELETE FROM ownership
    WHERE owner = %s
    AND resource = %s
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

GRANT_SELECT_OWNERSHIP = """
    GRANT SELECT ON ownership TO %s;
"""

DROP_USER = """
    DROP USER %s;
"""

REVOKE_EXECUTE = """
    REVOKE EXECUTE ON FUNCTION get_audit_logs FROM %s;
"""

REVOKE_SELECT = """
    REVOKE SELECT ON audit_log FROM %s;
"""

REVOKE_SELECT_OWNERSHIP = """
    REVOKE SELECT ON ownership FROM %s;
"""

class Database:

    def __init__(self, dsn):
        self.connection = psycopg2.connect(dsn)
        self.connection.autocommit = True
        cursor = self.connection.cursor()
        cursor.execute(CREATE_TABLE)

    def insert(self, user, resource, action, success,):
        # owner = owner.replace('@', '_at_').replace('.', '_dot_')
        # cursor = self.connection.cursor()
        # cursor.execute(SELECT_OWNER, (owner,))
        # LOGGER.info(cursor.fetchall())
        cursor = self.connection.cursor()
        cursor.execute(INSERT, (user, resource, action, success,))
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
                'success': row[SUCCESS_INDEX],
                'access_time': str(row[ACCESS_TIME_INDEX]),
            }

            logs.append(log)

        return logs

    def add_ownership(self, owner, resource):
        owner = owner.replace('@', '_at_').replace('.', '_dot_')
        cursor = self.connection.cursor()
        cursor.execute(SELECT_OWNERSHIP, (owner,))
        result = cursor.fetchall()
        if not result:
            # cursor = self.connection.cursor()
            # cursor.execute(REVOKE_EXECUTE % owner)
            # cursor = self.connection.cursor()
            # cursor.execute(REVOKE_SELECT_OWNERSHIP % owner)
            # cursor = self.connection.cursor()
            # cursor.execute(REVOKE_SELECT % owner)
            # cursor = self.connection.cursor()
            # cursor.execute(DROP_USER % owner)
            cursor = self.connection.cursor()
            cursor.execute(CREATE_USER % owner)
            cursor = self.connection.cursor()
            cursor.execute(GRANT_SELECT % owner)
            cursor = self.connection.cursor()
            cursor.execute(GRANT_SELECT_OWNERSHIP % owner)
            cursor = self.connection.cursor()
            cursor.execute(GRANT_EXECUTE % owner)

        cursor = self.connection.cursor()
        cursor.execute(INSERT_OWNERSHIP, (owner, resource,))

    def remove_ownership(self, owner, resource):
        owner = owner.replace('@', '_at_').replace('.', '_dot_')
        cursor = self.connection.cursor()
        cursor.execute(DELETE_OWNERSHIP, (owner, resource,))
        rows_affected = cursor.rowcount

        cursor = self.connection.cursor()
        cursor.execute(SELECT_OWNERSHIP, (owner,))
        result = cursor.fetchall()
        if not result:
            cursor = self.connection.cursor()
            cursor.execute(REVOKE_EXECUTE % owner)
            cursor = self.connection.cursor()
            cursor.execute(REVOKE_SELECT_OWNERSHIP % owner)
            cursor = self.connection.cursor()
            cursor.execute(REVOKE_SELECT % owner)
            cursor = self.connection.cursor()
            cursor.execute(DROP_USER % owner)

        return rows_affected

    def grant(self, owner):
        owner = owner.replace('@', '_at_').replace('.', '_dot_')
        cursor = self.connection.cursor()
        cursor.execute(CREATE_USER % owner)
        cursor = self.connection.cursor()
        cursor.execute(GRANT_SELECT % owner)
        cursor = self.connection.cursor()
        cursor.execute(GRANT_EXECUTE % owner)