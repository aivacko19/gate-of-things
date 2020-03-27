import psycopg2
from psycopg2 import OperationalError
import os
from packet import Packet

CREATE_TABLE = """
    DO $$
    BEGIN
        IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'packet_type') THEN
            CREATE TYPE packet_type AS ENUM ('publish', 'subscribe', 'unsubscribe');
        END IF;
    END
    $$;
    CREATE TABLE IF NOT EXISTS packet_tracker (
        id SERIAL PRIMARY KEY,
        server_owned BOOLEAN DEFAULT FALSE,
        client_id INTEGER NOT NULL,
        packet_id INTEGER NOT NULL,
        type packet_type NOT NULL,
        UNIQUE (server_owned, client_id, packet_id)
    );
"""

DELETE = """
    DELETE FROM packet_tracker
    WHERE id = %s
"""

SELECT = """
    SELECT * FROM packet_tracker
    WHERE client_id = %s
    AND packet_id = %s
    AND server_owned = %s
"""

insert_keys = ['server_owned', 'client_id', 'packet_id', 'type']
insert_values = ", ".join(['%s'] * len(insert_keys))
INSERT = f"""
    INSERT INTO packet_tracker ({", ".join(insert_keys)})
    VALUES ({insert_values})
"""

class PacketDB:

    __instance = None

    @staticmethod
    def getInstance():
        if not PacketDB.__instance:
            DB_NAME = os.environ.get('DB_NAME', 'mydb')
            DB_USER = os.environ.get('DB_USER', 'root')
            DB_PASS = os.environ.get('DB_PASS', 'root')
            DB_HOST = os.environ.get('DB_HOST', '192.168.99.100')
            PacketDB.__instance = PacketDB(DB_NAME, DB_USER, DB_PASS, DB_HOST)

        return PacketDB.__instance

    def __init__(self, db_name, db_user, db_password, db_host):
        self.connection = psycopg2.connect(
            database=db_name,
            user=db_user,
            password=db_password,
            host=db_host,)
        self.connection.autocommit = True
        cursor = self.connection.cursor()
        cursor.execute(CREATE_TABLE)

    def delete(self, cid):
        cursor = self.connection.cursor()
        cursor.execute(DELETE, (cid,))

    def get(self, cid, pid, server_owned=False):
        cursor = self.connection.cursor()
        cursor.execute(SELECT, (cid, pid, server_owned))
        result = cursor.fetchone()
        if not result: return None
        return Packet(result)

    def add(self, packet):
        cursor = self.connection.cursor()
        cursor.execute(INSERT, packet.get_db_row())

    def add_client(self, cid, pid, ptype):
        cursor = self.connection.cursor()
        cursor.execute(INSERT, (False, cid, pid, ptype))


    def close(self):
        self.connection.close()
        
if __name__ == '__main__':
    DB_NAME = os.environ.get('DB_NAME', 'mydb')
    DB_USER = os.environ.get('DB_USER', 'root')
    DB_PASS = os.environ.get('DB_PASS', 'root')
    DB_HOST = os.environ.get('DB_HOST', '192.168.99.100')
    db = PacketDB(DB_NAME, DB_USER, DB_PASS, DB_HOST)

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