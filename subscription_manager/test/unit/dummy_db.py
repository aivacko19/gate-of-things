class Database:

    def __init__(self):
        self.db = {}
        self.packets = {}

    def delete(self, session):
        if not self.db.get(session.get_id()): return False
        del self.db[session.get_id()]
        return True

    def delete_sub(self, sub):
        if not self.db.get(sub.get_session_id()): return False
        session = self.db.get(sub.get_session_id())
        if not session.get('subs').get(sub.get_topic_filter()): return False
        del self.db[sub.get_session_id()]['subs'][sub.get_topic_filter()]
        return True

    def delete_sub_by_email_and_topic(self, email, topic):
        cid = None
        for key, session in self.db.items():
            if session.get('email') == email:
                cid = key
                break
        if not cid: return False
        session = self.db.get(cid)
        if not session.get('subs').get(topic): return False
        del self.db[cid]['subs'][topic]
        return True

    def delete_sub_by_id(self, sub_id):
        delete_cid, delete_topic = None, None
        for cid, session in self.db.items():
            for topic, sub in session.get('subs'):
                if sub.get('id') == sub_id:
                    delete_cid, delete_topic = cid, topic
                    break
            if delete_cid: break
        if not delete_cid: return False
        del self.db[delete_cid]['subs'][delete_topic]
        return True

    def get(self, cid):
        cursor = self.connection.cursor()
        LOGGER.info('Executing SELECT statement for table "session"')
        cursor.execute(SELECT, (cid,))
        result = cursor.fetchone()
        if not result: return None
        session = Session(result)
        cursor = self.connection.cursor()
        LOGGER.info('Executing SELECT statement for table "subscription')
        cursor.execute(SELECT_SUBS, (session.get_id(),))
        result = cursor.fetchall()
        for sub in result:
            session.add_sub(Subscription(sub))
        return session

    def get_sub(self, cid, topic_filter):
        cursor = self.connection.cursor()
        cursor.execute(SELECT_SUB, (cid, topic_filter))
        result = cursor.fetchone()
        if not result: return None
        return Subscription(result)

    def get_matching_subs(self, topic):
        cursor = self.connection.cursor()
        cursor.execute(SELECT_MATCHING_SUBS, (topic,))
        result = cursor.fetchall()
        subs = list()
        for sub in result:
            subs.append(Subscription(sub))
        return subs

    def get_subscribed_users(self, topic):
        cursor = self.connection.cursor()
        cursor.execute(SELECT_SUBSCRIBED_USERS, (topic,))
        result = cursor.fetchall()
        subs = list()
        for row in result:
            subs.append(row[0])

        return subs


    def add(self, cid, email):
        cursor = self.connection.cursor()
        cursor.execute(INSERT, (cid, email))

    def add_sub(self, sub):
        exists = self.get_sub(sub.get('session_id'), sub.get('filter'))
        if exists:
            return self.update_sub(sub)
        else:
            return self.insert_sub(sub)

    def insert_sub(self, sub):
        cursor = self.connection.cursor()
        cursor.execute(INSERT_SUB, (
            sub.get('session_id'),
            sub.get('filter'),
            sub.get('sub_id', 0),
            sub.get('max_qos', 2),
            sub.get('no_local', False),
            sub.get('retain_as_published', False),
            sub.get('retain_handling', 0),))
        result = cursor.fetchone()
        if not result:
            return 0

        return result[0]


    def update(self, session):
        cursor = self.connection.cursor()
        cursor.execute(UPDATE, (session.get_email(), session.get_id()))

    def update_sub(self, sub):
        cursor = self.connection.cursor()
        cursor.execute(UPDATE_SUB, (
            sub.get('sub_id', 0),
            sub.get('max_qos', 2),
            sub.get('no_local', False),
            sub.get('retain_as_published', False),
            sub.get('retain_handling', 0),
            sub.get('session_id'),
            sub.get('filter'),))
        
        result = cursor.fetchone()
        if not result:
            return 0

        return result[0]


    def get_packet_ids(self, session):
        cursor = self.connection.cursor()
        cursor.execute(SELECT_PACKET_IDS, (session.get_id(),))
        result = cursor.fetchall()
        ids = list()
        for packet_id in result:
            ids.append(packet_id[0])
        return ids

    def is_pid_in_use(self, session_id, packet_id):
        cursor = self.connection.cursor()
        cursor.execute(SELECT_PACKET_ID, (session_id, packet_id))
        return bool(cursor.fetchone())

    def add_packet_id(self, session_id, packet_id):
        cursor = self.connection.cursor()
        cursor.execute(INSERT_PACKET_ID, (session_id, packet_id))

    def delete_packet_id(self, session_id, packet_id):
        cursor = self.connection.cursor()
        cursor.execute(DELETE_PACKET_ID, (session_id, packet_id))

        return cursor.rowcount > 0
