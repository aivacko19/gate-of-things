
class Connection:
    def __init__(self, addr, sender, client_id, clean_start, will, 
                 will_delay, keep_alive, session_expiry, auth_data):
        self.addr = addr
        self.sender = sender
        self.id = client_id
        self.clean_start = clean_start
        self.will = will
        self.will_delay = will_delay
        self.keep_alive = keep_alive,
        self.session_expiry = session_expiry
        self.auth_data = auth_data
        self.authenticated = False

    def is_authenticated():
        return self.authenticated



