
import abstract_service

class Service(abstract_service.AbstractService):

    def __init__(self, queue, db):
        self.db = db
        abstract_service.AbstractService.__init__(self, queue)
        self.actions = {
            'verify': self.verify,}

    # Receive client verification from OAuth service
    def verify(self, request, props):
        cid = props.correlation_id
        email = request.get('email')
        temp_id = int(cid)
        if email:
            self.db.set_username(temp_id, email)
        else:
            self.db.set_failed(temp_id)
