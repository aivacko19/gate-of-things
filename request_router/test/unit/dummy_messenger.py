replies = {
    'client_with_session': {'email': 'client@gmail.com'},
    'client_no_session': {'email': None}
}

class Messenger:

    def __init__(self):
        self.service = {}

    def publish(self, request, queue, correlation_id=None):
        if queue not in self.service: self.service[queue] = list()
        self.service[queue].append({'request': request, 'cid': correlation_id})

    def rpc(self, request, queue, correlation_id=None):
        return replies.get(correlation_id)

    def get(self, queue):
        service = self.service.get(queue)
        if not service: return None
        if len(service) == 0: return None
        request = service[0]
        service.remove(service[0])
        return request