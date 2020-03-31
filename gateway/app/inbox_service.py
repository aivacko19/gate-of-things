#!/usr/bin/env python3

import queue

import amqp_helper

class InboxService(amqp_helper.AmqpAgent):

    def __init__(self):
        self.inbox = queue.Queue()
        amqp_helper.AmqpAgent.__init__(self)
        
    def main(self, request, props):
        self.inbox.put((request ,props.correlation_id))

    def get(self):
        if self.inbox.empty():
            return None, None
        return self.inbox.get()
