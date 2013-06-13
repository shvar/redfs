#!/usr/bin/python
from uuid import UUID

from common.abstractions import AbstractApp
from common.inhabitants import Node
from common.utils import coalesce


class TestNodeApp(AbstractApp):
    __slots__ = ('__handle_transaction_before_create',
                 '__handle_transaction_after_create',
                 '__handle_transaction_destroy')

    ports_map = {12345: Node(UUID('951d0302-90e2-4ee4-a140-1615b2154da8'))}

    def __init__(self, handle_transaction_before_create=None,
                       handle_transaction_after_create=None,
                       handle_transaction_destroy=None):
        self.__handle_transaction_before_create = \
            coalesce(handle_transaction_before_create, lambda tr, state: None)
        self.__handle_transaction_after_create = \
            coalesce(handle_transaction_after_create, lambda tr, state: None)
        self.__handle_transaction_destroy = \
            coalesce(handle_transaction_destroy, lambda tr, state: None)


    def handle_transaction_before_create(self, tr, state):
        return self.__handle_transaction_before_create(tr, state)


    def handle_transaction_after_create(self, tr, state):
        return self.__handle_transaction_after_create(tr, state)


    def handle_transaction_destroy(self, tr, state):
        return self.__handle_transaction_destroy(tr, state)
