#!/usr/bin/python
"""A memory-based implementation of Transaction Manager."""

#
# Imports
#

from __future__ import absolute_import
import bisect
import collections as col
import logging
import random
from collections import defaultdict, deque
from operator import attrgetter
from threading import RLock
from types import NoneType

from twisted.internet import defer

from contrib.dbc import contract_epydoc

from common.utils import coalesce, in_main_thread

from .abstractions import (
    AbstractApp, AbstractInhabitant, AbstractTransaction, AbstractMessage)
from .abstract_transaction_manager import (
    AbstractTransactionManager, TransactionProcessingException)
from .typed_uuids import MessageUUID, PeerUUID, TransactionUUID



#
# Constants
#

logger = logging.getLogger(__name__)



#
# Classes
#

class TransactionManagerInMemory(AbstractTransactionManager):
    """
    The central class for controlling all the running transactions and
    their messages, storing the data in memory.

    @ivar __outgoing_messages: The messages which should be sent to each
        possible peer, grouped by the peer itself.
        Note that for each of the peers, the messages are ordered.
        Also note that it is not a defaultdict for a reason:
        if some host is present in the dict, it means there are indeed messages
        for it (i.e. its messages list is not empty).
        Example: {'<Node ABC>': ['<Message 1 to ABC>', '<Message 2 to ABC>',]}

    @ivar __outgoing_message_notifs_by_host_uuid:
        The deferred objects which should be called
        whenever the corresponding message for such host arrives.

    @todo: __outgoing_messages_lock should not be a transaction-manager-wise
           but separate for each of the destination peer.

    @ivar __transactions_by_uuid: all running transactions,
                                  grouped by their UUIDs.
    @type __transactions_by_uuid: dict

    @ivar __states_by_uuid: the payloads for all running transactions,
                            grouped by the transaction UUIDs.
    @type __states_by_uuid: dict

    @ivar __transactions_by_end_time: all running transactions,
                                      ordered by their (expected) end time.
    @type __transactions_by_end_time: list

    @ivar __states_by_class_name: states of all running transactions,
        grouped by their class (into the dicts, which map the transaction UUIDS
        to the states).
    @type __states_by_class_name: defaultdict

    @invariant: len(self.__transactions_by_uuid) == \
                len(sef.__states_by_uuid) == \
                len(self.__transactions_by_end_time) == \
                sum(len(per_class)
                        for per_class in __states_by_class_name.itervalues())
    """

    __slots__ = ('__outgoing_messages',
                 '__outgoing_messages_by_uuid',
                 '__outgoing_messages_by_class_name',
                 '__outgoing_message_notifs_by_host_uuid',
                 '__outgoing_messages_lock',
                 '__transactions_by_uuid', '__transactions_by_end_time',
                 '__states_by_uuid', '__states_lock',
                 '__states_by_class_name', '__transactions_lock')


    def __assert_invariant(self):
        if __debug__:
            with self.__transactions_lock:  # TODO: use ReadLock when possible
                (l1, l2, l3, l4) = \
                    (len(self.__transactions_by_uuid),
                     len(self.__transactions_by_end_time),
                     len(self.__states_by_uuid),
                     sum(len(per_class)
                             for per_class
                                 in self.__states_by_class_name.itervalues()))
            assert l1 == l2 == l3 == l4, (l1, l2, l3, l4)
        return True


    @contract_epydoc
    def __init__(self, *args, **kwargs):
        """Constructor."""
        super(TransactionManagerInMemory, self).__init__(*args, **kwargs)

        self.__outgoing_messages = {}
        self.__outgoing_messages_by_uuid = {}
        self.__outgoing_messages_by_class_name = defaultdict(set)
        self.__outgoing_message_notifs_by_host_uuid = defaultdict(deque)
        self.__outgoing_messages_lock = RLock()

        # Dict of transactions
        self.__transactions_by_uuid = {}
        # List of (end_time, transaction) tuples;
        # used to access transactions in their oldest-to-youngest order
        self.__transactions_by_end_time = []
        self.__states_by_uuid = {}
        self.__states_lock = RLock()
        self.__states_by_class_name = defaultdict(dict)
        # The transactions may be deleted recursively
        self.__transactions_lock = RLock()


    @contract_epydoc
    def get_tr_states(self, class_name=None, dst_uuid=None):
        """Implementation of interface from C{AbstractTransactionManager}.

        @type class_name: basestring, NoneType
        @type dst_uuid: PeerUUID, NoneType

        @rtype: col.Iterable
        """
        # Currently, it makes no sense to support queries on
        # transaction_manager_in_memory without some concrete class_name.
        assert class_name is not None, class_name
        with self.__states_lock:
            if class_name is not None:
                assert class_name in self._msg_class_by_msg_name, \
                       repr(class_name)

                states_by_this_class_name = \
                    self.__states_by_class_name[class_name]

                # For safety, return the copy of the storage,
                # so that external modification will not corrupt the storage.
                if dst_uuid is None:
                    return states_by_this_class_name.values()
                else:
                    # Extra filter by dst_uuid
                    return [state
                                for tr_uuid, state
                                    in states_by_this_class_name.iteritems()
                                if self.__transactions_by_uuid[tr_uuid]
                                       .message.dst.uuid == dst_uuid]


    @contract_epydoc
    def get_outgoing_msg_uuids(self, class_name=None, dst_uuid=None):
        """Implementation of interface from C{AbstractTransactionManager}.

        @type class_name: basestring, NoneType
        @type dst_uuid: PeerUUID, NoneType

        @rtype: col.Iterable
        """
        # Currently, it makes no sense to support queries on
        # transaction_manager_in_memory without some concrete class_name.
        assert class_name is not None, class_name
        with self.__states_lock:
            if class_name is not None:
                assert class_name in self._msg_class_by_msg_name, \
                       repr(class_name)

                msgs_by_this_class_name = \
                    self.__outgoing_messages_by_class_name[class_name]

                # For safety, return the copy of the storage,
                # so that external modification will not corrupt the storage.
                if dst_uuid is None:
                    return [msg.uuid
                                for msg in msgs_by_this_class_name]
                else:
                    # Extra filter by dst_uuid
                    return [msg.uuid
                                for msg in msgs_by_this_class_name
                                if msg.dst.uuid == dst_uuid]


    @contract_epydoc
    def _get_state_by_uuid(self, tr_uuid):
        """Implementation of interface from C{AbstractTransactionManager}.

        @type tr_uuid: TransactionUUID

        @rtype: AbstractTransaction.State

        @raises KeyError: if the transaction state for such transaction
            does not exist.
        """
        return self.__states_by_uuid[tr_uuid]


    @contract_epydoc
    def _set_state_by_uuid(self, tr_uuid, state):
        """Implementation of interface from C{AbstractTransactionManager}.

        @type tr_uuid: TransactionUUID

        @type state: AbstractTransaction.State

        @raises KeyError: if the transaction state for such transaction
            does not exist.
        """
        with self.__states_lock:
            if tr_uuid not in self.__states_by_uuid:
                raise KeyError(u'{!r} not in {!r}'
                                   .format(tr_uuid, self.__states_by_uuid))
            else:
                self.__states_by_uuid[tr_uuid] = state


    @contract_epydoc
    def __contains__(self, tr):
        """Implementation of interface from C{AbstractTransactionManager}.

        @type tr: AbstractTransaction
        @rtype: bool
        """
        return tr.uuid in self.__transactions_by_uuid


    @contract_epydoc
    def get_tr_by_uuid(self, tr_uuid):
        """Implementation of interface from C{AbstractTransactionManager}.

        @rtype: AbstractTransaction, NoneType
        """
        return self.__transactions_by_uuid.get(tr_uuid)


    @contract_epydoc
    def post_message(self, message):
        """Implementation of interface from C{AbstractTransactionManager}.

        @type message: AbstractMessage
        """
        super(TransactionManagerInMemory, self).post_message(message)

        # At some point, we may want to callback the deferred
        # if it is waiting for the message.
        # But we do NOT want to call it from inside the lock!
        # So, if this deferred is not None after the lock is released,
        # it should be called then.
        d = None

        with self.__outgoing_messages_lock:
            # Do we indeed have to put the message in the queue?
            # Maybe we have a waiting Deferred object to send it directly?
            notifs_for_host = \
                self.__outgoing_message_notifs_by_host_uuid[message.dst.uuid]

            if notifs_for_host:
                # Yes, we have such an object. Don't put the message
                # in the queue then!
                d = notifs_for_host.popleft()
                # The deferred will be called after leaving the lock.
            else:
                # No, we have to put it to the queue.
                _messages = self.__outgoing_messages.setdefault(message.dst,
                                                                [])
                assert not _messages or _messages == sorted(_messages), \
                       repr(_messages)
                # The messages are sorted using the implicit ordering
                # relationship defined in AbstractMessage.
                bisect.insort(_messages, message)
                self.__outgoing_messages_by_uuid[message.uuid] = message
                self.__outgoing_messages_by_class_name[message.name] \
                    .add(message)

        # "d" now contains a deferred waiting for a message exactly like
        # we have. So let's give it one!
        if d is not None:
            d.callback(message)


    @contract_epydoc
    def wait_for_message_for_peer(self,
                                  inh, prefer_msg_uuid, still_wait_checker,
                                  d=None):
        """Implementation of interface from C{AbstractTransactionManager}.

        @type inh: AbstractInhabitant
        @type prefer_msg_uuid: MessageUUID, NoneType
        @type still_wait_checker: col.Callable
        @type d: defer.Deferred, NoneType
        @rtype: defer.Deferred

        @todo: C{still_wait_checker} is not used; instead, the user should
            probably delete the C{Deferred} from
            C{__outgoing_message_notifs_by_host_uuid}.
        """
        assert not in_main_thread()

        d = defer.Deferred() if d is None else d

        with self.__outgoing_messages_lock:
            # Do we have a message with the preferred UUID?
            candidate_msg = \
                self.__outgoing_messages_by_uuid.get(prefer_msg_uuid, None)

            # Do we have a message for a particular peer?
            reply_msg = self.deliver_message_for_peer(inh, prefer_msg_uuid)

            # What if we would try to deliver a message with a particular
            # UUID? Too lazy to implement it now (cause it's not needed at the
            # moment), but... could it help us?
            #
            if reply_msg is not None:
                # For now, if we have a reply message for some particular peer,
                # this takes precedence over the reply message
                # we could send to.
                # This might be wrong though.
                # Note that coalesce() may return a msg,
                # and reply_msg may be None, so we'd better compare them
                # only if reply_msg is definitely not None.
                if coalesce(candidate_msg, reply_msg) != reply_msg:
                    logger.warning("Could've deliver %r, but using %r instead",
                                   candidate_msg, reply_msg)

                # We have an outgoing message for inh already!
                d.callback(reply_msg)
            else:
                # Unfortunately, we don't have a message yet.
                # We have to put a deferred callback to the queue
                # for this peer.
                # Whenever a message directed to this host is added,
                # the message adder will call this callback.
                self.__outgoing_message_notifs_by_host_uuid[inh.uuid] \
                    .append(d)

        return d


    @contract_epydoc
    def deliver_message_for_peer(self, inh, prefer_msg_uuid):
        """Implementation of interface from C{AbstractTransactionManager}.

        @type inh: AbstractInhabitant
        @type prefer_msg_uuid: MessageUUID, NoneType
        @rtype: AbstractMessage, NoneType
        """
        with self.__outgoing_messages_lock:
            if inh in self.__outgoing_messages:
                waiting_messages = self.__outgoing_messages[inh]
                assert (waiting_messages and
                        isinstance(waiting_messages, list)), \
                       repr(waiting_messages)

                next_message = waiting_messages.pop(0)
                logger.debug('Found %r', next_message)

                # If the list is empty, then clean the mapping
                # for not to waste memory with the empty lists,
                # and also to simplify finding hosts which DO have messages.
                if not waiting_messages:
                    del self.__outgoing_messages[inh]
                    del self.__outgoing_messages_by_uuid[next_message.uuid]
                    self.__outgoing_messages_by_class_name[next_message.name] \
                        .remove(next_message)

                return next_message

        return None


    @contract_epydoc
    def deliver_any_message(self):
        """Implementation of interface from C{AbstractTransactionManager}.

        @rtype: AbstractMessage, NoneType
        """
        with self.__outgoing_messages_lock:
            if self.__outgoing_messages:
                if __debug__:
                    logger.debug(
                        'Do we have any messages at all? Testing against %s',
                        [u'{!r}: {!r}'.format(h, msg_per_host)
                             for h, msg_per_host
                                 in self.__outgoing_messages.iteritems()])

                # Select only the peers which have URLs declared.
                # If a peer doesn't have an URL, it cannot be connected to;
                # but in fact, it still can be used via the node (passthrough).
                peers = self.__outgoing_messages.keys()
                if peers:
                    peer = random.choice(peers)
                    logger.debug('Yes, delivering for %r', peer)
                    return self.deliver_message_for_peer(peer,
                                                         prefer_msg_uuid=None)
                else:
                    logger.debug('No, unfortunately.')

        return None


    @contract_epydoc
    def _add_transaction(self, tr, state):
        """Add the transaction to the transaction storage, thread-safely.

        @type tr: AbstractTransaction

        @type state: AbstractTransaction.State
        """
        logger.debug('__add %r', tr)

        with self.__transactions_lock:
            assert tr.uuid not in self.__transactions_by_uuid, \
                   (tr, self.__transactions_by_uuid)
            assert tr.uuid not in self.__states_by_uuid, \
                   (tr, self.__states_by_uuid)

            # 1. By UUID
            (self.__transactions_by_uuid[tr.uuid],
             self.__states_by_uuid[tr.uuid]) = (tr,
                                                state)

            # 2. By end time
            assert self.__transactions_by_end_time == \
                       sorted(self.__transactions_by_end_time), \
                   repr(self.__transactions_by_end_time)
            bisect.insort(self.__transactions_by_end_time,
                          (tr.max_end_time(), tr))

            # 3. By class
            self.__states_by_class_name[tr.msg_class.name][tr.uuid] = state

        self.__assert_invariant()


    @contract_epydoc
    def _del_transaction(self, tr):
        """Delete a transaction from the transaction storage, thread-safely.

        @type tr: AbstractTransaction
        """
        logger.debug('__del %r', tr)

        with self.__transactions_lock:
            assert tr.uuid in self.__transactions_by_uuid, \
                   (tr, self.__transactions_by_uuid)
            assert tr.uuid in self.__states_by_uuid, \
                   (tr, self.__states_by_uuid)
            assert (tr.max_end_time(), tr) in self.__transactions_by_end_time, \
                   (tr.max_end_time(), tr)

            # 1. By UUID
            del self.__transactions_by_uuid[tr.uuid]
            del self.__states_by_uuid[tr.uuid]

            # 2. By end time
            del_item = (tr.max_end_time(), tr)

            # The item should not be stored twice
            if __debug__:
                _bl = bisect.bisect_left(self.__transactions_by_end_time,
                                         del_item)
                _br = bisect.bisect_right(self.__transactions_by_end_time,
                                          del_item) - 1
                assert _bl == _br, \
                       (_bl, _br, del_item, self.__transactions_by_end_time)

            position = bisect.bisect_left(self.__transactions_by_end_time,
                                          del_item)
            assert self.__transactions_by_end_time[position] == del_item, \
                   repr(del_item)

            try:
                del self.__transactions_by_end_time[position]
            except IndexError:
                # TODO: But why? Maybe, it was removed in the same thread,
                # during switching away via Twisted, so that the threading lock
                # doesn't help (as it is a Twisted "green thread", rather than
                # a threading thread)?
                logger.warning('Failed to fast delete (%r, %r) at %r: %r',
                               tr.max_end_time(),
                               tr,
                               position,
                               self.__transactions_by_end_time)
                try:
                    self.__transactions_by_end_time.remove(del_item)
                except ValueError:
                    logger.warning('Failed to slow delete (%r, %r) at %r: %r',
                                   tr.max_end_time(),
                                   tr,
                                   position,
                                   self.__transactions_by_end_time)

            # 3. By class
            assert tr.uuid in self.__states_by_class_name[tr.msg_class.name], \
                   (tr, self.__states_by_class_name[tr.msg_class.name])
            del self.__states_by_class_name[tr.msg_class.name][tr.uuid]

            # Also:
            # All children in progress, unfortunately, should be deleted too
            children_copy = set(tr.children_in_progress)
            map(self._del_transaction, children_copy)

        self.__assert_invariant()


    @contract_epydoc
    def destroy_transaction_unquestionnably(self, transaction):
        """Implementation of interface from C{AbstractTransactionManager}.

        @type transaction: AbstractTransaction
        """
        if transaction.children_in_progress:
            children_copy = set(transaction.children_in_progress)
            logger.debug('For transaction %r the following children '
                             'still exist: %r and we are killing them too.',
                         transaction, children_copy)
            map(self.destroy_transaction_unquestionnably, children_copy)

        transaction._waiting = False
        logger.debug('Destroying transaction %r', transaction)
        self._destroy_transaction(transaction)

        if transaction.parent is not None:
            self.try_destroy_transaction(transaction.parent)


    @contract_epydoc
    def handle_incoming_message(self, _message):
        """
        All the transaction handling operations
        invoked when the message is being received.

        @param _message: Message being received.
        @type _message: AbstractMessage
        """
        # What shall we do with this message?
        # If this is an incoming message, it starts a new
        # transaction.from_uuid.
        # If this is an incoming ack, it completes some existing transaction.

        if not _message.is_ack:
            # This is a new inbound transaction

            logger.debug('Handling non-ack, creating transaction')
            transaction = self._create_transaction_for_message(_message)

        else:
            # We received a transaction-ack
            message_ack = _message

            if message_ack.uuid not in self.__transactions_by_uuid:
                raise TransactionProcessingException(
                          u'Received {!r} as an ACK, but it is not present '
                          u'in the transaction storage: {!r}'
                              .format(message_ack,
                                      self.__transactions_by_uuid))

            logger.debug('Handling ack...')
            # This is an ACK to some existing transaction
            transaction_being_responded = \
                self.__transactions_by_uuid[message_ack.uuid]

            assert transaction_being_responded._waiting is True, \
                   repr(transaction_being_responded)
            transaction_being_responded._waiting = False

            transaction_being_responded.message_ack = message_ack

            logger.debug('Handling ack, destroying %r',
                         transaction_being_responded)
            self.try_destroy_transaction(transaction_being_responded)
