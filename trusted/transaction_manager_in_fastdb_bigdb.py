#!/usr/bin/python
"""
A FastDB-based (and using BigDB for protocol messages) implementation
of Transaction Manager.
"""

#
# Imports
#

from __future__ import absolute_import
import collections as col
import logging
from collections import defaultdict
from datetime import timedelta
from threading import RLock
from types import NoneType

from twisted.internet import defer, error, reactor, task, threads

from contrib.dbc import contract_epydoc

from common.abstractions import (
    AbstractApp, AbstractInhabitant, AbstractTransaction, AbstractMessage)
from common.abstract_transaction_manager import (
    AbstractTransactionManager, TransactionProcessingException)
from common.typed_uuids import MessageUUID, PeerUUID, TransactionUUID
from common.utils import exceptions_logged, in_main_thread
from common.twisted_utils import callFromThread, callInThread

from trusted.docstore.fdbqueries import FDBQueries
from trusted.docstore.bdbqueries import BDBQueries



#
# Constants
#

logger = logging.getLogger(__name__)

POLL_FOR_OUTGOING_MESSAGES_PERIOD = timedelta(seconds=1)

_TMP_NOTHING_IN_REACTOR_THREAD = True
"""
Temporary variable to enable extra checks that nothing is called from the
reactor thread.
When enabled, some extra asserts are added. I'm still not sure that
all of them are already satisfied by all code branches, so this might be
dangerous and needs empirical proof.

@todo: remove when everything is fixed!
"""



#
# Classes
#

class TransactionManagerInFastDBBigDB(AbstractTransactionManager):
    """
    The central class for controlling all the running transactions and
    their messages, storing the data in FastDB (and also messages in BigDB).
    """

    __slots__ = ('__fdbw_factory', '__bdbw_factory',
                 '__transactions_by_uuid', '__transactions_lock')


    def __init__(self, fdbw_factory, bdbw_factory, *args, **kwargs):
        """Constructor.

        @type fdbw_factory: DocStoreWrapperFactory
        @type bdbw_factory: DocStoreWrapperFactory
        """
        super(TransactionManagerInFastDBBigDB, self).__init__(*args, **kwargs)

        self.__fdbw_factory = fdbw_factory
        self.__bdbw_factory = bdbw_factory

        # Dict of transactions
        self.__transactions_by_uuid = {}

        # The transactions may be deleted recursively
        self.__transactions_lock = RLock()

        # TODO: ticket:151
        logger.debug('Wiping the transactions storage; '
                     'TODO: stop it after stateless transactions are '
                     'fully supported!')

        for node in self.app.ports_map.itervalues():
            logger.debug('Deleting transactions from %s', node.uuid)
            with self.__fdbw_factory() as fdbw:
                FDBQueries.Transactions.del_all_transactions_from_somebody(
                    node.uuid, fdbw)


    @contract_epydoc
    def get_tr_states(self, class_name=None, dst_uuid=None):
        """Implementation of interface from C{AbstractTransactionManager}.

        @type class_name: basestring, NoneType
        @type dst_uuid: PeerUUID, NoneType

        @rtype: col.Iterable
        """
        assert not _TMP_NOTHING_IN_REACTOR_THREAD or not in_main_thread()

        if class_name is not None:
            assert class_name in self._msg_class_by_msg_name, \
                   repr(class_name)

            with self.__fdbw_factory() as fdbw:
                # Return a copy of the storage, to free the FastDB wrapper.
                return list(FDBQueries.Transactions.get_transaction_states(
                                tr_type=class_name,
                                dst_uuid=dst_uuid,
                                fdbw=fdbw))


    @contract_epydoc
    def get_outgoing_msg_uuids(self, class_name=None, dst_uuid=None):
        """Implementation of interface from C{AbstractTransactionManager}.

        @type class_name: basestring, NoneType
        @type dst_uuid: PeerUUID, NoneType

        @rtype: col.Iterable
        """
        assert not _TMP_NOTHING_IN_REACTOR_THREAD or not in_main_thread()

        with self.__bdbw_factory() as bdbw:
            # Return a copy of the storage, to free the BigDB wrapper.
            return list(BDBQueries.Messages.get_message_uuids(
                            class_name=class_name,
                            dst_uuid=dst_uuid,
                            bdbw=bdbw))


    @contract_epydoc
    def _get_state_by_uuid(self, tr_uuid):
        """Implementation of interface from C{AbstractTransactionManager}.

        @type tr_uuid: TransactionUUID

        @rtype: AbstractTransaction.State

        @raises KeyError: if the transaction state for such transaction
            does not exist.
        """
        assert not _TMP_NOTHING_IN_REACTOR_THREAD or not in_main_thread()

        with self.__fdbw_factory() as fdbw:
            return FDBQueries.Transactions.get_transaction_state(tr_uuid, fdbw)


    @contract_epydoc
    def _set_state_by_uuid(self, tr_uuid, state):
        """Implementation of interface from C{AbstractTransactionManager}.

        @type tr_uuid: TransactionUUID

        @type state: AbstractTransaction.State

        @raises KeyError: if the transaction state for such transaction
            does not exist.
        """
        assert not _TMP_NOTHING_IN_REACTOR_THREAD or not in_main_thread()

        try:
            with self.__fdbw_factory() as fdbw:
                FDBQueries.Transactions.update_transaction_state(uuid=tr_uuid,
                                                                 state=state,
                                                                 fdbw=fdbw)
        except:
            logger.exception('Could not set state %r for %r', state, tr_uuid)
            raise KeyError(u'Cannot update {!r}'.format(tr_uuid))


    @contract_epydoc
    def __contains__(self, tr):
        """Implementation of interface from C{AbstractTransactionManager}.

        @type tr: AbstractTransaction
        @rtype: bool
        """
        # Even though, for now, we could do
        #     return tr.uuid in self.__transactions_by_uuid
        # , it's safer if we rely on the Mongo to find which transactions
        # indeed exist.
        assert not _TMP_NOTHING_IN_REACTOR_THREAD or not in_main_thread()

        with self.__fdbw_factory() as fdbw:
            return FDBQueries.Transactions.transaction_exists(tr.uuid, fdbw)


    @contract_epydoc
    def get_tr_by_uuid(self, tr_uuid):
        """Implementation of interface from C{AbstractTransactionManager}.

        @rtype: AbstractTransaction, NoneType
        """
        assert not _TMP_NOTHING_IN_REACTOR_THREAD or not in_main_thread()

        return self.__transactions_by_uuid.get(tr_uuid)


    @contract_epydoc
    def post_message(self, message):
        """Implementation of interface from C{AbstractTransactionManager}.

        @type message: AbstractMessage
        """
        assert not _TMP_NOTHING_IN_REACTOR_THREAD or not in_main_thread()

        super(TransactionManagerInFastDBBigDB, self).post_message(message)

        logger.verbose('Posting message %r', message)
        with self.__bdbw_factory() as bdbw:
            BDBQueries.Messages.add_message(msg=message, bdbw=bdbw)


    @contract_epydoc
    def wait_for_message_for_peer(self,
                                  inh, prefer_msg_uuid, still_wait_checker,
                                  d=None):
        """Implementation of interface from C{AbstractTransactionManager}.

        @type inh: AbstractInhabitant
        @type prefer_msg_uuid: MessageUUID, NoneType
        @type still_wait_checker: col.Callable
        @type d: defer.Deferred, NoneType

        @returns: a deferred that fires with the message, or maybe
            a {error.ConnectionClosed()}.
        @rtype: defer.Deferred
        """
        assert not in_main_thread()

        if d is None:
            logger.debug('Checking for message for %r', inh)
        else:
            logger.debug('1 second passed, polling a message for %r', inh)

        d = defer.Deferred() if d is None else d

        if not still_wait_checker():
            logger.debug("Seems like we don't need to wait for %r anymore",
                         inh)
            d.errback(error.ConnectionClosed(u'No need to wait on {!r}'
                                                 .format(inh)))

        else:
            logger.debug("Let's deliver a message for %r, preferrably %r",
                         inh, prefer_msg_uuid)
            reply_msg = self.deliver_message_for_peer(inh, prefer_msg_uuid)
            assert isinstance(reply_msg, (AbstractMessage, NoneType)), \
                   repr(reply_msg)

            if reply_msg is not None:
                # We have an outgoing message for inh already!
                logger.verbose('Going to deliver a message for %r: %r',
                               inh, reply_msg)
                d.callback(reply_msg)
            else:
                # Unfortunately, we don't have a message yet.
                # Let's recall this function in, say, a second.
                logger.verbose('No messages for %r, retrying in %r',
                               inh, POLL_FOR_OUTGOING_MESSAGES_PERIOD)

                callFromThread(
                    task.deferLater,
                    reactor,
                    POLL_FOR_OUTGOING_MESSAGES_PERIOD.total_seconds(),
                    lambda: callInThread(
                                self.__wait_for_message_for_peer_ignore_result,
                                inh, prefer_msg_uuid, still_wait_checker, d))

        return d


    @exceptions_logged(logger)
    @contract_epydoc
    def __wait_for_message_for_peer_ignore_result(
            self, inh, prefer_msg_uuid, still_wait_checker, d):
        """Like C{wait_for_message_for_peer}, but in a secondary thread.

        @note: this should NOT be refactored directly into
            the C{wait_for_message_for_peer} function.

        @return: nothing, since the C{Deferred} to be called is passed
            as an argument
        @rtype: NoneType
        """
        assert not in_main_thread()

        # Ignore returned Deferred object!
        self.wait_for_message_for_peer(
            inh, prefer_msg_uuid, still_wait_checker, d)


    @contract_epydoc
    def deliver_message_for_peer(self, inh, prefer_msg_uuid):
        """Implementation of interface from C{AbstractTransactionManager}.

        @type inh: AbstractInhabitant
        @type prefer_msg_uuid: MessageUUID, NoneType
        @rtype: AbstractMessage, NoneType
        """
        assert not _TMP_NOTHING_IN_REACTOR_THREAD or not in_main_thread()

        with self.__bdbw_factory() as bdbw:
            return BDBQueries.Messages.take_message_for_peer(
                       my_node=self.app.server_process.me,
                       dst_uuid=inh.uuid,
                       prefer_msg_uuid=prefer_msg_uuid,
                       bdbw=bdbw)


    @contract_epydoc
    def deliver_any_message(self):
        """Implementation of interface from C{AbstractTransactionManager}.

        @note: currently, C{TransactionManagerInFastDBBigDB} is assumed to
            be used on Nodes only; and on the Node, C{.deliver_any_message()}
            is not used. Thus, this method is currently not implemented.
            It needs to be fixed if/when C{TransactionManagerInFastDBBigDB}
            becomes used on some hosts.
        """
        raise NotImplementedError('Not used on the Node')


    @contract_epydoc
    def _add_transaction(self, tr, state):
        """Add the transaction to the transaction storage, thread-safely.

        @type tr: AbstractTransaction

        @type state: AbstractTransaction.State
        """
        assert not _TMP_NOTHING_IN_REACTOR_THREAD or not in_main_thread()

        logger.debug('__add %r', tr)

        assert tr.uuid not in self.__transactions_by_uuid, \
               (tr, self.__transactions_by_uuid)

        with self.__transactions_lock:
            self.__transactions_by_uuid[tr.uuid] = tr

            with self.__fdbw_factory() as fdbw:
                FDBQueries.Transactions.add_transaction(
                    type_=tr.type, uuid=tr.uuid,
                    src_uuid=state.tr_src_uuid, dst_uuid=state.tr_dst_uuid,
                    ts=state.tr_start_time, state=state,
                    parent_uuid=None if tr.parent is None
                                     else tr.parent.uuid,
                    fdbw=fdbw)


    @contract_epydoc
    def _del_transaction(self, tr):
        """
        Delete a transaction from the transaction storage,
        in a thread-safe way.

        @type tr: AbstractTransaction
        """
        assert not _TMP_NOTHING_IN_REACTOR_THREAD or not in_main_thread()

        logger.debug('__del %r', tr)

        assert tr.uuid in self.__transactions_by_uuid, \
               (tr, self.__transactions_by_uuid)

        with self.__transactions_lock:
            del self.__transactions_by_uuid[tr.uuid]

            with self.__fdbw_factory() as fdbw:
                FDBQueries.Transactions.del_transaction(uuid=tr.uuid,
                                                        fdbw=fdbw)

            # Also:
            # All children in progress, unfortunately, should be deleted too
            children_copy = set(tr.children_in_progress)
            map(self._del_transaction, children_copy)

        # self.__assert_invariant()


    @contract_epydoc
    def destroy_transaction_unquestionnably(self, transaction):
        """Implementation of interface from C{AbstractTransactionManager}.

        @type transaction: AbstractTransaction
        """
        assert not _TMP_NOTHING_IN_REACTOR_THREAD or not in_main_thread()

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
        assert not _TMP_NOTHING_IN_REACTOR_THREAD or not in_main_thread()

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
                          'Received {!r} as an ACK, but it is not present '
                              'in the transaction storage'
                              .format(message_ack))

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
