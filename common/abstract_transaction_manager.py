#!/usr/bin/python
"""
The general interface which should be satisfied by the implementations
of a Transaction Manager.

The Transaction Manager is an entity allowing to handle start and stop
of transactions independently of the transport below.
"""

#
# Imports
#

from __future__ import absolute_import
import collections as col
import logging
import re
import traceback
from abc import ABCMeta, abstractmethod
from datetime import datetime
from functools import partial
from threading import RLock
from types import NoneType

from twisted.internet import defer

from contrib.dbc import contract_epydoc

from .abstractions import (
    AbstractApp, AbstractInhabitant, AbstractMessage,
    AbstractTransaction, IPrintable)
from .datetime_ex import TimeDeltaEx
from .typed_uuids import MessageUUID, PeerUUID, TransactionUUID
from .utils import duration_logged, exceptions_logged, in_main_thread



#
# Constants
#

logger = logging.getLogger(__name__)
logger_tr_duration = logging.getLogger('duration.transaction')



#
# Classes
#

class TransactionProcessingException(Exception):
    pass



class AbstractTransactionManager(object):
    """An abstract interface for every Transaction Manager implementation.

    @note: There should be only a single instance of transaction manager
           running on the system.

    @ivar __updating_states_by_uuid: the set of UUIDs whose states
        are got for update.
    @type __updating_states_by_uuid: col.MutableSet
    """

    __slots__ = ('_msg_class_by_msg_name', '_ta_class_by_msg_name', 'app',
                 '__updating_states_by_uuid', '__updating_states_by_uuid_lock')

    __metaclass__ = ABCMeta


    __single_instance_initialized = False



    class StateContextManager(IPrintable):
        """
        The abstract interface, specyfing the context manager
        that performs the bookkeeping of a transaction state payload
        stored by the transaction manager.
        """

        __slots__ = ('__tr_uuid', '__state', '__for_update',
                     '__getter', '__setter', '__before_enter', '__after_exit')


        @contract_epydoc
        def __init__(self, tr_uuid, for_update, getter, setter,
                     before_enter, after_exit):
            """Constructor.

            @type tr_uuid: TransactionUUID

            @param for_update: whether the state is retrieved with
                the intention to update it and write back to the storage;
                the default value should likely be C{False}.
            @type for_update: bool

            @param getter: a callable that physically gets the state
                (that is, accepting no arguments).
                This getter might raise C{KeyError} on data inavailability.
            @type getter: col.Callable
            @param setter: a callable that physically sets the state
                (that is, accepting a single argument, the new value).
                This setter might raise C{KeyError} on data inavailability.
            @type setter: col.Callable

            @type before_enter: col.Callable
            @type after_exit: col.Callable
            """
            self.__tr_uuid = tr_uuid
            self.__for_update = for_update
            self.__getter, self.__setter = getter, setter
            self.__before_enter, self.__after_exit = before_enter, after_exit


        def __str__(self):
            return u'tr_uuid={tr_uuid!r}, ' \
                   u'for_update={for_update!r}' \
                    .format(tr_uuid=self.__tr_uuid,
                            for_update=self.__for_update)


        @contract_epydoc
        def __enter__(self):
            """
            @returns: the transaction state payload or C{None}
                if it is not retrievable.
            @rtype: AbstractTransaction.State

            @raises KeyError: (from C{self.getter}) if the transaction state
                for such transaction does not exist.
            """
            logger.verbose(u'Entering %r', self)
            self.__before_enter()
            self.__state = self.__getter()  # may raise KeyError
            return self.__state


        @contract_epydoc
        def __exit__(self, exc_type, exc_val, exc_tb):
            """
            @raises KeyError: (from C{self.getter}) if the transaction state
                for such transaction does not exist.
            """
            if exc_type is exc_val is exc_tb is None:
                if self.__for_update:
                    # Attention! repr(self.__state) may be way too heavy.
                    logger.verbose(u'Leaving %r and saving changes for %s',
                                   self, type(self.__state))
                    self.__setter(self.__state)  # may raise KeyError
                else:
                    # Attention! repr(self.__state) may be way too heavy.
                    logger.verbose(u'Leaving %r and ignoring changes for %s',
                                   self, type(self.__state))
            else:
                # Problem
                logger.error(u'There was a problem inside %r (%r, %r, %r)',
                             self, exc_type, exc_val, exc_tb)
            self.__after_exit()



    @contract_epydoc
    def __init__(self, tr_classes, app, *args, **kwargs):
        """Constructor.

        @param tr_classes: The sequence of supported transaction classes.
        @type tr_classes: tuple
        @precondition: all(issubclass(tr, AbstractTransaction)
                               for tr in tr_classes) # tr_classes

        @param app: The application singleton which owns this manager.
        @type app: AbstractApp
        """
        super(AbstractTransactionManager, self).__init__(*args, **kwargs)

        cls = self.__class__
        assert not cls.__single_instance_initialized
        cls.__single_instance_initialized = True

        self._msg_class_by_msg_name = {ta_class.msg_class.name:
                                               ta_class.msg_class
                                           for ta_class in tr_classes}
        self._ta_class_by_msg_name = {ta_class.msg_class.name: ta_class
                                          for ta_class in tr_classes}

        self.app = app
        self.__updating_states_by_uuid = set()
        self.__updating_states_by_uuid_lock = RLock()


    @abstractmethod
    def __contains__(self, tr):
        """
        @param tr: transaction to check for presence in the
            transaction manager.
            Normal containers DO allow to check the variables of any types
            (and just return C{False} in such cases), but we DO NOT.
        @type tr: AbstractTransaction

        @rtype: bool
        """
        pass


    @abstractmethod
    def get_tr_by_uuid(self, tr_uuid):
        """Get a transaction by its UUID.

        Returns C{None}, if the transaction is absent.

        @rtype: AbstractTransaction, NoneType

        @todo: convert to some more universal C{.find_transaction()} method.
        """
        pass


    @abstractmethod
    def post_message(self, message):
        """Prepare a message for delayed delivery and put it into the queues.

        @note: when overriding, you'd better call a parent implementation
            (this one) before your own.

        @param message: message to deliver.
        @type message: AbstractMessage
        """
        if message.in_transfer:
            logger.debug(u'Previous attempt to transfer %r failed, retrying',
                         message)
            message.in_transfer = False


    @abstractmethod
    def wait_for_message_for_peer(self,
                                  inh, prefer_msg_uuid, still_wait_checker, d):
        """
        Given an inhabitant, return a C{Deferred} which is called
        when a message for that inhabitant is available.
        The result of the C{Deferred} is the C{AbstractMessage}.

        Used in the HTTP Server transports only.

        The argument of the C{Deferred} is precisely that message,
        thus the code inside the C{Deferred} callables should not call
        C{deliver_message_for_peer} or something similar to get it.

        @param inh: for what inhabitant we are trying to deliver a message
        @type inh: AbstractInhabitant

        @param prefer_msg_uuid: prefer that the message UUID to be this.
        @type prefer_msg_uuid: MessageUUID, NoneType

        @param still_wait_checker: a callable predicate which, at any point,
            tells whether the waiting is still needed.
        @type still_wait_checker: col.Callable

        @param d: if exists, this is the C{Deferred} object which should be
            called when the message is available. If omitted (as regularly
            expected), a new C{Deferred} object will be created.
        @type d: defer.Deferred, NoneType

        @returns: a C{Deferred} object that will be called when the message
            is available. Might be equal to C{d} argument.
        @rtype: defer.Deferred
        """
        assert not in_main_thread()


    @abstractmethod
    def deliver_message_for_peer(self, inh, prefer_msg_uuid):
        """Get the next message waiting to be delivered to a particular peer.

        @param inh: an inhabinant who is the destination for the message.dst
        @type inh: AbstractInhabitant

        @param prefer_msg_uuid: (optional) prefer that the message UUID
            to be this.
        @type prefer_msg_uuid: MessageUUID, NoneType

        @returns: The next message for this inhabitant, or None.
        @rtype: AbstractMessage, NoneType
        """
        pass


    @abstractmethod
    def deliver_any_message(self):
        """Get the next message waiting to be delivered to somebody.

        Not used on the Node.

        @returns: The next available message for any inhabitant, or C{None}.
        @rtype: AbstractMessage, NoneType
        """
        pass


    _RE_MSG_NAME_WITH_ACK = re.compile(r'^(.+?)(_ACK)?$')

    @contract_epydoc
    def create_message(self, name, src, dst, status_code,
                       direct=True, uuid=None):
        """Given a message name, create an appropriate message object.

        @param name: message name (may contain '_ACK')
        @type name: str

        @param direct: whether the message destination is the receiving peer.
                       If False, the receiving peer acts as a pass-through.
        @type direct: bool

        @return: A new message instance for the given message.
        @rtype: AbstractMessage
        """
        cls = self.__class__
        mo = cls._RE_MSG_NAME_WITH_ACK.match(name)
        assert mo is not None, repr(mo)

        mo_groups = mo.groups()

        name = mo_groups[0]
        is_ack = mo_groups[1] is not None

        if name not in self._msg_class_by_msg_name:
            raise TransactionProcessingException(u'Message {} not supported'
                                                     .format(name))

        # We have a message. Find its class constructor, and create the message
        msg = self._msg_class_by_msg_name[name](src, dst, is_ack, direct, uuid,
                                                status_code)

        return msg


    @abstractmethod
    def destroy_transaction_unquestionnably(self, transaction):
        """
        Use only if you are fully sure the transaction must be killed;
        such as: when any errors occured during the transaction processing.

        @type transaction: AbstractTransaction
        """
        pass


    @abstractmethod
    def handle_incoming_message(self, _message):
        """
        All the transaction handling operations
        invoked when the message is being received.

        @param _message: Message being received.
        @type _message: AbstractMessage

        @raises TransactionProcessingException: if the message could not
            be processed properly.
        """
        pass


    @abstractmethod
    def get_tr_states(self, class_name, dst_uuid):
        """Get the states of the running transactions using some criteria.

        @param class_name: (optional) the name of the desired
            transaction class.
        @type class_name: basestring, NoneType

        @param dst_uuid: (optional) the UUID of the destination peer.
        @type dst_uuid: PeerUUID, NoneType

        @return: the iterable over the transaction states.
        @rtype: col.Iterable
        """
        pass


    @abstractmethod
    def get_outgoing_msg_uuids(self, class_name, dst_uuid):
        """Return the UUIDs of the outgoing messages using some criteria.

        @param class_name: (optional) the name of the desired message class.
        @type class_name: basestring, NoneType

        @param dst_uuid: (optional) the UUID of the destination peer.
        @type dst_uuid: PeerUUID, NoneType

        @return: the iterable over the the UUIDs of outgoing messages.
        @rtype: col.Iterable
        """
        pass


    @contract_epydoc
    def open_tr_state(self, tr_uuid, for_update):
        """Open the transaction-type-specific state payload, maybe for writing.

        If opened with C{for_update=True}, will be automatically written back
        to the storage after leaving the context.

        @param tr_uuid: the UUID of the transaction, for which
            the payload should be returned.
        @type tr_uuid: TransactionUUID

        @param for_update: whether the state is retrieved with the intention
            of update it and automatically write back to the storage;
            the default value should likely be C{False}.
        @type for_update: bool

        @raise KeyError: if the transaction could not be found.

        @returns: the state-retrieving context manager.
        @rtype: AbstractTransactionManager.StateContextManager
        """
        cls = self.__class__

        logger.debug(u'Opening state context manager for transaction %s%s',
                     tr_uuid,
                     ' for update' if for_update else '')

        return cls.StateContextManager(
                   tr_uuid=tr_uuid,
                   for_update=for_update,
                   getter=partial(self._get_state_by_uuid, tr_uuid),
                   setter=partial(self._set_state_by_uuid, tr_uuid),
                   before_enter=partial(self.__before_context_enter,
                                        tr_uuid, for_update),
                   after_exit=partial(self.__after_context_exit,
                                      tr_uuid, for_update))


    def __before_context_enter(self, tr_uuid, for_update):
        """Called right before the state accessing context is opened."""
        # Need a lock to atomically check for existence and add the value.
        logger.verbose('Entering state context for %s for %s',
                       tr_uuid, 'update' if for_update else 'read')
        if for_update:
            with self.__updating_states_by_uuid_lock:
                if tr_uuid in self.__updating_states_by_uuid:
                    logger.error('Warning: re-entering the state access '
                                     'context for %r: %s',
                                 tr_uuid, ''.join(traceback.format_stack()))
                else:
                    self.__updating_states_by_uuid.add(tr_uuid)


    def __after_context_exit(self, tr_uuid, for_update):
        """Called right after the state accessing context is closed."""
        logger.verbose('Leaving state context for %s for %s',
                       tr_uuid, 'update' if for_update else 'read')
        # We don't even need locks here.
        if for_update:
            try:  # EAFP
                self.__updating_states_by_uuid.remove(tr_uuid)
            except KeyError:
                logger.error('Exiting the state access context for %r '
                                 "which doesn't present: %s",
                             tr_uuid, ''.join(traceback.format_stack()))


    @abstractmethod
    def _get_state_by_uuid(self, tr_uuid):
        """Given the transaction UUID, get the transaction state.

        @type tr_uuid: TransactionUUID

        @rtype: AbstractTransaction.State

        @raises KeyError: if the transaction state for such transaction
            does not exist.
        """
        pass


    @abstractmethod
    def _set_state_by_uuid(self, tr_uuid, state):
        """Given the transaction UUID, set the transaction state.

        @type tr_uuid: TransactionUUID

        @type state: AbstractTransaction.State

        @raises KeyError: if the transaction state for such transaction
            does not exist.
        """
        pass


    __EARLIEST_TRANSACTION_START = None

    @contract_epydoc
    def _create_transaction(self, transaction, state):
        """Create a transaction - common internal actions.

        In particular, this function calls C{on_begin()} of every transaction.
        Even more, if C{on_begin()} hasn't returned a C{Deferred} but
        immediately completed itself, you may assume that, at the end of
        C{_create_transaction}, the outgoing message is ready.

        @type transaction: AbstractTransaction
        @type state: AbstractTransaction.State
        """
        cls = self.__class__
        assert not in_main_thread(), repr(transaction)

        # The transaction is created. But this means that the transaction
        # start_time has been set up, so now we can mark the earliest
        # start_time ever seen.
        if cls.__EARLIEST_TRANSACTION_START is None:
            cls.__EARLIEST_TRANSACTION_START = transaction.start_time

        # First, we add the transaction itself
        # to the list of transactions in progress.
        self._add_transaction(transaction, state)
        logger.debug(u'+1 tr %r, %r',
                     transaction, self)

        # Add to the parent list of transaction-in-progress
        if transaction.parent is not None:
            logger.debug(u'BINDING %r TO PARENT %r',
                         transaction, transaction.parent)
            transaction.parent.children_in_progress.add(transaction)

        # Then, we call the per-transaction handlers (which execute
        # the transaction-specific actions, start new transactions, etc).

        # We should not have come here unless we have @exceptions_logged()
        # somewhere above in the stack; but for safety, let's add yet another
        # one for each on_begin() and on_end().


        @duration_logged(u'{}'.format(transaction.msg_class.name))
        def _wrapped_on_begin():
            """
            Wraps C{on_begin()} call of a transaction to make sure any errors
            are logged.

            @todo: possibly, C{on_begin()} and C{on_end()} may be called
                with the transaction state as an argument, what could save
                some MongoDB calls.

            @return: a C{Deferred} which is called when on_begin has completed.
            """
            assert not in_main_thread(), repr(transaction)

            d = defer.maybeDeferred(transaction.on_begin)
            d.addCallback(
                lambda r: logger.debug(
                              u'%r: after _wrapped_on_begin() cb',
                              transaction))
            d.addErrback(
                lambda failure: logger.error(
                                    u'Bad %r in _wrapped_on_begin(): %s/%s',
                                    transaction,
                                    failure.getErrorMessage(),
                                    failure.getTraceback()))
            return d


        # We call the per-operator (per-node/per-host process) handlers.
        self.app.handle_transaction_before_create(transaction, state)

        logger.debug(u'> on_begin handler for %r', transaction)
        d = _wrapped_on_begin()
        logger.debug(u'< on_begin handler for %r', transaction)

        # Then, we again call the per-operatorhandlers.
        self.app.handle_transaction_after_create(transaction, state)

        # We processed its beginning. Maybe we should end this transaction.
        # Is the transaction complete? Do we have any waiting child
        # transactions?
        logger.debug(u'Done with creation, now trying to destroy %r',
                     transaction)
        self.try_destroy_transaction(transaction)


    @contract_epydoc
    def _destroy_transaction(self, transaction):
        """Destroy a transaction - common internal actions.

        In particular, this function calls C{on_end()} of every transaction.
        Even more, if C{on_end()} hasn't returned a C{Deferred} but
        immediately completed itself, you may assume that, at the end of
        C{_destroy_transaction}, the outgoing message is ready.

        @type transaction: AbstractTransaction
        """
        cls = self.__class__
        assert not in_main_thread(), repr(transaction)

        if not transaction.destroying_lock.acquire(False):
            logger.debug(u'%r is being locked for destroy already',
                         transaction)
        elif transaction.destroying:
            # Reentering
            logger.debug(u'%r is destroying already',
                         transaction)
        else:
            transaction.destroying = True
            transaction.destroying_lock.release()

            # First, call the per-transaction handlers (which execute
            # the transaction-specific actions, maybe start new transactions,
            # etc).

            # We should not have come here unless we have @exceptions_logged()
            # somewhere above in the stack; but for safety, let's add
            # yet another one for each on_begin() and on_end().


            @duration_logged(u'{}'.format(transaction.msg_class.name))
            def _wrapped_on_end():
                """
                Wraps C{on_end()} call of a transaction to make sure any errors
                are logged.

                @todo: possibly, C{on_begin()} and C{on_end()} may be called
                    with the transaction state as an argument, what could save
                    some MongoDB calls.

                @note: it's theoretically possible that during calling
                    C{.on_end()} of a transaction, the parent transaction is
                    already destroyed. This is because

                @return: a C{Deferred} which is called when on_end
                    has completed.
                """
                assert not in_main_thread(), repr(transaction)

                d = defer.maybeDeferred(transaction.on_end)
                # At the stage of callback execution, the transaction likely
                # has already died.
                d.addCallback(
                    lambda r: logger.debug(u'%r: after _wrapped_on_end() cb',
                                           transaction))
                d.addErrback(
                    lambda failure: logger.error(u'Error in %s for %r: %s\n%s',
                                                 u'_wrapped_on_end()',
                                                 transaction,
                                                 failure.getErrorMessage(),
                                                 failure.getTraceback()))
                return d


            logger.debug(u'> on_end handler for %r', transaction)
            d = _wrapped_on_end()
            logger.debug(u'< on_end handler for %r', transaction)

            # Open last transaction state before deleting it.
            with self.open_tr_state(tr_uuid=transaction.uuid,
                                    for_update=False) as state:
                pass  # do nothing here, but keep the variable for later

            # Then, remove the transaction
            # from the list of transactions in progress.
            self._del_transaction(transaction)
            logger.debug(u'-1 tr %r: %r',
                         transaction, self)

            logger.debug(u'%r: after _wrapped_on_end() outside', transaction)

            # Now let's log the transaction duration.
            assert cls.__EARLIEST_TRANSACTION_START is not None
            end_time = datetime.utcnow()
            offset = TimeDeltaEx.from_timedelta(
                         transaction.start_time
                         - cls.__EARLIEST_TRANSACTION_START)
            duration = TimeDeltaEx.from_timedelta(end_time
                                                  - transaction.start_time)
            logger_tr_duration.debug(u'[%s %r %f %f]: %s',
                                     transaction.msg_class.name,
                                     transaction,
                                     offset.in_seconds(),
                                     duration.in_seconds(),
                                     duration)

            assert transaction._waiting is False, \
                   u'@pause-ing the on_end() handler is not supported, ' \
                   u'please check the transaction {!r}!' \
                       .format(transaction)

            # Then, call the per-operator (per-node/per-host process) handlers
            # which perform the particular message sending, etc.
            self.app.handle_transaction_destroy(transaction, state)

            logger.debug(u'Notifying subscribers about %r completion',
                         transaction)
            # And after that, notify all the subscribers
            # that this transaction was completed.
            _failure = transaction.failure
            if not _failure:
                logger.verbose(u'callback %r with state', transaction)
                transaction.completed.callback(state)
            else:
                logger.verbose(u'errback %r with %r', transaction, _failure)
                transaction.completed.errback(_failure)

            # Remove from the parent list of transaction-in-progress
            if transaction.parent is not None:
                logger.debug(u'Removing %r from parent (%r)',
                             transaction, transaction.parent)
                transaction.parent.children_in_progress.remove(transaction)

            logger.debug(u'_destroy_transaction() completed for %r',
                         transaction)
            logger.debug('')


    @contract_epydoc
    def create_new_transaction(self,
                               name, src, dst, parent=None,
                               *args, **kwargs):
        """
        Given a transaction name, create an completely new
        appropriate transaction.

        @param name: message name.
        @type name: str

        @param parent: parent transaction.
        @type parent: NoneType, AbstractTransaction

        @param args: The list of positional arguments which should be
                     passed to the transaction.

        @param kwargs: The dictionary of the named arguments which should be
                       passed to the transaction.

        @return: A new transaction instance for the given message.
        @rtype: AbstractTransaction
        """
        _args = args if args is not None else []
        _kwargs = kwargs if kwargs is not None else {}

        # Create a brand new message
        msg = self.create_message(name, src, dst,
                                  AbstractMessage.StatusCodes.OK)
        tr = self._create_transaction_for_message(msg, parent,
                                                  *_args, **_kwargs)
        return tr


    @contract_epydoc
    def _create_transaction_for_message(self, message, parent=None,
                                        *args, **kwargs):
        """Given a message, create an appropriate transaction.

        @param args: the positional arguments which should be passed
                     to the transaction.

        @param kwargs: the named arguments which should be passed
                       to the transaction.

        @return: a new transaction instance for the given message.
        @rtype: AbstractTransaction
        """
        name = message.name

        _args = args if args is not None else []
        _kwargs = kwargs if kwargs is not None else {}

        if name not in self._ta_class_by_msg_name:
            raise TransactionProcessingException(
                      u'Transaction {} not supported'.format(name))

        tr_class = self._ta_class_by_msg_name[name]
        transaction = tr_class(manager=self,
                               message=message,
                               parent=parent)

        try:
            state = tr_class.State(tr_start_time=transaction.start_time,
                                   tr_uuid=transaction.uuid,
                                   tr_src_uuid=transaction.message.src.uuid,
                                   tr_dst_uuid=transaction.message.dst.uuid,
                                   *_args, **_kwargs)
        except TypeError:
            logger.error(u'Problems during creating the %s.State()',
                         tr_class.__name__)
            raise

        self._create_transaction(transaction, state)
        return transaction


    @contract_epydoc
    def try_destroy_transaction(self, transaction):
        """
        Check if the transaction can be destroyed (e.g. doesn't have
        any children in progress), and destroy it if possible.

        In fact, this transaction may have been destroyed already.

        @type transaction: AbstractTransaction
        """
        if transaction in self:
            logger.debug(u'Trying to destroy %r', transaction)
            if transaction.children_in_progress:
                logger.debug(u'For transaction %r the following children '
                                 u'still exist: %r',
                             transaction,
                             transaction.children_in_progress)
            elif transaction._waiting:
                logger.debug(u'Transaction %r is in Waiting state',
                             transaction)
            else:
                self._destroy_transaction(transaction)

                if transaction.parent is not None:
                    self.try_destroy_transaction(transaction.parent)
        else:
            logger.debug(u'%r already destroyed', transaction)
