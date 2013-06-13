#!/usr/bin/python
# -*- coding: utf-8 -*-
"""Various abstract classes and interfaces used throughout the system."""

#
# Imports
#

from __future__ import absolute_import
import collections as col
import logging
import sys
import threading
from abc import ABCMeta, abstractmethod, abstractproperty
from datetime import datetime, timedelta
from functools import wraps, total_ordering
from types import NoneType
from uuid import UUID

from twisted.internet.defer import Deferred
from twisted.python.failure import Failure

from contrib.dbc import consists_of, contract_epydoc

from .typed_uuids import MessageUUID, PeerUUID, TransactionUUID
from .utils import antisymmetric_comparison, coalesce, gen_uuid



# Logging
#

logger = logging.getLogger(__name__)
logger_message = logging.getLogger('common.AbstractMessage')
logger_transaction = logging.getLogger('common.AbstractTransaction')
logger_inhabitant = logging.getLogger('common.AbstractInhabitant')



#
# Interfaces
#

class IPrintable(object):
    """
    This interface requires you to implement the C{__str__} method
    which generates the presentation of the object as its arguments
    to its constructor; then the C{__repr__} will be provided automatically.
    """

    __metaclass__ = ABCMeta


    @abstractmethod
    def __str__(self):
        return ''

    def __repr__(self):
        return u'{}({})'.format(self.__class__.__name__, self)



class IJSONable(object):
    """
    This interface requires you to implement the methods converting the object
    from and to JSON-compatible structure (dicts/lists/other simple types
    only).
    """

    __metaclass__ = ABCMeta


    @abstractmethod
    def to_json(self):
        """
        Convert this object to the simple JSON-compatible structure.

        Can be inherited (if it is ok that the inherited class is serialized
        to a dictionary).
        """
        return {}


    @classmethod
    @abstractmethod  # TODO: should become @abstractclassmethod
    def from_json(cls, json_struct):  # pylint:disable=E0213
        """
        Create this object from a simple JSON-compatible structure.
        The derived method must be a classmethod.
        Such method should return a callable (probably functools.partial
        object) which creates an instance of C{cls} class from C{json_struct}
        upon call.

        Why callable rather than the class itself? This way, the from_json
        implementation may be inherited.

        @rtype: col.Callable
        """
        return cls


    def __str__(self):
        return u', '.join(u'{}={!r}'.format(k, v)
                              for k, v in self.to_json().iteritems())


    def __repr__(self):
        return u'{}.from_json({})'.format(self.__class__.__name__, self)



class IOutgoingSession(object):
    """
    This interface defines how an outgoing connection session should look.
    """

    __metaclass__ = ABCMeta


    @abstractmethod
    def connecting(self, realm):
        """
        This method should be called to notify that we are starting a session
        of connecting to some peer.
        """
        pass


    @abstractmethod
    def connected(self, realm):
        """
        This method should be called to notify that we successfully connected
        to some peer.
        """
        pass


    @abstractmethod
    def connection_failed(self, realm):
        """
        This method should be called to notify that we lost a connection
        to some peer for some unfortunate reasons.
        """
        pass



class IIncomingSession(object):
    """
    This interface defines how an incoming connection session should look.
    """

    __metaclass__ = ABCMeta


    @abstractmethod
    def connected(self):
        """
        This method should be called to notify that some peer is connected
        to us.
        """
        pass



#
# Classes
#

class AbstractApp(object):
    __metaclass__ = ABCMeta


    @abstractmethod
    def handle_transaction_before_create(self, tr, state):
        """Must be overridden!

        @type tr: AbstractTransaction
        @type state: AbstractTransaction.State
        """
        pass


    @abstractmethod
    def handle_transaction_after_create(self, tr, state):
        """Must be overridden!

        @type tr: AbstractTransaction
        @type state: AbstractTransaction.State
        """
        pass


    @abstractmethod
    def handle_transaction_destroy(self, tr, state):
        """Must be overridden!

        @type tr: AbstractTransaction
        @type state: AbstractTransaction.State
        """
        pass



class AbstractMessage(IPrintable):
    """
    Any message in the system, be that a request (ANY_MESSAGE)
    or response (ANY_MESSAGE_ACK).

    The messages have an ordering relationship defined.
    The message A is considered ordered before message B (A < B)
    if A.priority < B.priority,
    or A.start_time < B.start_time,
    or A.uuid < B.uuid (not that much important, needed only to
    distinguish between A and B if they have different uuid-s).

    If the message A is ordered before message B, it is assumed
    to be processed earlier.

    @todo: body_receiver is either BodyReceiver(Protocol) for a response,
           or not defined. That should be made common for request and response.

    @cvar name: name of the message. Is abstract, and should be implemented
                in every subclass.
    @type name: str

    @cvar version: version of the message. Is abstract, and should be
                   implemented in every subclass.
    @type version: int

    @cvar priority: priority of the message. The messages with the higher
                    priority take precedence in the message queues.
                    Default value is 50; it is assumed that the priorities will
                    be in the range from 0 to 100, 100 is the most-important
                    ones.
                    Feel free to override in any subclass.
    @type priority: int

    @note: This is an abstract class that should never be used on its own.

           Every subclass MUST implement name attribute and version attribute.

           The subclass MAY implement the C{init_from_headers()} and
           C{get_headers()} methods.
           The subclass MAY implement the C{init_from_body()} and C{get_body()}
           methods. Alternately, it may implement C{_init_from_body()} /
           C{_init_from_body_ack()} and/or C{_get_body()} / C{_get_body_ack()},
           to separate between the request and response code.

           If the C{init_from_headers()} method is implemented, it should
           contain the code for conversion from the HTTP headers of the
           received message to the internal representation; note that the
           conversion should alter the C{self} object for the method.

           If the C{get_headers()} method is implemented, it should contain
           the code for conversion from the internal message representation to
           the dictionary of HTTP headers ready for sending over the networks.

           If the C{init_from_body()} method is implemented, it should contain
           the code for conversion from the textual body of the received
           message to the internal representation; note that the conversion
           should alter the C{self} object for the method. Also note that the
           C{init_from_body() method will be called AFTER calling the
           C{init_from_headers()} method, if available.
           By default, it calls the C{_init_from_body()} method on request
           messages, and calls the C{_init_from_body_ack()} method in response
           message (is_ack = True).

           If the get_body() method is implemented, it should contain the code
           for conversion from the internal message representation to
           the textual body ready for sending over the networks.
           By default, it calls the _get_body() method on request messages,
           and calls the _get_body_ack() method in response message
           (is_ack = True).

    @ivar uuid: the UUID of the message and its associated transaction.
    @type uuid: MessageUUID

    @ivar in_transfer: whether the message is being transferred right now.

        It is important only for outgoing messages (all incoming messages
        get processed only after they are fully received at the moment,
        what may be wrong btw).
        But for outgoing messages, while the message is sitting in the queue,
        C{in_transfer} is C{False};
        but as soon as the message is put on the network, C
        {in_transfer} becomes C{True}.
    @type in_transfer: bool

    @ivar revive: whether the message is reviving.
        Currently, all messages are assumed reviving except
        1. C{LogoutMessage} and
        2. C{LoginMessage} when explicitly set C{revive=False}.
    @type revive: bool

    @param good: whether the message was fully sent/retrieved successfully.
    @type good: bool

    @param status_code: the status code of the message.
    @type status_code: numbers.Integral

    @todo: shouldn't we delete the C{revive} field as outdated?
    """

    __slots__ = ('src', 'dst', 'is_ack', 'direct', 'uuid', 'body_receiver',
                 'start_time', 'good', 'status_code', 'in_transfer', 'revive')

    __metaclass__ = ABCMeta


    class AbstractResultCodes(object):
        """Result codes for any message (stored inside the message contents).

        Any subclass must implement the following fields:
          * _all - the set/frozenset that enumerates all the supported result
                   codes;
          * _to_str - the mapping from each of the codes to the string;
          * _is_good - the mapping from each of the codes to the success
                       status.

        @deprecated: C{AbstractStatusCodes} should be used instead.
        """
        __slots__ = ()

        __metaclass__ = ABCMeta


        # TODO: change to @abstractclassproperty or similar when possible
        @classmethod
        def _all(cls):
            return set()

        # TODO: change to @abstractclassproperty or similar when possible
        @classmethod
        def _to_str(cls):
            return {}

        # TODO: change to @abstractclassproperty or similar when possible
        @classmethod
        def _is_good(cls):
            return {}


        @classmethod
        def to_str(cls, code):
            return cls._to_str().get(code, 'N/A:{}'.format(code))


        @classmethod
        def is_good(cls, code):
            return cls._is_good().get(code, False)


    class StatusCodes(object):
        """Result codes for any message (stored inside the message headers).

        Any subclass may implement the following fields:
          * _all - the set/frozenset that enumerates all the supported result
                   codes;
          * _to_str - the mapping from each of the codes to the string;
          * _is_good - the mapping from each of the codes to the success
                       status.
        """
        __slots__ = ()

        __metaclass__ = ABCMeta

        (OK,
         GENERAL_FAILURE) = range(2)
        # All status codes in overridden classes must start from 64,
        # i.e. be in a form like C{range(64, ...)}.


        # TODO: change to @abstractclassproperty or similar when possible
        @classmethod
        def _all(cls):
            """May be overridden."""
            return frozenset([
                cls.OK,
                cls.GENERAL_FAILURE,
            ])

        # TODO: change to @abstractclassproperty or similar when possible
        @classmethod
        def _to_str(cls):
            """May be overridden."""
            return {
                cls.OK: 'Ok',
                cls.GENERAL_FAILURE: 'General failure',
            }

        # TODO: change to @abstractclassproperty or similar when possible
        @classmethod
        def _is_good(cls):
            """May be overridden."""
            return {
                cls.OK: True,
                cls.GENERAL_FAILURE: False,
            }


        @classmethod
        def to_str(cls, code):
            return cls._to_str().get(code, 'N/A:{}'.format(code))


        @classmethod
        def is_good(cls, code):
            return cls._is_good().get(code, False)


    name = None
    version = None

    priority = 50


    @contract_epydoc
    def __init__(self, src, dst, is_ack=False, direct=True, uuid=None,
                 status_code=StatusCodes.OK, start_time=None,
                 *args, **kwargs):
        """Constructor.

        @param src: (mandatory) transaction originator (sender).
        @type src: AbstractInhabitant

        @param dst: (mandatory) transaction destination (addressee).
        @type dst: AbstractInhabitant

        @param direct: whether the message is sent to the peer directly.
                       If False, the peer acts as a pass-through.

        @type uuid: UUID, NoneType

        @type start_time: datetime, NoneType
        """
        assert not args and not kwargs, (args, kwargs)

        super(AbstractMessage, self).__init__(*args, **kwargs)
        self.src, self.dst = src, dst

        self.is_ack = is_ack
        self.direct = direct

        self.uuid = MessageUUID.safe_cast_uuid(uuid if uuid is not None
                                                    else gen_uuid())

        self.body_receiver = None
        self.start_time = start_time if start_time is not None \
                                     else datetime.utcnow()

        self.good = True
        self.status_code = status_code
        self.in_transfer = False
        self.revive = True


    def __str__(self):
        """Magic method for representation as a string.

        Note that at the moment of __str__ calling, the body may have not yet
        been downloaded completely. Check for the self.body_receiver.done
        status.
        """
        return u'src={self.src!r}, dst={self.dst!r}, '\
               u'uuid={self.uuid!r}{opt_ack}{opt_good}{status_code}'\
               u'{in_transfer}'.format(
                   self=self,
                   opt_ack=', is_ack=True' if self.is_ack else '',
                   opt_good='' if self.good else ' (BAD)',
                   status_code=', status_code={!r}'.format(self.status_code),
                   in_transfer=' (IO)' if self.in_transfer else ''
               )


    @property
    def type(self):
        """The name of the transaction, optionally with trailing C{_ACK}."""
        return self.name + ('_ACK' if self.is_ack else '')


    def __cmp__(self, other):
        r"""Ordering rule (see the comment on the whole class).

        Two messages are considered equal if they both have the same uuid.
        In fact, there should never be two different objects of
        the same message.
        """
        if isinstance(other, AbstractMessage):
            result = cmp((-self.priority,  self.start_time,  self.uuid),
                         (-other.priority, other.start_time, other.uuid))

            # The UUIDs are equal then and only then if the objects are
            # the same.
            assert ((id(self) == id(other)) ==
                    (self.uuid == other.uuid) ==
                    (result == 0)), \
                   (self, other)

            return result
        else:
            raise TypeError(u'other is {!r}'.format(other))


    def __hash__(self):
        """
        For use in hashable collections.
        """
        return hash(self.uuid)


    def get_headers(self):
        """
        HTTP headers of the message: they will be added to the physical
        message.

        Is empty by default, but may be overridden in any subclass.

        @rtype: dict
        """
        return {}


    def init_from_headers(self, headers):
        """
        Internalization of the information available from the HTTP headers
        Alters the "self" object.

        Is empty by default, but may be overridden in any subclass.

        @postcondition: result is None
        """
        pass


    def get_body(self):
        """
        External representation of the message:
        the contents that is sent for it in the physical message.

        Is empty by default, but may be overridden in any subclass.

        @note: make sure to never call it in the main thread!
            It may be heavy to process.

        @rtype: types.GeneratorType
        """
        return self._get_body_ack() if self.is_ack else self._get_body()


    def _get_body(self):
        """
        External representation of the request message:
        the contents that is sent for it in the physical message.

        Is empty by default (in fact, contains a single zero byte string),
        but may be overridden in any subclass.

        @note: make sure to never call it in the main thread!
            It may be heavy to process.

        @rtype: types.GeneratorType
        """
        yield '\x00'


    def _get_body_ack(self):
        """
        External representation of the response message:
        the contents that is sent for it in the physical message.

        Is empty by default (in fact, contains a single zero byte string),
        but may be overridden in any subclass.

        @note: make sure to never call it in the main thread!
            It may be heavy to process.

        @rtype: types.GeneratorType
        """
        yield '\x00'


    def init_from_body(self, body):
        """
        Internalization of the message from the physical contents.
        Alters the C{self} object.

        Is empty by default, but may be overridden in any subclass.

        @note: make sure to never call it in the main thread!
            It may be heavy to process.

        @postcondition: result is None
        """
        try:
            result = (self._init_from_body_ack if self.is_ack
                                               else self._init_from_body)(body)
        except Exception:
            logger.exception('Could not parse body')
            self.good = False
            result = None

        return result


    def _init_from_body(self, body):
        """
        Internalization of the request message from the physical contents.
        Alters the C{self} object.

        Is empty by default, but may be overridden in any subclass.

        @note: make sure to never call it in the main thread!
            It may be heavy to process.

        @postcondition: result is None
        """
        pass


    def _init_from_body_ack(self, body):
        """
        Internalization of the response message from the physical contents.
        Alters the "self" object.

        Is empty by default, but may be overridden in any subclass.

        @note: make sure to never call it in the main thread!
            It may be heavy to process.

        @postcondition: result is None
        """
        pass


    def reply(self, status_code=StatusCodes.OK):
        """Generate a reply to this message.

        @returns: A new message which is replying to this one.
        @rtype: AbstractMessage
        """
        assert not self.is_ack, \
               'Cannot reply to an ACK message {!r}!'.format(self)
        reply_msg = self.__class__(src=self.dst,
                                   dst=self.src,
                                   is_ack=True,
                                   uuid=self.uuid,
                                   status_code=status_code)
        return reply_msg


    def get_pragma(self):
        """Get the value of Pragma HTTP header for this message.

        @returns: The Pragma HTTP header contents for this message.
        @rtype: str
        """
        return ','.join('{}={}'.format(*pairs)
                            for pairs
                            in {'msgtype': self.type,
                                'version': self.version,
                                'msgid': self.uuid,
                                'from': self.src.uuid,
                                'to': self.dst.uuid,
                                'errors': '',
                                'status_code': self.status_code}.iteritems())


    @property
    def maximum_timeout(self):
        """Get the timeout after which the message is considered lost.

        The duration lasts from the moment after the message is completely sent
        out, to the moment when the ACK message contents (with body!)
        is completely received.

        After "losing" a (non-ACK) message, the ACK message with good=False
        will be generated automatically.

        If C{None}, the message will not be considered lost automatically,
        no matter how much time passed.

        @rtype: NoneType, timedelta
        """
        return None



def pauses(f):
    """
    The decorator that can be put on a transaction method to mark
    that before the method is completed, the C{.pause()} transaction method
    should be invoked.

    @note: C{@pauses} decorator pauses the transaction object B{before}
        the decorated function is called, while C{@unpauses} decorator
        unpauses the transaction object B{afer} the decorated function
        is called.
    """
    @wraps(f)
    @contract_epydoc
    def wrapper(self, *args, **kwargs):
        """
        @type self: AbstractTransaction
        """
        logger.verbose('> Pausing %r', self)
        self.pause()
        logger.verbose('< Pausing %r', self)
        result = f(self, *args, **kwargs)
        return result

    return wrapper


def unpauses(f):
    """
    The decorator that can be put on a transaction method to mark
    that after the method is completed, the C{.unpause()} transaction method
    should be invoked.

    @note: C{@pauses} decorator pauses the transaction object B{before}
        the decorated function is called, while C{@unpauses} decorator
        unpauses the transaction object B{afer} the decorated function
        is called.
    """
    @wraps(f)
    @contract_epydoc
    def wrapper(self, *args, **kwargs):
        """
        @type self: AbstractTransaction
        """
        result = f(self, *args, **kwargs)
        logger.verbose('> Unpausing %r and trying to destroy', self)
        self.unpause(try_destroy=True)
        logger.verbose('< Unpausing %r and trying to destroy', self)
        return result

    return wrapper


def unpauses_incoming(f):
    """
    Similar to C{@unpause}, but should be put on the methods of an incoming
    transaction. Contrary to C{@unpause}, this decorator does NOT try
    to destroy the transaction after unpausing, relying upon the incoming
    transaction processing that will try to destroy it too.

    The decorator that can be put on a transaction method to mark
    that after the method is completed, the C{.unpause()} transaction method
    should be invoked.

    @note: C{@pauses} decorator pauses the transaction object B{before}
        the decorated function is called, while C{@unpauses} decorator
        unpauses the transaction object B{afer} the decorated function
        is called.
    """
    @wraps(f)
    @contract_epydoc
    def wrapper(self, *args, **kwargs):
        """
        @type self: AbstractTransaction
        """
        result = f(self, *args, **kwargs)
        logger.verbose('> Unpausing %r', self)
        self.unpause(try_destroy=False)
        logger.verbose('< Unpausing %r', self)
        return result

    return wrapper



@antisymmetric_comparison
@total_ordering
class AbstractTransaction(IPrintable):
    """An abstract class for every transaction in progress.

    @note: This is an abstract class that should never be used on its own.

    @cvar msg_class: message class for the message causing this transaction.
                     Is abstract, and should be implemented in every subclass.
    @type msg_class: AbstractMessage

    @cvar State: every transaction class should have C{State} subclass defined,
        which contains the state-specific payload for this transaction.
    @type State: type

    @param manager: Transaction manager instance supporting this transaction.
    @type manager: AbstractTransactionManager

    @ivar _waiting: whether the transaction is in "waiting" state between
        C{on_begin} and C{on_end};
        the transaction should enter the "waiting" state if it is an
        outgoing one, and will continue on reply; otherwise it will be
        autokilled right after C{on_begin} handling
        (unless it generated any subtransactions), thus immediately
        entering the C{on_end} handler.
        Default value is C{False}, that is by default the transaction
        moves from C{on_begin} to C{on_end} handlers immediately, if it
        doesn't have any subtransactions.
        The subclasses should not change this field directly, but invoke
        the C{.pause()} and C{.unpause()} methods instead.
    @type _waiting: bool

    @param message: the message that starts this transaction.
    @type message: (NoneType, messages.AbstractMessage)

    @param message_ack: the message that ends this transaction.
    @type message_ack: (NoneType, messages.AbstractMessage)

    @ivar parent: parent transaction (if applicable).
    @type parent: (NoneType, AbstractTransaction)

    @ivar children_completed: list of all child transactions which were
        completed already, in the order of their completion (which, for
        various reasons, may be different from the order of their start).
    @type children_completed: list<AbstractTransaction>

    @ivar children_in_progress: set of all child transactions being
                                in progress.
    @type children_in_progress: set<AbstractTransaction>

    @ivar completed: The C{Deferred} object which is called when the
        transaction is completed, and is passed with the transaction object.
        At the moment of the C{completed.callback}:
          * The transaction's C{.on_end()} has been called already;
          * The transaction B{has been} removed from the transaction manager
            already.
    @type completed: Deferred

    @ivar failure: If the transaction failed in the middle,
                   this contains a Twisted Failure object.
    @type failure: (NoneType, Failure)
    """
    __slots__ = ('manager', '_waiting', 'message', 'message_ack',
                 'children_completed', 'children_in_progress',
                 'parent', 'uuid', 'start_time', 'max_duration', 'completed',
                 'failure', 'destroying_lock', 'destroying')

    __metaclass__ = ABCMeta

    DEFAULT_DURATION = timedelta(seconds=10)  # Constant, don't ever change!

    msg_class = None



    class State(IPrintable):
        """The transaction state payload specific for any transaction type.

        @ivar tr_start_time: (read-only) the start time of the transaction.
        @type tr_start_time: datetime
        @ivar tr_uuid: (read-only) the UUID of the transaction.
        @type tr_uuid: TransactionUUID
        @ivar tr_src_uuid: (read-only) the UUID of the transaction source peer.
        @type tr_src_uuid: PeerUUID
        @ivar tr_dst_uuid: (read-only) the UUID of the transaction destination
            peer.
        @type tr_dst_uuid: PeerUUID
        """

        __slots__ = ('__tr_type', '__tr_start_time', '__tr_uuid',
                     '__tr_src_uuid', '__tr_dst_uuid')


        @contract_epydoc
        def __init__(self,
                     tr_type, tr_start_time, tr_uuid, tr_src_uuid, tr_dst_uuid,
                     *args, **kwargs):
            super(AbstractTransaction.State, self).__init__(*args, **kwargs)
            self.__tr_type = tr_type
            self.__tr_start_time = tr_start_time
            self.__tr_uuid = tr_uuid
            self.__tr_src_uuid = tr_src_uuid
            self.__tr_dst_uuid = tr_dst_uuid


        @property
        def tr_type(self):
            """Read-only property"""
            return self.__tr_type


        @property
        def tr_start_time(self):
            """Read-only property"""
            return self.__tr_start_time


        @property
        def tr_uuid(self):
            """Read-only property"""
            return self.__tr_uuid


        @property
        def tr_src_uuid(self):
            """Read-only property"""
            return self.__tr_src_uuid


        @property
        def tr_dst_uuid(self):
            """Read-only property"""
            return self.__tr_dst_uuid


        def __str__(self):
            """The default C{__str__} implementation for C{State} class.

            If you are creating any children and want to override
            this implementation, do it and do B{not} call
            C{super}-implementation!
            """
            return (u'tr_type={self.tr_type!r}, '
                     'tr_start_time={self.tr_start_time!r}, '
                     'tr_uuid={self.tr_uuid!r}, '
                     'tr_src_uuid={self.tr_src_uuid!r}, '
                     'tr_dst_uuid={self.tr_dst_uuid!r}'
                        .format(super=super(AbstractTransaction.State, self)
                                          .__str__(),
                                self=self))


    @contract_epydoc
    def __init__(self, manager, message,
                 parent=None,
                 start_time=None, max_duration=None):
        """Constructor.

        @note: at this moment, the transaction state is NOT yet initialized,
            so you cannot use C{self.open_state()} in any C{__init__()} method
            of a transaction.

        @param message: the request message that invoked this transaction.
        @type message: AbstractMessage

        @param parent: Parent transaction (if any)
        @type parent: AbstractTransaction, NoneType

        @param start_time: transaction start time; datetime.utcnow() if absent.
        @type start_time: datetime, NoneType

        @param max_duration: transaction expected duration, must be positive;
                             AbstractTransaction.DEFAULT_DURATION if absent.
        @type max_duration: timedelta, NoneType
        @precondition: max_duration is None or max_duration > timedelta(0)
                       # max_duration
        """
        self.manager = manager

        self._waiting = False

        self.message = message
        self.message_ack = None

        self.parent = parent

        self.children_completed = []
        self.children_in_progress = set()

        if start_time is not None:
            self.start_time = start_time
        else:
            self.start_time = datetime.utcnow()

        self.max_duration = coalesce(max_duration,
                                     AbstractTransaction.DEFAULT_DURATION)

        self.completed = Deferred()
        self.failure = None
        self.destroying_lock = threading.Lock()
        self.destroying = False


    def __str__(self):
        return u'{uuid} [{ch_inpr:d}/{ch_comp:d}]{if_fail}' \
               u'{opt_waiting}{in_transfer}{in_transfer_ack}'\
               u'{direction}'.format(
            uuid=self.uuid,
            ch_inpr=len(self.children_in_progress),
            ch_comp=len(self.children_completed),
            if_fail=u' FAIL({})'.format(self.failure.getErrorMessage())
                        if self.failure
                        else '',
            opt_waiting=' _waiting' if self._waiting else '',
            in_transfer=' IO' if self.message is not None and
                                 self.message.in_transfer
                              else '',
            in_transfer_ack=' IO ACK' if self.message_ack is not None and
                                         self.message_ack.in_transfer
                                      else '',
            direction=' OUT' if self.is_outgoing(deep_check=False) else ' IN'
        )


    @property
    def type(self):
        """Get a type of the transaction/messsage.

        @note: should actually be a C{@classpropery} if such entity type
            ever exists.

        @rtype: basestring
        """
        return self.msg_class.name


    @abstractmethod
    def on_begin(self, msg):
        """B{Must} be implemented in every subclass.

        This must be a method processing the beginning of the transaction.

        @returns: either nothing, or a C{Deferred} which runs its callback
            when the transaction startup actions really completed.
        @rtype: NoneType, Deferred
        """
        pass


    def on_child_end(self, child_state):
        """B{May} be implemented in every subclass.

        If implemented, should contain the code which is called whenever any
        child transaction (i.e. a transaction executed from inside the C{self}
        one) is completed.

        At the moment o

        @note: if overriding in a subclass, you may ignore calling the C{super}
            method (i.e. this one). Actually, it is suggest to not call the
            C{super(...).on_child_end()}, cause the existing implementation
            is made specifically to spot the transactions which has forgot
            to implement their real C{on_child_end()}-s.
                That is, if you see the warning log like below, you should
            visit the violating transaction and create a proper
            C{.on_child_end} implementation.

        @param child_state: the state of the child transaction.
        @type child_state: AbstractTransaction.State

        @returns: either nothing, or a C{Deferred} which runs its callback
            when the actions really completed.
        @rtype: NoneType, Deferred
        """
        logger.warning('Probably missing implementation for '
                           '%r.on_child_end(%r): ',
                       self, child_state)


    @abstractmethod
    def on_end(self, msg):
        """B{Must} be implemented in every subclass.

        This must be a method processing the end of the transaction.

        @returns: either nothing, or a C{Deferred} which runs its callback
            when the transaction finalization actions really completed.
        @rtype: NoneType, Deferred
        """
        pass


    @property
    def uuid(self):
        return self.message.uuid


    def __eq__(self, other):
        r"""Equality comparison.

        Two transactions are considered equal if they both have the same uuid.
        In fact, there should never be two different objects of the same
        transaction.

        >>> class MSG(object):
        ...     def __init__(self, uuid, in_transfer=False):
        ...         self.uuid = uuid; self.in_transfer = in_transfer

        >>> class Tr(AbstractTransaction):
        ...     def is_incoming(self, deep_check=False): pass
        ...     def is_outgoing(self, deep_check=False): pass
        ...     def on_begin(self): pass
        ...     def on_end(self): pass

        >>> t1a = Tr(manager=None,
        ...          message=MSG(UUID('e8c5944f-6d5f-4c58-b52c-66dac06cfbc8')),
        ...          start_time=datetime(2013, 1, 4, 16, 6, 27, 180731))
        >>> t1b = Tr(manager=None,
        ...          message=MSG(UUID('e8c5944f-6d5f-4c58-b52c-66dac06cfbc8')),
        ...          start_time=datetime(2013, 1, 4, 16, 6, 40, 867504))
        >>> t2a = Tr(manager=None,
        ...          message=MSG(UUID('fbeaf930-2df2-47d2-bec3-45da59a81f60')),
        ...          start_time=datetime(2013, 1, 4, 16, 6, 27, 180731))
        >>> t2b = Tr(manager=None,
        ...          message=MSG(UUID('fbeaf930-2df2-47d2-bec3-45da59a81f60')),
        ...          start_time=datetime(2013, 1, 4, 16, 6, 40, 867504))

        >>> # Reflectivity
        >>> t1a == t1a, t1a != t1a
        (True, False)

        >>> # Basic comparison
        >>> t1a == t2a, t1a == t2b, t1a != t2a, t1a != t2b
        (False, False, True, True)

        >>> # Same transaction UUID should not ever present
        >>> # in different transaction classes!
        >>> t1a == t1b  # doctest:+NORMALIZE_WHITESPACE
        Traceback (most recent call last):
          ...
        AssertionError: UUIDs must be equal only if IDs are equal:
            Tr(e8c5944f-6d5f-4c58-b52c-66dac06cfbc8 [0/0] IN),
            Tr(e8c5944f-6d5f-4c58-b52c-66dac06cfbc8 [0/0] IN)
        >>> t1a != t1b  # doctest:+NORMALIZE_WHITESPACE
        Traceback (most recent call last):
          ...
        AssertionError: UUIDs must be equal only if IDs are equal:
            Tr(e8c5944f-6d5f-4c58-b52c-66dac06cfbc8 [0/0] IN),
            Tr(e8c5944f-6d5f-4c58-b52c-66dac06cfbc8 [0/0] IN)

        >>> # Finally, make sure noone compares transactions with apples.
        >>> t1a == 42
        Traceback (most recent call last):
          ...
        TypeError: other is 42
        >>> t1a != 42
        Traceback (most recent call last):
          ...
        TypeError: other is 42
        >>> # ... and vice versa
        >>> 42 == t1a
        Traceback (most recent call last):
          ...
        TypeError: other is 42
        >>> 42 != t1a
        Traceback (most recent call last):
          ...
        TypeError: other is 42

        @type other: AbstractTransaction

        @rtype: bool
        """
        if isinstance(other, AbstractTransaction):
            # If UUIDs are equal, it should be the same transaction object
            assert (id(self) == id(other)) == (self.uuid == other.uuid), \
                   u'UUIDs must be equal only if IDs are equal: {!r}, {!r}' \
                       .format(self, other)
            return self.uuid == other.uuid
        else:
            # We should've return NotImplemented here;
            # but raise TypeError instead, to be sure that noone compares
            # Transactions with everything else.
            raise TypeError(u'other is {!r}'.format(other))


    def __le__(self, other):
        r"""Ordering comparison.

        The transaction C{A} is considered "less than" other transaction C{B}
        if C{A.start_time < B.start_time}.
        Otherwise, it is sorted by UUID (to be sorted at least somehow).

        >>> class MSG(object):
        ...     def __init__(self, uuid, in_transfer=False):
        ...         self.uuid = uuid; self.in_transfer = in_transfer

        >>> class Tr(AbstractTransaction):
        ...     def is_incoming(self, deep_check=False): pass
        ...     def is_outgoing(self, deep_check=False): pass
        ...     def on_begin(self): pass
        ...     def on_end(self): pass

        >>> t1a = Tr(manager=None,
        ...          message=MSG(UUID('e8c5944f-6d5f-4c58-b52c-66dac06cfbc8')),
        ...          start_time=datetime(2013, 1, 4, 16, 6, 27, 180731))
        >>> t1b = Tr(manager=None,
        ...          message=MSG(UUID('e8c5944f-6d5f-4c58-b52c-66dac06cfbc8')),
        ...          start_time=datetime(2013, 1, 4, 16, 6, 40, 867504))
        >>> t2a = Tr(manager=None,
        ...          message=MSG(UUID('fbeaf930-2df2-47d2-bec3-45da59a81f60')),
        ...          start_time=datetime(2013, 1, 4, 16, 6, 27, 180731))
        >>> t2b = Tr(manager=None,
        ...          message=MSG(UUID('fbeaf930-2df2-47d2-bec3-45da59a81f60')),
        ...          start_time=datetime(2013, 1, 4, 16, 6, 40, 867504))

        >>> # Same transaction UUID should not ever present
        >>> # in different transaction classes!
        >>> t1a < t1b  # doctest:+NORMALIZE_WHITESPACE
        Traceback (most recent call last):
          ...
        AssertionError: UUIDs must be equal only if IDs are equal:
            Tr(e8c5944f-6d5f-4c58-b52c-66dac06cfbc8 [0/0] IN),
            Tr(e8c5944f-6d5f-4c58-b52c-66dac06cfbc8 [0/0] IN)
        >>> t1a <= t1b  # doctest:+NORMALIZE_WHITESPACE
        Traceback (most recent call last):
          ...
        AssertionError: UUIDs must be equal only if IDs are equal:
            Tr(e8c5944f-6d5f-4c58-b52c-66dac06cfbc8 [0/0] IN),
            Tr(e8c5944f-6d5f-4c58-b52c-66dac06cfbc8 [0/0] IN)
        >>> t1a > t1b  # doctest:+NORMALIZE_WHITESPACE
        Traceback (most recent call last):
          ...
        AssertionError: UUIDs must be equal only if IDs are equal:
            Tr(e8c5944f-6d5f-4c58-b52c-66dac06cfbc8 [0/0] IN),
            Tr(e8c5944f-6d5f-4c58-b52c-66dac06cfbc8 [0/0] IN)
        >>> t1a >= t1b  # doctest:+NORMALIZE_WHITESPACE
        Traceback (most recent call last):
          ...
        AssertionError: UUIDs must be equal only if IDs are equal:
            Tr(e8c5944f-6d5f-4c58-b52c-66dac06cfbc8 [0/0] IN),
            Tr(e8c5944f-6d5f-4c58-b52c-66dac06cfbc8 [0/0] IN)

        >>> # Now, real comparisons...
        >>> t1a < t2a, t1a < t2b, t1a > t2a, t1a > t2b
        (True, True, False, False)
        >>> # ... and vice versa, to ensure antisymmetricity.
        >>> # There were some problems with it before...
        >>> t2a < t1a, t2b < t1a, t2a > t1a, t2b > t1a
        (False, False, True, True)
        >>> # And the same, but with ≤/≥.
        >>> t1a <= t2a, t1a <= t2b, t1a >= t2a, t1a >= t2b
        (True, True, False, False)
        >>> t2a <= t1a, t2b <= t1a, t2a >= t1a, t2b >= t1a
        (False, False, True, True)

        >>> # Compare with apples?
        >>> t1a < 42
        Traceback (most recent call last):
          ...
        TypeError: other is 42
        >>> t1a <= 42
        Traceback (most recent call last):
          ...
        TypeError: other is 42
        >>> t1a > 42
        Traceback (most recent call last):
          ...
        TypeError: other is 42
        >>> t1a >= 42
        Traceback (most recent call last):
          ...
        TypeError: other is 42
        >>> # ... and vice versa
        >>> 42 < t1a
        Traceback (most recent call last):
          ...
        TypeError: other is 42
        >>> 42 <= t1a
        Traceback (most recent call last):
          ...
        TypeError: other is 42
        >>> 42 > t1a
        Traceback (most recent call last):
          ...
        TypeError: other is 42
        >>> 42 >= t1a
        Traceback (most recent call last):
          ...
        TypeError: other is 42

        @type other: AbstractTransaction

        @rtype: bool
        """
        return (self == other or
                self.start_time < other.start_time or
                self.uuid < other.uuid)


    def __hash__(self):
        """For use in hashable collections."""
        return hash(self.uuid)


    def open_state(self, for_update=False):
        """Open the transaction state payload for this transaction.

        @param for_update: whether the state is retrieved with the intention
            of update it and write back to the storage;
            the default value should likely be C{False}.
        @type for_update: bool

        @raise KeyError: from C{self.manager.open_tr_state()}.

        @returns: the state-retrieving context manager.
        @rtype: AbstractTransactionManager.StateContextManager

        @postcondition: result is None or result.msg_class = self.msg_class
        """
        return self.manager.open_tr_state(tr_uuid=self.uuid,
                                          for_update=for_update)


    def max_end_time(self):
        """
        @return: the expected time of transaction completion.
        @rtype: datetime.datetime
        """
        return self.start_time + self.max_duration


    def pause(self):
        """
        Pause the transaction, so that it doesn't move to the ACK step
        automatically.
        """
        logger_transaction.debug(u'▮▮ %r', self)
        self._waiting = True


    def unpause(self, try_destroy):
        """Unpause the transaction, and go to the ACK step automatically.

        @param try_destroy: Whether unpausing the transaction should causes
            the automatic attempt to destroy the transaction.
            Default is C{True}.
            Makes sense to do it automatically in case if
            we are unpausing an outgoing transaction, but don't do it
            if we are unpausing a previously paused incoming transaction.
        @type try_destroy: bool
        """
        logger_transaction.debug(u'► %r', self)
        self._waiting = False
        if try_destroy:
            self.manager.try_destroy_transaction(self)


    def fail(self, failure):
        """Mark the transaction failed.

        The loggers in it do not generate error-level logs,
        as the caller should have generated one already.

        @param failure: Failure object
        @type failure: Failure
        """
        logger_transaction.debug(u'☒ %r', self)

        assert isinstance(failure, Failure), repr(failure)
        if self.failure is not None:
            logger_transaction.debug('The transaction %r was already failed '
                                         'with %r, but failing again with %r.',
                                     self, self.failure, failure)
        else:
            self.failure = failure

        #self.unpause(try_destroy = True)
        self.manager.destroy_transaction_unquestionnably(self)


    @abstractmethod
    def is_incoming(self, deep_check=True):
        """C{is_incoming()} method must be implemented in every subclass.

        This must be a method which executes the actual verification
        whether transaction is incoming to this peer.

        @return: Whether the transaction is incoming to this peer.
        @rtype: bool
        """
        pass


    @abstractmethod
    def is_outgoing(self, deep_check=True):
        """C{is_outgoing()} method must be implemented in every subclass.

        This must be a method which executes the actual verification
        whether transaction is outgoing from this peer.

        @return: Whether the transaction is outgoing from this peer.
        @rtype: bool
        """
        pass


    def is_incoming_deep_check(self, me, is_incoming_result):
        assert is_incoming_result == \
                   (not self.is_outgoing(deep_check=False)), \
               (self.message.src, self.message.dst, me)


    def is_outgoing_deep_check(self, me, is_outgoing_result):
        assert is_outgoing_result == \
                   (not self.is_incoming(deep_check=False)), \
               (self.message.src, self.message.dst, me)



@antisymmetric_comparison
@total_ordering
class AbstractInhabitant(IPrintable):
    """Any system inhabitant, be that a server, a node, or a host.

    In some rare cases, it can even be used by itself.
    """
    __slots__ = ('uuid', 'urls')


    @contract_epydoc
    def __init__(self, uuid, urls=None, *args, **kwargs):
        """Constructor.

        @type uuid: UUID
        @type urls: NoneType, list
        """
        assert not args and not kwargs, (args, kwargs)
        super(AbstractInhabitant, self).__init__(*args, **kwargs)

        self.uuid = PeerUUID.safe_cast_uuid(uuid if uuid is not None
                                                 else gen_uuid())

        self.urls = coalesce(urls, [])
        assert (consists_of(self.urls, basestring) and
                all(url.startswith('https://') for url in self.urls)), \
               repr(self.urls)


    def __str__(self):
        return u'uuid={self.uuid!r}{opt_urls}'.format(
                   self=self,
                   opt_urls=u', urls={!r}'.format(self.urls) if self.urls
                                                             else '')


    def __eq__(self, other):
        r"""Equality comparison.

        Two inhabitants are considered equal if they both have the same uuid.
        As the inhabitants objects are stored in the DB and being
        occasionally re-read, in fact there MIGHT be two different objects
        of the same inhabitant.

        >>> class Inh(AbstractInhabitant):
        ...     def dst_auth_realm(self): return 'INH'

        >>> i1 = Inh(UUID('e8c5944f-6d5f-4c58-b52c-66dac06cfbc8'))
        >>> i1a = Inh(UUID('e8c5944f-6d5f-4c58-b52c-66dac06cfbc8'))
        >>> i2 = Inh(UUID('fbeaf930-2df2-47d2-bec3-45da59a81f60'))

        >>> # Does "=" and "≠" work?
        >>> i1 == i1, i1 == i1a, i1 == i2, i1 != i1, i1 != i1a, i1 != i2
        (True, True, False, False, False, True)

        >>> # # Compare with apples? --- debug and analyze!
        >>> # i1 == 2, i1 != 42
        >>> # (False, True)

        @type other: AbstractInhabitant

        @rtype: bool
        """
        return self.uuid == other.uuid
                   # NotImplemented together with @antisymmetric_comparison
                   # needs more testing...
                   # if isinstance(other, AbstractInhabitant) \
                   # else NotImplemented


    def __lt__(self, other):
        r"""Ordering comparison.

        >>> class Inh(AbstractInhabitant):
        ...     def dst_auth_realm(self): return 'INH'

        >>> i1 = Inh(UUID('e8c5944f-6d5f-4c58-b52c-66dac06cfbc8'))
        >>> i1a = Inh(UUID('e8c5944f-6d5f-4c58-b52c-66dac06cfbc8'))
        >>> i2 = Inh(UUID('fbeaf930-2df2-47d2-bec3-45da59a81f60'))

        >>> # Test "<" and "≤"
        >>> i1 < i1, i1 <= i1, i1 < i1a, i1 <= i1a
        (False, True, False, True)
        >>> i1 < i2, i1 <= i2, i2 < i1, i2 <= i1
        (True, True, False, False)

        >>> # Thanks to @total_ordering, test also ">" and "≥"
        >>> i1 > i1, i1 >= i1, i1 > i1a, i1 >= i1a
        (False, True, False, True)
        >>> i1 > i2, i1 >= i2, i2 > i1, i2 >= i1
        (False, False, True, True)

        >>> # Compare with apples? -- needs more testing together with
        >>> # @antisymmetric_comparison
        >>> # i1 < 2, i1 <= 3, i1 > 4, i1 > 5
        >>> # (False, False, True, True)
        >>> # ... and vice versa
        >>> # 2 < i1, 3 <= i1, 4 > i1, 5 > i1
        >>> # (True, True, False, False)

        @type other: AbstractInhabitant

        @rtype: bool
        """
        return self.uuid < other.uuid \
                   if isinstance(other, AbstractInhabitant) \
                   else NotImplemented


    def __hash__(self):
        """
        For use in hashable collections.
        """
        return hash(self.uuid)


    @abstractproperty
    def dst_auth_realm(self):
        """Override in any child!

        @returns: The HTTP authentication realm for incoming connections to
                  this inhabitant.
        @rtype: str
        """
        return None
