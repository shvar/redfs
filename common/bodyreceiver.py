#!/usr/bin/python
"""
C{BodyReceiver} interface implementations.
"""

#
# Imports
#

from __future__ import absolute_import

import cStringIO as StringIO
import logging
from abc import ABCMeta
from types import NoneType

from twisted.internet.defer import Deferred
from twisted.web.server import Request

from contrib.dbc import contract_epydoc

from .abstractions import AbstractMessage
from .utils import exceptions_logged, in_main_thread


#
# Logging
#

logger = logging.getLogger(__name__)



#
# Classes
#

class AbstractBodyReceiver(object):
    """
    The main interface for using any body receiver is to subscribe
    to the on_finish Deferred attribute.

    All childs have access to the "semi-protected" field named _body,
    which is a C{StringIO.OutputType} variable being filled
    by the body contents while it is uploaded.

    @ivar on_finish: Deferred callback which is executed when the body
        is received.
    @type on_finish: Deferred

    @ivar done: Whether the body receive process is completed already.
    @type done: bool

    @invariant: isinstance(self._body, StringIO.OutputType)
    """

    __metaclass__ = ABCMeta


    @contract_epydoc
    def __init__(self, message, on_finish=None, *args, **kwargs):
        """Constructor.

        @type message: AbstractMessage
        @type on_finish: NoneType, Deferred
        """
        super(AbstractBodyReceiver, self).__init__(*args, **kwargs)
        self.message = message
        self.on_finish = on_finish if on_finish is not None \
                                   else Deferred()
        self.done = False
        self._body = StringIO.StringIO()


    @exceptions_logged(logger)
    def _body_received(self):
        """
        This method should be called as soon as possible
        after the body is received, to parse the body contents.
        So it cannot be just added to the C{self.on_finish} via C{addCallback},
        as C{on_finish} may already have some earlier callback
        that could be invoked before the body is parsed.
        """
        self.done = True

        data = self._body.getvalue()
        self._body.close()

        assert not in_main_thread()  # init_from_body may take long
        self.message.init_from_body(data)
        self.on_finish.callback(data)


    @exceptions_logged(logger)
    def _request_finished(self):
        """
        @todo: May be not None if some error occured during the request
            uploading.
        """
        logger.debug('REQUEST BODY FINISHED for %r!', self.message)
        self._body_received()



class RequestBodyReceiver(AbstractBodyReceiver):
    """
    Basic HTTP request body receiver.
    """

    @contract_epydoc
    def __init__(self, request, *args, **kwargs):
        """Constructor.

        @todo: in fact, we should consider the request finished
               when its data size reaches the one mentioned in the header.

        @type request: Request
        """
        super(RequestBodyReceiver, self).__init__(*args, **kwargs)

        # In request, the body is received already.
        _rc = request.content
        if isinstance(_rc, StringIO.OutputType):
            try:
                data = _rc.getvalue()
            except ValueError:
                # We may get "ValueError: I/O operation on closed file" here
                logger.error('Reading from the closed file')
                data = ''
        else:
            _rc.seek(0)
            data = _rc.read()

        assert data, (request, data)
        self._body.write(data)

        self._request_finished()

        #request.notifyFinish().addCallback(self._request_finished)



class ResponseBodyReceiver(AbstractBodyReceiver):
    """Basic HTTP response body receiver.

    When the HTTP response is received, all the data is already downloaded,
    so the HTTP response body receiver just fills the data and causes
    the C{.on_finish} callback be called immediately.
    """

    @exceptions_logged(logger)
    def deliver_response_body(self, body):
        """
        At the moment of response body receiver assignment,
        in fact the body is already received fully.
        """
        self._body.write(body)
        self._request_finished()
