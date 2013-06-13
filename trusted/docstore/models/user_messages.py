#!/usr/bin/python
"""
The FastDB model for the message (together with its translations)
between the Node and the Host.
"""

#
# Imports
#

from __future__ import absolute_import
from datetime import datetime
from functools import partial

from contrib.dbc import consists_of, contract_epydoc

from common import datatypes

from trusted.ibsonable import IBSONable

from .idocstoredocument import IDocStoreTopDocument



#
# Classes
#

class UserMessage(IDocStoreTopDocument, IBSONable, datatypes.UserMessage):
    """
    A message (together with its translations) between the Node and the Host.

    @note: username may mismatch the original one casewise.
    """

    __slots__ = ()

    bson_schema = {
        'username_uc': basestring,
        'key': basestring,
        'ts': datetime,
        'body': lambda body: consists_of(body.iterkeys(), basestring) and
                             consists_of(body.itervalues(), basestring)
    }


    @property
    def username_uc(self):
        return self.username.upper()


    def to_bson(self):
        cls = self.__class__

        doc = super(UserMessage, self).to_bson()

        doc.update({
            'username_uc': self.username_uc,
            'key': self.key,
            'ts': self.ts,
            'body': self.body,
        })

        assert cls.validate_schema(doc), repr(doc)

        return doc


    @classmethod
    def from_bson(cls, doc):
        assert cls.validate_schema(doc), repr(doc)

        return partial(super(UserMessage, cls).from_bson(doc),
                       username=doc['username_uc'],
                       key=doc['key'],
                       ts=doc['ts'],
                       body=doc['body'])


    @classmethod
    @contract_epydoc
    def from_common(cls, common_message):
        """
        Create an UserMessage model from the more common implementation,
        C{datatypes.UserMessage}.

        @type common_message: datatypes.UserMessage
        @rtype: UserMessage
        """
        return cls(username=common_message.username,
                   key=common_message.key,
                   ts=common_message.ts,
                   body=common_message.body)
