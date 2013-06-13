#!/usr/bin/python
"""
The FastDB model for the authentication token (for the web interface).
"""

#
# Imports
#

from __future__ import absolute_import
from datetime import datetime
from functools import partial
from uuid import UUID

from contrib.dbc import contract_epydoc

from common import datatypes
from common.abstractions import IPrintable

from trusted.ibsonable import IBSONable

from .idocstoredocument import IDocStoreTopDocument



#
# Classes
#

class AuthToken(IDocStoreTopDocument, IBSONable, datatypes.AuthToken):
    """
    Authentication token, for the purpose of login-less authentication from the
    GUI (which is logged in to the cloud already) by opening the web browser.
    """

    __slots__ = ()

    bson_schema = {
        'ts': datetime,
        'uuid': UUID,
    }


    def __repr__(self):
        return IPrintable.__repr__(self)


    def __str__(self):
        return u'{idocstore_str}, {datatypes_str}'.format(
                   idocstore_str=IDocStoreTopDocument.__str__(self),
                   datatypes_str=datatypes.AuthToken.__str__(self))


    def to_bson(self):
        r"""
        >>> AuthToken(ts=datetime(2012, 9, 21, 15, 45, 2, 478074),
        ...           uuid=UUID('030f417b-c1dd-41d1-837c-847e7ab886a2')
        ... ).to_bson()  # doctest:+NORMALIZE_WHITESPACE
        {'ts': datetime.datetime(2012, 9, 21, 15, 45, 2, 478074),
         'uuid': UUID('030f417b-c1dd-41d1-837c-847e7ab886a2')}
        """
        cls = self.__class__

        doc = super(AuthToken, self).to_bson()

        doc.update({
            'ts': self.ts,
            'uuid': self.uuid,
        })

        assert cls.validate_schema(doc), repr(doc)

        return doc


    @classmethod
    def from_bson(cls, doc):
        r"""
        >>> AuthToken.from_bson({
        ...     'ts': datetime(2012, 9, 21, 15, 45, 2, 478074),
        ...     'uuid': UUID('030f417b-c1dd-41d1-837c-847e7ab886a2')
        ... })()  # doctest:+NORMALIZE_WHITESPACE
        AuthToken(_id=None,
            ts=datetime.datetime(2012, 9, 21, 15, 45, 2, 478074),
            uuid=UUID('030f417b-c1dd-41d1-837c-847e7ab886a2'))
        """
        assert cls.validate_schema(doc), repr(doc)

        return partial(super(AuthToken, cls).from_bson(doc),
                       ts=doc['ts'],
                       uuid=doc['uuid'])


    @classmethod
    def from_common(cls, common_auth_token):
        r"""
        Create an AuthToken model from the more common implementation,
        C{datatypes.AuthToken}.

        >>> common_token = \
        ...     datatypes.AuthToken(
        ...         ts=datetime(2012, 9, 21, 15, 45, 2, 478074),
        ...         uuid=UUID('030f417b-c1dd-41d1-837c-847e7ab886a2'))
        >>> AuthToken.from_common(common_token) # doctest:+NORMALIZE_WHITESPACE
        AuthToken(_id=None,
            ts=datetime.datetime(2012, 9, 21, 15, 45, 2, 478074),
            uuid=UUID('030f417b-c1dd-41d1-837c-847e7ab886a2'))

        @type common_auth_token: datatypes.AuthToken
        @rtype: AuthToken
        """
        return cls(ts=common_auth_token.ts,
                   uuid=common_auth_token.uuid)
