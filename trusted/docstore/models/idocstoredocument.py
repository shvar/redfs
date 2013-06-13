#!/usr/bin/python
"""Mandatory interfaces for the docstore documents."""

#
# Imports
#

from __future__ import absolute_import
from functools import partial

from abc import ABCMeta

from common.abstractions import IPrintable

from ._common import BSONValidatableMixin



#
# Classes
#

class IDocStoreAnyDocument(IPrintable, BSONValidatableMixin):
    """
    The interface for every DocStore document, no matter if top level
    or subdocument.
    """

    __metaclass__ = ABCMeta

    # Cannot use __slots__ in case of multiple inheritance
    # __slots__ = ('_id',)


    def __str__(self):
        """
        Override any other C{__str__} definition.
        """
        return u''



class IDocStoreTopDocument(IDocStoreAnyDocument):
    """The interface for every top-level DocStore document.

    @ivar _id: the optional field (may be C{None}) containing the internal _id
        of the document in the DocStore.
    @type _id: bson.objectid.ObjectId, NoneType
    """

    # Cannot use __slots__ due to multiple inheritance

    def __init__(self, _id=None, *args, **kwargs):
        r"""Constructor.

        >>> import bson
        >>> IDocStoreTopDocument(
        ...     bson.objectid.ObjectId('505c933dd18ab66186000000'))
        IDocStoreTopDocument(_id=ObjectId('505c933dd18ab66186000000'))
        """
        super(IDocStoreTopDocument, self).__init__(*args, **kwargs)
        self._id = _id


    def __str__(self):
        return u'{super!s}_id={self._id!r}'.format(
                   super=super(IDocStoreTopDocument, self).__str__(),
                   self=self)


    @classmethod
    def from_bson(cls, doc):
        return partial(super(IDocStoreTopDocument, cls).from_bson(doc),
                       _id=doc.get('_id'))


    def to_bson(self):
        doc = super(IDocStoreTopDocument, self).to_bson()

        if self._id is not None:
            doc.update({'_id': self._id})

        return doc
