#!/usr/bin/python
"""
The FastDB model for the link used to share files and also for the
referral program.
"""

#
# Imports
#

from __future__ import absolute_import
import numbers
from datetime import datetime
from functools import partial
from mimetypes import guess_type
from types import NoneType
from uuid import UUID

from contrib.dbc import contract_epydoc

from common.abstractions import IPrintable
from common.utils import coalesce

from trusted.ibsonable import IBSONable

from .idocstoredocument import IDocStoreTopDocument



#
# Classes
#

class MagnetLink(IDocStoreTopDocument, IBSONable):
    """
    A link to share files and also for referral program.
    """

    __slots__ = ('code', 'username_uc', 'ds_uuid', 'basedirectory_path',
                 'rel_path', 'active', 'blocked', 'viewsN', 'downloadsN',
                 'created_ts', 'last_view_ts', 'last_download_ts', 'mime')

    bson_schema = {
        'code': basestring,
        'username_uc': basestring,
        'ds_uuid': UUID,
        'basedirectory_path': basestring,
        'rel_path': basestring,
        # This fields has default values. We don't need to change in normal way
        'active': bool,
        'viewsN': numbers.Integral,
        'downloadsN': numbers.Integral,
        'created_ts': datetime,
        # Optional fields
        'blocked': (bool, NoneType),
        'last_view_ts': (datetime, NoneType),
        'last_download_ts': (datetime, NoneType),
        'mime': (basestring, NoneType),
    }


    @contract_epydoc
    def __init__(self,
                 code, username_uc, ds_uuid, basedirectory_path, rel_path,
                 active=True, blocked=False, viewsN=0, downloadsN=0,
                 created_ts=None, last_view_ts=None, last_download_ts=None,
                 mime=None,
                 *args, **kwargs):
        r"""Constructor.

        >>> # No optional arguments
        >>> MagnetLink(code='jDd6r',
        ...            username_uc='JOHNDOE@WHITEHOUSE.GOV',
        ...            ds_uuid=UUID('766796e9-6f08-456d-806b-a24bc01f180c'),
        ...            basedirectory_path='C:/Windows/System32/drivers',
        ...            rel_path='etc/hosts',
        ... )  # doctest:+NORMALIZE_WHITESPACE +ELLIPSIS
        MagnetLink(_id=None,
                   username_uc="JOHNDOE@WHITEHOUSE.GOV",
                   ds_uuid=UUID('766796e9-6f08-456d-806b-a24bc01f180c'),
                   basedirectory_path="C:/Windows/System32/drivers",
                   rel_path="etc/hosts",
                   active=True,
                   viewsN=0,
                   downloadsN=0,
                   created_ts=datetime.datetime(...))

        >>> # All optional arguents
        >>> MagnetLink(
        ...     code='KzFp3',
        ...     username_uc='NICK@BLACKHOUSE.COM',
        ...     ds_uuid=UUID('ac349f0e-3c16-7832-acfe-c4357ac18c57'),
        ...     basedirectory_path='/home/username/my_folder',
        ...     rel_path='photos/avatar.png',
        ...     active=False,
        ...     viewsN=3,
        ...     downloadsN=1,
        ...     created_ts=datetime(2012, 1, 1, 1, 1, 1, 111111),
        ...     blocked=True,
        ...     last_view_ts=datetime(2012, 2, 2, 2, 2, 2, 222222),
        ...     last_download_ts=datetime(2012, 3, 3, 3, 3, 3, 333333),
        ...     mime="test/mimetype"
        ... )  # doctest:+NORMALIZE_WHITESPACE
        MagnetLink(_id=None,
                   username_uc="NICK@BLACKHOUSE.COM",
                   ds_uuid=UUID('ac349f0e-3c16-7832-acfe-c4357ac18c57'),
                   basedirectory_path="/home/username/my_folder",
                   rel_path="photos/avatar.png",
                   active=False,
                   viewsN=3,
                   downloadsN=1,
                   created_ts=datetime.datetime(2012, 1, 1, 1, 1, 1, 111111),
                   blocked=True,
                   last_view_ts=datetime.datetime(2012, 2, 2, 2, 2, 2, 222222),
                   last_download_ts=datetime.datetime(2012, 3, 3,
                                                      3, 3, 3, 333333),
                   mime="test/mimetype")

        @param code: the code of magnet link
        @type code: basestring

        @param username_uc: the name of the user.
        @type username_uc: basestring

        @param ds_uuid: the UUID of the Host.
        @type ds_uuid: UUID

        @param basedirectory_path: the path of base_directory. Is using to
            find unique file.
        @type basedirectory_path: basestring

        @param rel_path: the rel_path of file_local.
        @type rel_path: basestring

        @param active: is magnetlink is active or not.
        @type active: bool

        @param viewsN: count of views specific magnet link.
        @type viewsN: numbers.Integral

        @param downloadsN: count of downloads file of magnet link.
        @type downloadsN: numbers.Integral

        @type created_ts: datetime, NoneType

        @param blocked: is magnetlink blocked by administrator or not.
        @type blocked: bool

        @type last_view_ts: datetime, NoneType
        @type last_download_ts: datetime, NoneType

        @param mime: mimetype of the file.
        @type mime: basestring, NoneType
        """
        super(MagnetLink, self).__init__(*args, **kwargs)
        self.code = code
        self.username_uc = username_uc
        self.ds_uuid = ds_uuid
        self.basedirectory_path = basedirectory_path
        self.rel_path = rel_path
        # By the way we need to be able to set other values but default.
        # I don't know for what, but I think so.
        self.active = active
        self.viewsN = viewsN
        self.downloadsN = downloadsN
        self.created_ts = coalesce(created_ts, datetime.utcnow())
        # Optional fields
        self.blocked = blocked
        self.last_view_ts = last_view_ts
        self.last_download_ts = last_download_ts
        self.mime = coalesce(mime, guess_type(rel_path)[0])


    def __repr__(self):
        return IPrintable.__repr__(self)


    def __str__(self):
        return (
            '{idocstore_str}, '
            'username_uc="{self.username_uc}", '
            'ds_uuid={self.ds_uuid!r}, '
            'basedirectory_path="{self.basedirectory_path}", '
            'rel_path="{self.rel_path}", '
            'active={self.active}, '
            'viewsN={self.viewsN}, '
            'downloadsN={self.downloadsN}, '
            'created_ts={self.created_ts!r}'
            '{opt_blocked}'
            '{opt_last_view_ts}'
            '{opt_last_download_ts}'
            '{opt_mime}'.format(
                idocstore_str=IDocStoreTopDocument.__str__(self),
                self=self,
                opt_blocked = '' if not self.blocked
                                 else ', blocked={}'.format(self.blocked),
                opt_last_view_ts = '' if self.last_view_ts is None
                                      else ', last_view_ts={!r}'.format(
                                               self.last_view_ts),
                opt_last_download_ts = '' if self.last_download_ts is None
                                          else ', last_download_ts={!r}'.format(
                                                   self.last_download_ts),
                opt_mime = '' if self.mime is None
                              else ', mime="{}"'.format(self.mime)))


    def to_bson(self):
        r"""
        >>> # No optional arguments
        >>> MagnetLink(code='jDd6r',
        ...            username_uc='JOHNDOE@WHITEHOUSE.GOV',
        ...            ds_uuid=UUID('766796e9-6f08-456d-806b-a24bc01f180c'),
        ...            basedirectory_path='C:/Windows/System32/drivers',
        ...            rel_path='etc/hosts',
        ... ).to_bson()  # doctest:+NORMALIZE_WHITESPACE +ELLIPSIS
        {'created_ts': datetime.datetime(...),
         'code': 'jDd6r',
         'ds_uuid': UUID('766796e9-6f08-456d-806b-a24bc01f180c'),
         'username_uc': 'JOHNDOE@WHITEHOUSE.GOV',
         'downloadsN': 0,
         'viewsN': 0,
         'basedirectory_path':
         'C:/Windows/System32/drivers',
         'rel_path': 'etc/hosts',
         'active': True}

        >>> # All optional arguents
        >>> MagnetLink(
        ...     code='KzFp3',
        ...     username_uc='NICK@BLACKHOUSE.COM',
        ...     ds_uuid=UUID('ac349f0e-3c16-7832-acfe-c4357ac18c57'),
        ...     basedirectory_path='/home/username/my_folder',
        ...     rel_path='photos/avatar.png',
        ...     active=False,
        ...     viewsN=3,
        ...     downloadsN=1,
        ...     created_ts=datetime(2012, 1, 1, 1, 1, 1, 111111),
        ...     blocked=True,
        ...     last_view_ts=datetime(2012, 2, 2, 2, 2, 2, 222222),
        ...     last_download_ts=datetime(2012, 3, 3, 3, 3, 3, 333333),
        ...     mime="test/mimetype"
        ... ).to_bson()  # doctest:+NORMALIZE_WHITESPACE
        {'created_ts': datetime.datetime(2012, 1, 1, 1, 1, 1, 111111),
         'code': 'KzFp3',
         'ds_uuid': UUID('ac349f0e-3c16-7832-acfe-c4357ac18c57'),
         'username_uc': 'NICK@BLACKHOUSE.COM',
         'downloadsN': 1,
         'viewsN': 3,
         'basedirectory_path': '/home/username/my_folder',
         'mime': 'test/mimetype',
         'rel_path': 'photos/avatar.png',
         'last_download_ts': datetime.datetime(2012, 3, 3, 3, 3, 3, 333333),
         'active': False,
         'last_view_ts': datetime.datetime(2012, 2, 2, 2, 2, 2, 222222),
         'blocked': True}
        """
        cls = self.__class__

        doc = super(MagnetLink, self).to_bson()

        doc.update({
            'code': self.code,
            'username_uc': self.username_uc,
            'ds_uuid': self.ds_uuid,
            'basedirectory_path': self.basedirectory_path,
            'rel_path': self.rel_path,
            'active': self.active,
            'viewsN': self.viewsN,
            'downloadsN': self.downloadsN,
            'created_ts': self.created_ts,
        })

        # Optional fields
        if self.blocked:
            doc.update({'blocked': self.blocked})
        if self.last_view_ts:
            doc.update({'last_view_ts': self.last_view_ts})
        if self.last_download_ts:
            doc.update({'last_download_ts': self.last_download_ts})

        if self.mime:
            doc.update({'mime': self.mime})

        assert cls.validate_schema(doc), repr(doc)

        return doc


    @classmethod
    def from_bson(cls, doc):
        r"""
        >>> # No optional arguments
        >>> MagnetLink.from_bson({
        ...     'code': 'jDd6r',
        ...     'username_uc': 'JOHNDOE@WHITEHOUSE.GOV',
        ...     'ds_uuid': UUID('766796e9-6f08-456d-806b-a24bc01f180c'),
        ...     'basedirectory_path': 'C:/Windows/System32/drivers',
        ...     'rel_path': 'etc/hosts',
        ...     'active': True,
        ...     'viewsN': 0,
        ...     'downloadsN': 0,
        ...     'created_ts': datetime(2012, 1, 1, 1, 1, 1, 111111),
        ... })()  # doctest:+NORMALIZE_WHITESPACE
        MagnetLink(_id=None,
                   username_uc="JOHNDOE@WHITEHOUSE.GOV",
                   ds_uuid=UUID('766796e9-6f08-456d-806b-a24bc01f180c'),
                   basedirectory_path="C:/Windows/System32/drivers",
                   rel_path="etc/hosts",
                   active=True,
                   viewsN=0,
                   downloadsN=0,
                   created_ts=datetime.datetime(2012, 1, 1, 1, 1, 1, 111111))

        >>> # All optional arguents
        >>> MagnetLink.from_bson({
        ...     'code': 'KzFp3',
        ...     'username_uc': 'NICK@BLACKHOUSE.COM',
        ...     'ds_uuid': UUID('ac349f0e-3c16-7832-acfe-c4357ac18c57'),
        ...     'basedirectory_path': '/home/username/my_folder',
        ...     'rel_path': 'photos/avatar.png',
        ...     'active': False,
        ...     'viewsN': 3,
        ...     'downloadsN': 1,
        ...     'created_ts': datetime(2012, 1, 1, 1, 1, 1, 111111),
        ...     'blocked': True,
        ...     'last_view_ts': datetime(2012, 2, 2, 2, 2, 2, 222222),
        ...     'last_download_ts': datetime(2012, 3, 3, 3, 3, 3, 333333),
        ...     'mime': "test/mimetype"
        ... })()  # doctest:+NORMALIZE_WHITESPACE
        MagnetLink(_id=None,
                   username_uc="NICK@BLACKHOUSE.COM",
                   ds_uuid=UUID('ac349f0e-3c16-7832-acfe-c4357ac18c57'),
                   basedirectory_path="/home/username/my_folder",
                   rel_path="photos/avatar.png",
                   active=False,
                   viewsN=3,
                   downloadsN=1,
                   created_ts=datetime.datetime(2012, 1, 1, 1, 1, 1, 111111),
                   blocked=True,
                   last_view_ts=datetime.datetime(2012, 2, 2, 2, 2, 2, 222222),
                   last_download_ts=datetime.datetime(2012, 3, 3,
                                                      3, 3, 3, 333333),
                   mime="test/mimetype")
        """
        assert cls.validate_schema(doc), repr(doc)

        return partial(super(MagnetLink, cls).from_bson(doc),
                       code=doc['code'],
                       username_uc=str(doc['username_uc']),
                       ds_uuid=doc['ds_uuid'],
                       basedirectory_path=doc['basedirectory_path'],
                       rel_path=doc['rel_path'],
                       active=doc['active'],
                       viewsN=doc['viewsN'],
                       downloadsN=doc['downloadsN'],
                       created_ts=doc['created_ts'],
                       # Optional fields
                       blocked=doc.get('blocked', False),
                       last_view_ts=doc.get('last_view_ts'),
                       last_download_ts=doc.get('last_download_ts'),
                       mime=doc.get('mime'))


    def to_webjson(self, dt_format):
        r"""Convert to a presentation usable for AJAX transportation.

        >>> # No optional arguments
        >>> MagnetLink(
        ...     code='jDd6r',
        ...     username_uc='JOHNDOE@WHITEHOUSE.GOV',
        ...     ds_uuid=UUID('766796e9-6f08-456d-806b-a24bc01f180c'),
        ...     basedirectory_path='C:/Windows/System32/drivers',
        ...     rel_path='etc/hosts',
        ... ).to_webjson('%Y-%m-%dT%H:%M:%SZ')\
        ...     # doctest:+NORMALIZE_WHITESPACE +ELLIPSIS
        {'basedirectory_path': 'C:/Windows/System32/drivers',
         'active': True,
         'created_ts': '...-...-...T...:...:...Z',
         'code': 'jDd6r',
         'ds_uuid': '766796e96f08456d806ba24bc01f180c',
         'rel_path': 'etc/hosts',
         'username_uc': 'JOHNDOE@WHITEHOUSE.GOV',
         'downloadsN': 0,
         'viewsN': 0}

        >>> # All optional arguents
        >>> MagnetLink(
        ...     code='KzFp3',
        ...     username_uc='NICK@BLACKHOUSE.COM',
        ...     ds_uuid=UUID('ac349f0e-3c16-7832-acfe-c4357ac18c57'),
        ...     basedirectory_path='/home/username/my_folder',
        ...     rel_path='photos/avatar.png',
        ...     active=False,
        ...     viewsN=3,
        ...     downloadsN=1,
        ...     created_ts=datetime(2012, 1, 1, 1, 1, 1, 111111),
        ...     blocked=True,
        ...     last_view_ts=datetime(2012, 2, 2, 2, 2, 2, 222222),
        ...     last_download_ts=datetime(2012, 3, 3, 3, 3, 3, 333333),
        ...     mime="test/mimetype"
        ... ).to_webjson('%Y-%m-%dT%H:%M:%SZ')  # doctest:+NORMALIZE_WHITESPACE
        {'code': 'KzFp3',
         'last_view_ts': '2012-02-02T02:02:02Z',
         'username_uc': 'NICK@BLACKHOUSE.COM',
         'downloadsN': 1,
         'viewsN': 3,
         'mime': 'test/mimetype',
         'rel_path': 'photos/avatar.png',
         'active': False,
         'last_download_ts': '2012-03-03T03:03:03Z',
         'blocked': True,
         'created_ts': '2012-01-01T01:01:01Z',
         'ds_uuid': 'ac349f0e3c167832acfec4357ac18c57',
         'basedirectory_path': '/home/username/my_folder'}

        @param dt_format: a format for C{.strftime()} method, which is needed
            to represent the datetime object for the AJAX.
        @type dt_format: basestring
        """
        ml_webjson = {'code': self.code,
                      'username_uc': self.username_uc,
                      'ds_uuid': self.ds_uuid.hex,
                      'basedirectory_path': self.basedirectory_path,
                      'rel_path': self.rel_path,
                      'created_ts': self.created_ts.strftime(dt_format),
                      'viewsN': self.viewsN,
                      'downloadsN': self.downloadsN,
                      'active': self.active}

        if self.last_view_ts:
            ml_webjson['last_view_ts'] = self.last_view_ts.strftime(dt_format)
        if self.last_download_ts:
            ml_webjson['last_download_ts'] = self.last_download_ts\
                                                 .strftime(dt_format)
        if self.blocked:
            ml_webjson['blocked'] = self.blocked
        if self.mime:
            ml_webjson['mime'] = self.mime
        return ml_webjson
