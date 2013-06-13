#!/usr/bin/python
"""
Various classes representing the networking components of the cloud, such as
the Node or the Host.
"""

#
# Imports
#

from __future__ import absolute_import
import base64
import collections as col
from datetime import datetime
from functools import partial
from types import NoneType
from uuid import UUID

from contrib.dbc import contract_epydoc

from . import settings
from .abstractions import AbstractInhabitant, IJSONable, IPrintable
from .crypto import AES_KEY_SIZE
from .typed_uuids import PeerUUID, UserGroupUUID
from .utils import antisymmetric_comparison, coalesce



#
# Classes
#

class UserGroup(IPrintable, IJSONable):
    """
    Any user group in the backup system.
    """
    __slots__ = ('uuid', 'name', 'private', 'enc_key')


    @contract_epydoc
    def __init__(self, uuid, name, private, enc_key):
        """
        @type uuid: UserGroupUUID

        @type name: str

        @type private: bool

        @type enc_key: str, buffer, NoneType
        @precondition: enc_key is None or len(enc_key) == AES_KEY_SIZE
        """
        self.uuid = uuid
        self.name = name
        self.private = private
        self.enc_key = str(enc_key)


    def __str__(self):
        return u'uuid={uuid!r}, name={name!r}, ' \
               u'private={private!r}, enc_key={enc_key!r}' \
                   .format(uuid=self.uuid,
                           name=self.name,
                           private=self.private,
                           enc_key=self.enc_key)


    def to_json(self):
        result = {'uuid': self.uuid.hex,
                  'name': self.name,
                  'private': 1 if self.private else 0}
        if self.enc_key is not None:
            result['enc_key'] = base64.b64encode(self.enc_key)
        return result


    @classmethod
    def from_json(cls, json_struct):
        return partial(super(UserGroup, cls).from_json(json_struct),
                       uuid=UserGroupUUID(json_struct['uuid']),
                       name=str(json_struct['name']),
                       private=bool(json_struct['private']),
                       enc_key=base64.b64decode(json_struct.get('enc_key')))



@antisymmetric_comparison
class User(IPrintable):
    """
    Any user of the backup system.
    """
    __slots__ = ('name', 'digest')


    @contract_epydoc
    def __init__(self, name, digest):
        """
        @param name: the username of the user; must be in ASCII-only for now.
        @type name: str

        @type digest: NoneType, str
        @precondition: digest is None or len(digest) == 40
        """
        self.name = name
        self.digest = digest


    def __hash__(self):
        return hash(self.name)


    def __eq__(self, other):
        assert isinstance(other, User), repr(other)
        if __debug__:
            if self.name == other.name:
                assert self.digest == other.digest, \
                       (self.digest, other.digest)

        return self.name == other.name


    def __str__(self):
        return u'name={name!r}, digest={digest!r}'.format(name=self.name,
                                                          digest=self.digest)



class UserWithGroups(User):
    """
    Any user of the backup system, with groups bound.
    """

    __slots__ = ('base_group', 'groups')


    @contract_epydoc
    def __init__(self, base_group, groups, *args, **kwargs):
        """
        @type base_group: UserGroup
        @type groups: col.Iterable
        """
        super(UserWithGroups, self).__init__(*args, **kwargs)
        self.base_group = base_group
        self.groups = groups


    def __str__(self):
        return u'base_group={!r}, groups={!r}, {}'\
                   .format(self.base_group,
                           self.groups,
                           super(UserWithGroups, self).__str__())



class Host(AbstractInhabitant):
    """
    Any host in the backup system.
    """
    __slots__ = ('name', 'user')


    @contract_epydoc
    def __init__(self, name=None, user=None, *args, **kwargs):
        """
        @param name: the name of the host; may be in Unicode.
        @type name: NoneType, basestring

        @type user: NoneType, User
        """
        super(Host, self).__init__(*args, **kwargs)
        self.name = name
        self.user = user


    def __str__(self):
        return u'{super}{opt_name}{opt_user}' \
                   .format(opt_name='' if self.name is None
                                       else u', name={!r}'.format(self.name),
                           opt_user='' if self.user is None
                                       else u', user={!r}'.format(self.user),
                           super=super(Host, self).__str__())


    @property
    def dst_auth_realm(self):
        return settings.HTTP_AUTH_REALM_HOST



class Node(AbstractInhabitant):
    """
    Any node in the backup system.
    """

    @property
    def dst_auth_realm(self):
        return settings.HTTP_AUTH_REALM_NODE



class HQ(AbstractInhabitant):
    """
    Any HQ server in the backup system.
    """

    @property
    def dst_auth_realm(self):
        raise NotImplementedError()



class HostAtNode(Host):
    """
    Any host, with the data interesting only for the node.
    """
    __slots__ = ('last_seen',)


    def __init__(self, last_seen=None, *args, **kwargs):
        assert last_seen is None or isinstance(last_seen, datetime), \
               last_seen

        super(HostAtNode, self).__init__(*args, **kwargs)
        self.last_seen = coalesce(last_seen, datetime.min)


    def __str__(self):
        return u'{super}{opt_last_seen}'.format(
                   super=super(HostAtNode, self).__str__(),
                   opt_last_seen='' if self.last_seen == datetime.min
                                    else ', last_seen={!r}, '
                                             .format(self.last_seen))



class TrustedHostAtNode(HostAtNode):
    """
    Trusted host, with the data interesting only for the node.
    """
    __slots__ = ('for_storage', 'for_restore',)


    def __init__(self, for_storage=None, for_restore=None, *args, **kwargs):
        assert isinstance(for_storage, bool) and isinstance(for_restore, bool)

        super(TrustedHostAtNode, self).__init__(*args, **kwargs)
        self.for_storage = for_storage
        self.for_restore = for_restore


    def __str__(self):
        return u'for_storage={!r}, for_restore={!r}, {super}' \
                   .format(self.for_storage, self.for_restore,
                           super=super(HostAtNode, self).__str__())
