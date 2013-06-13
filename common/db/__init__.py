#!/usr/bin/python
"""
Common module for database access.
"""

#
# Imports
#

from __future__ import absolute_import
import collections as col
import gc
import json
import logging
import numbers
import os
import posixpath
import re
import sqlite3
import struct
import sys
from abc import ABCMeta
from base64 import b64decode, b64encode
from collections import defaultdict
from datetime import datetime, timedelta, tzinfo
from functools import partial
from itertools import chain, imap, ifilter, izip, tee
from textwrap import dedent as _dd
from threading import Lock, RLock
from types import NoneType
from uuid import UUID

from OpenSSL import crypto

import sqlalchemy
from sqlalchemy import event, func, orm
from sqlalchemy.sql.expression import (
    select as sql_select, update as sql_update,
    insert as sql_insert, delete as sql_delete,
    bindparam)

try:
    import psycopg2
    import psycopg2.extras
except ImportError:
    # We need psycopg2 only if going to use it
    psycopg2 = None

from contrib.dbc import consists_of, contract_epydoc

from common import constants, limits, typed_uuids
from common.abstract_dataset import AbstractDataset, AbstractBasicDatasetInfo
from common.abstractions import IPrintable, AbstractInhabitant
from common.chunks import Chunk, ChunkInfo, LocalPhysicalFileState
from common.crypto import Fingerprint, AES_KEY_SIZE
from common.datasets import DatasetWithAggrInfo
from common.datatypes import AutoUpdateSettings, LogReportingSettings
from common.datetime_ex import TimeEx, TimeDeltaEx
from common.inhabitants import Host, Node, User, UserGroup, UserWithGroups
from common.itertools_ex import iunzip
from common.path_ex import (
    encode_posix_path, decode_posix_path, get_intermediate_paths_posix)
from common.typed_uuids import (
    ChunkUUID, DatasetUUID, FileUUID, HostUUID, NodeUUID, UserGroupUUID)
from common.utils import (
    coalesce, duration_logged, strptime, strftime, exceptions_logged,
    gen_uuid, pformat, fix_strptime)

from . import models, hex_convert
from .types import crc32_to_signed, crc32_to_unsigned



#
# Logging and other constants
#

logger = logging.getLogger(__name__)
logger_db = logging.getLogger('common.db_wrapper')
logger_db_retries = logging.getLogger('common.db_retried_wrapper')

DEFAULT_DB_TIMEOUT = 60  # seconds



#
# Functions
#

@contract_epydoc
def uuid_adapt_sqlite(uuid):
    """
    Use as::

      sqlite3.register_adapter(UUID, uuid_adapt_sqlite)

    >>> uuid_adapt_sqlite(UUID('73e5bce5-99e9-4165-9d0f-8c8369b3433f'))
    '73e5bce599e941659d0f8c8369b3433f'

    @type uuid: UUID
    """
    return uuid.hex


@contract_epydoc
def uuid_convert_sqlite(value):
    """
    Use as::

      sqlite3.register_converter('uuid', uuid_convert_sqlite)

    >>> uuid_convert_sqlite('73e5bce599e941659d0f8c8369b3433f')
    UUID('73e5bce5-99e9-4165-9d0f-8c8369b3433f')

    @type value: basestring
    @precondition: len(value) == 32 # value
    """
    return UUID(value)


@contract_epydoc
def crc32_convert(value):
    """
    Use as::

      sqlite3.register_converter('crc32', crc32_convert)

    >>> crc32_convert('-2005302914')
    -2005302914

    @type value: basestring
    @rtype: int
    """
    return int(value)


@contract_epydoc
def _unpack_path_settings(packed_settings):
    """
    Given the path-specific settings from the DB, unpack them
    and make all the settings which probably were not stored.

    @type packed_settings: dict
    @rtype: dict
    """
    # For now, only "f+" (inclusive filter) and f- (exclusive filter)
    # settings are supported.
    assert set(packed_settings.iterkeys()) <= set(('f+', 'f-')), \
           repr(packed_settings)

    # Make a copy
    new_settings = dict(packed_settings)

    if 'f+' not in new_settings:
        new_settings['f+'] = []

    if 'f-' not in new_settings:
        new_settings['f-'] = []

    return new_settings


@contract_epydoc
def _pack_path_settings(path_settings):
    """
    Given all the path-specific settings, pack it into a more compact form
    to store in the DB.

    @type path_settings: dict
    @rtype: dict
    """
    # For now, only "f+" (inclusive filter) and "f-" (exclusive filter)
    # settings are supported.
    assert set(path_settings.iterkeys()) <= set(('f+', 'f-')), \
           repr(path_settings)

    # Make a copy
    new_settings = dict(path_settings)

    # "all" filter, if exists, is all-covering, and no other filters is needed.
    _pack_filters = lambda filters: (['all'] if 'all' in filters
                                             else filters)

    if 'f+' in new_settings:
        new_filters = _pack_filters(new_settings['f+'])
        if not new_filters:
            del new_settings['f+']

    if 'f-' in new_settings:
        new_filters = _pack_filters(new_settings['f-'])
        if not new_filters:
            del new_settings['f-']

    return new_settings


_SCHEDULE_ITEM_KEYS = frozenset(('uuid',
                                 'name',
                                 'period',
                                 'next_backup_datetime',
                                 'paths'))


@contract_epydoc
def serialize_backup_schedule_item(item):
    """
    For every single record in the schedule, serialize it to the string.

    @type item: dict
    @precondition: set(item.iterkeys()) == _SCHEDULE_ITEM_KEYS # item
    """
    return {'uuid': item['uuid'].hex,
            'name': item['name'],
            'period': item['period'],
            'next_backup_datetime': strftime(item['next_backup_datetime']),
            'paths': {encode_posix_path(k): _pack_path_settings(v)
                          for k, v in item['paths'].iteritems()}}


@contract_epydoc
def serialize_backup_schedule(schedule):
    """
    Given a schedule data (as a list of schedule items as dicts),
    return its serialized (to string) representation.

    @type schedule: list
    @precondition: consists_of(schedule, dict) # schedule
    @rtype: basestring
    """
    return json.dumps(map(serialize_backup_schedule_item,
                          schedule),
                      sort_keys=__debug__)


@contract_epydoc
def deserialize_backup_schedule_item(ser):
    """
    For every single record in the schedule, deserialize it from the string.

    @type ser: dict
    @precondition: set(ser.iterkeys()) == _SCHEDULE_ITEM_KEYS # ser
    """
    return {'uuid': UUID(ser['uuid']),
            'name': ser['name'],
            'period': ser['period'],
            'next_backup_datetime': strptime(ser['next_backup_datetime']),
            'paths': {decode_posix_path(k): _unpack_path_settings(v)
                          for k, v in ser['paths'].iteritems()}}


@contract_epydoc
def deserialize_backup_schedule(ser):
    """
    Given a serialized (to string) schedule data,
    return its deserialized (to the list of schedule items as dicts)
    representation.

    @type ser: basestring
    @rtype: list
    """
    data = json.loads(ser)
    assert isinstance(data, list) and consists_of(data, dict), \
           repr(data)

    return map(deserialize_backup_schedule_item, data)


@contract_epydoc
def serialize_paths_to_backup(paths):
    """
    Given the data for paths to backup (as a dictionary),
    return its serialized (to string) representation.

    @type paths: dict
    @rtype: basestring
    """
    paths_packed = {encode_posix_path(k): _pack_path_settings(v)
                        for k, v in paths.iteritems()}

    return json.dumps(paths_packed, sort_keys=__debug__)


@contract_epydoc
def deserialize_paths_to_backup(ser):
    """
    Given a serialized (to string) data for paths to backup,
    return its deserialized (to the dictionary)
    representation.

    @type ser: basestring
    @rtype: dict
    """
    return {decode_posix_path(k): _unpack_path_settings(v)
                for k, v in json.loads(ser).iteritems()}



#
# Classes
#

class AbstractEncodedSettings(object):
    """
    An abstract class which doesn't do anything on its own but intended to be
    multiply-inherited by any other class which deals with settings,
    which are encoded on setting-name basis.

    Every subclass is expected to have the following class-level fields:

      * ALL_SETTINGS, storing the mapping of the setting names
        to their default values.
      * _SETTING_SERIALIZER, storing the mapping of the setting names
        to their specific types.
    """
    __metaclass__ = ABCMeta

    __slots__ = tuple()


    # See the class docstring for the details!
    # You must implement the proper class fields in any subclass.
    ALL_SETTINGS = None
    _SETTING_SERIALIZER = None

    # The following store type information come in tuples:
    # first goes the actual value serializer (that serializes
    # the "d"eserialized value),
    # second goes the serializer (that deserializes the "s"erialized value).

    _TYPE_BASESTRING = (
        basestring,
        lambda d: d,
        lambda s: s
    )
    _TYPE_BOOL = (
        bool,
        lambda d: '1' if d else '0',
        lambda s: bool(int(s))
    )
    _TYPE_BLOB = (
        buffer,
        buffer,
        lambda s: s
    )
    _TYPE_BLOB_HEX = (
        str,
        b64encode,
        b64decode
    )
    _TYPE_DATETIME = (
        datetime,
        strftime,
        strptime
    )
    _TYPE_FLOAT = (
        float,
        str,
        float
    )

    _TYPE_INT = (
        int,
        str,
        int
    )

    _TYPE_JSON_DICT = (
        dict,
        json.dumps,
        json.loads
    )
    _TYPE_JSON_LIST = (
        list,
        json.dumps,
        json.loads
    )
    _TYPE_UUID = (
        UUID,
        lambda d: d.hex,
        UUID
    )
    _TYPE_BACKUP_SCHEDULE = (
        list,
        serialize_backup_schedule,
        deserialize_backup_schedule
    )
    _TYPE_PATHS_TO_BACKUP = (
        dict,
        serialize_paths_to_backup,
        deserialize_paths_to_backup
    )
    _TYPE_SSL_PKEY = (
        crypto.PKey,
        lambda d: b64encode(crypto.dump_privatekey(crypto.FILETYPE_ASN1, d)),
        lambda s: crypto.load_privatekey(crypto.FILETYPE_ASN1,
                                         b64decode(s)) if s is not None
                                                       else None
    )
    _TYPE_SSL_CERT = (
        crypto.X509,
        lambda d: b64encode(crypto.dump_certificate(crypto.FILETYPE_ASN1, d)),
        lambda s: crypto.load_certificate(crypto.FILETYPE_ASN1,
                                          b64decode(s)) if s is not None
                                                        else None
    )
    _TYPE_LOG_REPORTING_SETTINGS = (
        LogReportingSettings,
        lambda d: json.dumps(d.to_json()),
        lambda s: LogReportingSettings.from_json(json.loads(s))()
    )
    _TYPE_AUTO_UPDATE_SETTINGS = (
        AutoUpdateSettings,
        lambda d: json.dumps({'update': 1 if d.update else 0}),
        lambda s: AutoUpdateSettings(update=bool(json.loads(s).get('update',
                                                                   True)))
    )


    @classmethod
    def _decode(cls, name, value):
        """
        Given a setting name and serializable value, decode the value
        to the native type; if needed, all necessary checks are done.
        """
        assert name in cls.ALL_SETTINGS, repr(name)

        type_, serializer, deserializer = cls._SETTING_SERIALIZER[name]
        if value is None:
            return None
        else:
            try:
                result = deserializer(value)
            except Exception:
                logger.exception('Cannot decode %r/%r, using defaults',
                                 name, value)
                return cls.ALL_SETTINGS[name]

            assert result is None or isinstance(result, type_), \
               u'For {!r}, result is {!r}/{!r} while should be {!r}'\
                   .format(name, result, type(result), type_)
            return result


    @classmethod
    def _encode(cls, name, value):
        """
        Given a setting name and value, encode the value
        to the storable type (string); if needed, all the necessary checks
        are done.
        """
        assert name in cls.ALL_SETTINGS, \
               (name, cls, cls.ALL_SETTINGS)

        type_, serializer, deserializer = cls._SETTING_SERIALIZER[name]
        default_value = cls.ALL_SETTINGS[name]

        # The value may be None only if the default value is None too:
        # if the default value is None, the value may be any,
        # but if the default value is not None, the value is not None too.
        assert default_value is None or value is not None, \
               (default_value, value)
        # But if the value is not None, it should be of a very specific type
        assert value is None or isinstance(value, type_), \
               'For {0!r}, value is {1!r}/{2!r} while should be /{3!r}'\
                   .format(name, value, type(value), type_)

        return None if value is None \
                    else serializer(value)


    @classmethod
    def get_default(cls, name, direct=False):
        """
        Get the default value for a setting by its name.
        """
        assert name in cls.ALL_SETTINGS, repr(name)
        value = cls.ALL_SETTINGS[name]

        return cls._encode(name, value) if direct \
                                        else value



class BasicDatasetInfoWith_IDs(AbstractBasicDatasetInfo):

    __slots__ = ('_id', '_ugroup_id')


    def __init__(self, _id, _ugroup_id, *args, **kwargs):
        """Constructor."""
        super(BasicDatasetInfoWith_IDs, self).__init__(*args, **kwargs)
        self._id = _id
        self._ugroup_id = _ugroup_id



class Queries(object):
    """A dummy namespace for all the common DB queries.

    @note: you do not ever need to instance this class.
    """
    __slots__ = tuple()


    class System(object):
        """
        All DB queries related to the various system administration operations.
        """

        __slots__ = tuple()


        MAINTENANCE_QUERIES = {
            'postgresql': ['VACUUM ANALYZE'],
            'sqlite': ['VACUUM', 'ANALYZE']
        }

        MAX_TIME_WITHOUT_MAINTENANCE = timedelta(days=1)


        @classmethod
        @contract_epydoc
        def maintenance(cls, rdbw, force=False):
            """
            Perform all the primary maintenance functions

            @type rdbw: DatabaseWrapperSQLAlchemy
            @param force: Force run maintenance
                          even if it is not a time for that.
            """
            now = datetime.utcnow()

            last_maint = Queries.LocalSettings.get_setting(
                             Queries.LocalSettings.LAST_DB_MAINTENANCE,
                             rdbw)

            next_maint = last_maint + cls.MAX_TIME_WITHOUT_MAINTENANCE

            # Did the earliest time of the next maintenance occur?
            if now < next_maint and not force:
                logger.debug('Now %s, next maintenance after %s'
                                 ', skipping maintenance',
                             now, next_maint)
            else:
                logger.debug('Running maintenance for %s!',
                             rdbw.rdb_url)

                Queries.LocalSettings.set_local_setting(
                    Queries.LocalSettings.LAST_DB_MAINTENANCE, now, rdbw)

                rdbw.session.commit()

                # Now let's go low-level...

                # First some preparations
                if rdbw.dialect_name == 'postgresql':
                    if psycopg2 is None:
                        raise Exception('Please install python-psycopg2!')

                    conn = rdbw.session.connection()
                    prev_isolation_level = \
                        conn.connection.connection.isolation_level
                    # Most likely, prev_isolation_level is
                    # ISOLATION_LEVEL_READ_COMMITTED

                    # Stop any transactions, we need to switch to AUTOCOMMIT
                    # level to run VACUUM and similar operations.
                    conn.connection.connection.set_isolation_level(
                        psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)

                try:
                    for q in cls.MAINTENANCE_QUERIES[rdbw.dialect_name]:
                        try:
                            rdbw.query(q)
                        except sqlite3.OperationalError as e:
                            # Having an SQL error during the maintenance
                            # is generall bad but not deadly,
                            # so report the error and go on.
                            logger.exception(
                                'Error during the %s operation: %s', q)
                finally:
                    if rdbw.dialect_name == 'postgresql':
                        conn.connection.connection.set_isolation_level(
                            prev_isolation_level)
                        # conn.close()
                logger.debug('Maintenance for %s completed',
                             rdbw.rdb_url)



    class Inhabitants(object):
        """All DB queries related to the cloud inhabitants (Nodes, Hosts, etc).
        """

        __slots__ = tuple()

        CREATE_VIEW_USER_IN_GROUP = _dd("""\
            CREATE VIEW user_in_group_view AS
                SELECT
                    ("group".id = "user".id) AS is_base,
                    "group".id AS group_id,
                    "group".uuid AS group_uuid,
                    "group".name AS group_name,
                    "group".private AS group_private,
                    "group".enc_key AS group_enc_key,
                    "user".id AS user_id,
                    "user".name AS user_name
                FROM
                    "group"
                    INNER JOIN membership
                        ON "group".id = membership.group_id
                    INNER JOIN "user"
                        ON membership.user_id = "user".id
            """)


        CREATE_HOST_VIEW = _dd("""\
            CREATE VIEW host_view AS
                SELECT
                    inhabitant.id AS id,
                    inhabitant.uuid AS uuid,
                    inhabitant.urls AS urls,
                    host.name AS name,
                    host.user_id AS user_id
                FROM
                    host
                    INNER JOIN inhabitant
                        USING (id)
            """)


        __CREATE_HOST_VIEW_INSERT_TRIGGER_SQLITE = _dd("""\
            CREATE TRIGGER host_view_insert_trigger
                INSTEAD OF INSERT ON host_view
                FOR EACH ROW
                BEGIN
                    INSERT OR IGNORE INTO inhabitant(uuid, urls)
                        VALUES (
                            new.uuid,
                            new.urls
                        );
                    INSERT OR ROLLBACK INTO host(id, name, user_id)
                        VALUES (
                            (SELECT id FROM inhabitant WHERE uuid = new.uuid),
                            new.name,
                            new.user_id
                        );
                END;
            """)

        __CREATE_HOST_VIEW_UPDATE_TRIGGER_SQLITE = _dd("""\
            CREATE TRIGGER IF NOT EXISTS host_view_update_trigger
                INSTEAD OF UPDATE ON host_view
                FOR EACH ROW
                BEGIN
                    UPDATE OR ROLLBACK inhabitant
                        SET
                            uuid = new.uuid,
                            urls = new.urls
                        WHERE id = old.id;
                    UPDATE OR ROLLBACK host
                        SET
                            name = new.name,
                            user_id = new.user_id
                        WHERE id = old.id;
                END;
            """)

        __CREATE_HOST_VIEW_DELETE_TRIGGER_SQLITE = _dd("""\
           CREATE TRIGGER IF NOT EXISTS host_view_delete_trigger
               INSTEAD OF DELETE ON host_view
               FOR EACH ROW
               BEGIN
                   DELETE FROM host WHERE id = old.id;
               END
            """)

        __CREATE_HOST_VIEW_TRIGGER_PROC_POSTGRESQL = _dd("""\
            CREATE OR REPLACE FUNCTION host_view_trigger_proc()
            RETURNS TRIGGER AS $$
            DECLARE
                _inhabitant_id inhabitant.id%TYPE;
            BEGIN
                IF (TG_OP = 'INSERT') THEN
                    -- INSERT OR IGNORE INTO inhabitant

                    SELECT id
                        FROM inhabitant
                        WHERE uuid = new.uuid
                        INTO _inhabitant_id;

                    IF NOT FOUND THEN
                        INSERT INTO inhabitant(uuid, urls)
                            VALUES (
                                new.uuid,
                                new.urls
                            )
                            RETURNING id INTO _inhabitant_id;
                    END IF;

                    INSERT INTO host(id, name, user_id)
                        VALUES (
                            _inhabitant_id,
                            new.name,
                            new.user_id
                        );

                    RETURN new;
                ELSIF (TG_OP = 'UPDATE') THEN
                    UPDATE inhabitant
                        SET
                            uuid = new.uuid,
                            urls = new.urls
                        WHERE id = old.id;

                    UPDATE host
                        SET
                            name = new.name,
                            user_id = new.user_id
                        WHERE id = old.id;

                    RETURN new;
                ELSIF (TG_OP = 'DELETE') THEN
                    DELETE FROM host WHERE id = old.id;

                    RETURN old;
                END IF;
            END;
            $$ LANGUAGE plpgsql
            """)

        __CREATE_HOST_VIEW_TRIGGER_POSTGRESQL = _dd("""\
            CREATE TRIGGER host_view_trigger
                INSTEAD OF INSERT OR UPDATE OR DELETE ON host_view
                FOR EACH ROW
                EXECUTE PROCEDURE host_view_trigger_proc()
            """)

        CREATE_HOST_VIEW_TRIGGERS = {
            'sqlite': [__CREATE_HOST_VIEW_INSERT_TRIGGER_SQLITE,
                       __CREATE_HOST_VIEW_UPDATE_TRIGGER_SQLITE,
                       __CREATE_HOST_VIEW_DELETE_TRIGGER_SQLITE],
            'postgresql': [__CREATE_HOST_VIEW_TRIGGER_PROC_POSTGRESQL,
                           __CREATE_HOST_VIEW_TRIGGER_POSTGRESQL]
        }


        CREATE_VIEW_USER_HOSTS = _dd("""\
            CREATE VIEW user_hosts_view AS
                SELECT
                    inhabitant.id AS host_id,
                    inhabitant.uuid AS host_uuid,
                    inhabitant.urls AS host_urls,
                    host.name AS host_name,
                    "user".id AS user_id,
                    "user".name AS user_name,
                    "user".digest AS user_digest
                FROM
                    inhabitant
                    INNER JOIN host
                        USING(id)
                    INNER JOIN "user"
                        ON host.user_id = "user".id
            """)


        INSERT_HOST = _dd("""\
            INSERT INTO host(id, user_id, name)
                VALUES (
                    :id,
                    (SELECT id
                         FROM "user"
                         WHERE upper("user".name) = upper(:user_name)),
                    :host_name
                )
            """)

        @classmethod
        @contract_epydoc
        def add_inhabitant(cls, uuid, urls, rdbw):
            """Add a basic inhabitant.

            @type uuid: UUID

            @returns: The id of the inserted inhabitant.
            """
            res = rdbw.execute(models.inhabitants.insert(),
                               {'uuid': uuid,
                                'urls': coalesce(urls, '[]')})
            assert res.rowcount == 1, res.rowcount
            return res.inserted_primary_key[0]


        @classmethod
        @contract_epydoc
        def add_ugroups(cls, groups, rdbw):
            """Add multiple user groups.

            @type groups: col.Iterable

            @type rdbw: DatabaseWrapperSQLAlchemy
            """
            groups = [{'uuid': group.uuid,
                       'name': group.name,
                       'private': group.private,
                       'enc_key': b64encode(group.enc_key)}
                          for group in groups]
            if groups:
                rdbw.execute(models.user_groups.insert(), groups)


        UPDATE_UGROUP = _dd("""\
            UPDATE "group"
                SET
                    name = :name,
                    private = :private,
                    enc_key = :enc_key
                WHERE uuid = :uuid
            """)

        @classmethod
        @contract_epydoc
        def update_ugroups(cls, groups, rdbw):
            """Update multiple user groups.

            @type groups: col.Iterable

            @type rdbw: DatabaseWrapperSQLAlchemy
            """
            groups_iter = ({'uuid': gr.uuid,
                            'name': gr.name,
                            'private': gr.private,
                            'enc_key': b64encode(gr.enc_key)}
                               for gr in groups)
            rdbw.querymany(cls.UPDATE_UGROUP, groups_iter)


        DELETE_UGROUP = _dd("""\
            DELETE FROM "group" WHERE uuid = :uuid
            """)

        @classmethod
        @contract_epydoc
        def delete_ugroups(cls, group_uuids, rdbw):
            """Delete multiple user groups.

            @type group_uuids: col.Iterable

            @type rdbw: DatabaseWrapperSQLAlchemy
            """
            args_iter = ({'uuid': u} for u in group_uuids)
            rdbw.querymany(cls.DELETE_UGROUP, args_iter)


        @classmethod
        @contract_epydoc
        def upsert_ugroup(cls, group, host_uuid, rdbw):
            """Insert or update the User Group information.

            @type group: UserGroup

            @param host_uuid: the UUID of the host (to validate the rights).
            @type host_uuid: UUID

            @type rdbw: DatabaseWrapperSQLAlchemy
            """
            row = cls.get_ugroup_by_uuid(group.uuid, rdbw)
            if row is None:
                # Add
                cls.add_ugroups([group], rdbw)
            else:
                # Update
                cls.update_ugroups([group], rdbw)


        @classmethod
        @contract_epydoc
        def add_user(cls, name, digest, rdbw):
            """Add a new user (with their own group).

            @type name: str

            @type digest: str
            @precondition: len(digest) == 40

            @type rdbw: DatabaseWrapperSQLAlchemy

            @returns: The user id of inserted user
            """
            m = models

            cl_group_id = sql_select([m.user_groups.c.id],
                                     m.user_groups.c.name == name)

            cl_insert_user = m.users.insert().values(user_id=cl_group_id,
                                                     name=name,
                                                     digest=digest)

            res = rdbw.execute(cl_insert_user)
            assert res.rowcount == 1, res.rowcount

            return res.inserted_primary_key[0]


        INSERT_MEMBERSHIP = _dd("""\
            INSERT INTO membership(user_id, group_id)
                VALUES (
                    (SELECT id
                         FROM "user"
                         WHERE upper(name) = upper(:user_name)),
                    (SELECT id
                         FROM "group"
                         WHERE uuid = :ugroup_uuid)
                )
            """)

        DELETE_MEMBERSHIP = _dd("""\
            DELETE FROM membership
                WHERE
                    user_id = (SELECT id
                                   FROM "user"
                                   WHERE upper(name) = upper(:user_name)) AND
                    group_id = (SELECT id
                                    FROM "group"
                                    WHERE uuid = :ugroup_uuid)
            """)

        @classmethod
        @contract_epydoc
        def add_user_to_group(cls, username, group_uuid, rdbw):
            """Add the user to the group (given by the Group UUID).

            @type username: str

            @type group_uuid: UserGroupUUID

            @type rdbw: DatabaseWrapperSQLAlchemy
            """
            if cls.is_user_in_group(username, group_uuid, rdbw):
                logger.debug('User %r is in group %s already',
                             username, group_uuid)
            else:
                rdbw.query(cls.INSERT_MEMBERSHIP,
                           {'user_name': username,
                            'ugroup_uuid': group_uuid})


        @classmethod
        @contract_epydoc
        def is_user_in_group(cls, username, group_uuid, rdbw):
            """
            Given a group UUID, check whether the user (given by the username)
            is present in the group already.
            """
            m = models

            cl_select_membership = \
                sql_select([m.memberships.c.id],
                           (func.upper(m.users.c.name) ==
                            func.upper(username)) &
                               (m.user_groups.c.uuid == group_uuid),
                           from_obj=[m.memberships
                                      .join(m.user_groups)
                                      .join(m.users,
                                            onclause=(m.users.c.user_id ==
                                                      m.memberships.c.user))])

            row = rdbw.execute(cl_select_membership).scalar()
            return row is not None


        @classmethod
        @contract_epydoc
        def add_user_with_group(cls, username, group, digest, rdbw):
            """
            Create a similarly-named group and user, and perform
            all the actions which are needed for adding a new user
            from scratch.

            @type username: str
            @type group: UserGroup

            @returns: The user id of inserted user
            """
            cls.add_ugroups([group], rdbw)
            user_id = cls.add_user(username, digest, rdbw)
            cls.add_user_to_group(username, group.uuid, rdbw)
            return user_id


        @classmethod
        @contract_epydoc
        def get_all_users(cls, rdbw):
            """Return all the users stored in the DB.

            @type rdbw: DatabaseWrapperSQLAlchemy

            @rtype: col.Iterable
            """
            m = models
            cl_select_all_users = sql_select([m.users.c.name,
                                              m.users.c.digest])

            return (User(name=str(row.name),
                         digest=str(row.digest))
                        for row in rdbw.execute(cl_select_all_users))


        SELECT_HOST_BY_UUID = _dd("""\
            SELECT id, uuid, urls, name
                FROM host_view
                WHERE uuid = :uuid
            """)

        @classmethod
        @contract_epydoc
        def get_host_by_uuid(cls, uuid, rdbw):
            """Return the host stored in the DB by its UUID.

            @type rdbw: DatabaseWrapperSQLAlchemy

            @rtype: Host, NoneType
            """
            host_view = models.Host.host_view().alias('host_view')
            cl_select_host = sql_select([host_view.c.uuid,
                                         host_view.c.urls,
                                         host_view.c.name],
                                        from_obj=host_view,
                                        whereclause=host_view.c.uuid ==
                                                        bindparam('uuid'))

            rows = rdbw.execute(cl_select_host, {'uuid': uuid})
            # There should be either no or a single result
            rows = list(rows)
            assert len(rows) in (0, 1), repr(rows)
            if rows:
                row = rows[0]
                return Host(uuid=HostUUID.safe_cast_uuid(row.uuid),
                            urls=json.loads(row.urls),
                            # Host-specific
                            name=str(row.name))
            else:
                return None


        SELECT_HOST_WITH_USER_BY_UUID = _dd("""\
            SELECT
                    host_uuid,
                    host_urls,
                    host_name,
                    user_name
                FROM user_hosts_view
                WHERE host_uuid = :uuid
            """)


        @classmethod
        @contract_epydoc
        def get_host_with_user_by_uuid(cls, uuid, rdbw):
            """
            Return the host stored in the DB by its UUID.
            The host will have the C{user} field initialized.

            @type rdbw: DatabaseWrapperSQLAlchemy

            @rtype: Host, NoneType
            """
            row = rdbw.query_value(cls.SELECT_HOST_WITH_USER_BY_UUID,
                                   {'uuid': uuid})
            if row is not None:
                user = cls.get_user_with_groups(str(row.user_name), rdbw)
                return Host(uuid=HostUUID.safe_cast_uuid(row.host_uuid),
                            urls=json.loads(row.host_urls),
                            # Host-specific
                            name=str(row.host_name),
                            user=user)
            else:
                logger.debug('No host found with UUID %s', uuid)
                return None


        SELECT_ALL_NODES = _dd("""\
            SELECT id, uuid, urls
                FROM
                    inhabitant
                    INNER JOIN node
                        USING (id)
            """)


        @classmethod
        @contract_epydoc
        def get_all_nodes(cls, rdbw):
            """
            Return all the peers stored in the DB.

            @type rdbw: DatabaseWrapperSQLAlchemy

            @rtype: col.Iterable
            @postcondition: consists_of(result, Host)
            """
            rows = rdbw.query(cls.SELECT_ALL_NODES)
            return (Node(uuid=NodeUUID.safe_cast_uuid(row.uuid),
                         urls=json.loads(row.urls))
                        for row in rows)


        # Arguments:
        # 1. host UUID
        # 2. host name
        # 3. host URLs
        INSERT_HOST_VIEW = _dd("""\
            INSERT INTO host_view(uuid, urls, name)
                VALUES (:uuid, :urls, :name)
            """)

        @classmethod
        @contract_epydoc
        def set_peers(cls, peers, rdbw):
            """
            Write the peers information into the DB, adding or overwriting it.

            @param peers: an iterable of Host objects
            @type peers: col.Iterable

            @type rdbw: DatabaseWrapperSQLAlchemy
            """
            for peer in peers:
                host = cls.get_host_by_uuid(peer.uuid, rdbw)
                if host is None:
                    rdbw.query(cls.INSERT_HOST_VIEW,
                               {'uuid': peer.uuid,
                                'urls': json.dumps(peer.urls),
                                'name': coalesce(peer.name, '')})


        UPDATE_USER = _dd("""\
            UPDATE "user"
                SET digest = :digest
                WHERE upper(name) = upper(:user_name)
            """)

        @classmethod
        @contract_epydoc
        def update_user_digest(cls, name, digest, rdbw):
            """
            @type name: str

            @type digest: str
            @precondition: len(digest) == 40

            @type rdbw: DatabaseWrapperSQLAlchemy

            @returns: The user id of inserted user
            """
            rdbw.query(cls.UPDATE_USER,
                       {'user_name': name, 'digest': digest})


        @classmethod
        def __db_ugroup_row_to_usergroup(cls, row):
            return UserGroup(uuid=UserGroupUUID.safe_cast_uuid(row.uuid),
                             name=str(row.name),
                             private=bool(row.private),
                             enc_key=b64decode(row.enc_key))


        # Arguments:
        # :ugroup_uuid
        # :host_uuid (to ensure the access rights)
        SELECT_GROUP_BY_UUID = _dd("""\
            SELECT id, uuid, name, private, enc_key
                FROM "group"
                WHERE uuid = :ugroup_uuid
            """)

        @classmethod
        @contract_epydoc
        def get_ugroup_by_uuid(cls, ugroup_uuid, rdbw):
            """Get the user group by its UUID.

            @type rdbw: DatabaseWrapperSQLAlchemy

            @returns: the user group (or None, if not available or accessible).
            @rtype: UserGroup, NoneType
            """
            row = rdbw.query_value(cls.SELECT_GROUP_BY_UUID,
                                   {'ugroup_uuid': ugroup_uuid})
            return None if row is None \
                        else cls.__db_ugroup_row_to_usergroup(row)


        SELECT_GROUPS_FOR_USER = _dd("""\
            SELECT
                    is_base,
                    group_uuid AS uuid,
                    group_name AS name,
                    group_private AS private,
                    group_enc_key AS enc_key
                FROM user_in_group_view
                WHERE upper(user_name) = upper(:user_name)
            """)

        @classmethod
        @contract_epydoc
        def get_groups_for_user(cls, username, rdbw):
            """Return all the groups for an existing user.

            @type rdbw: DatabaseWrapperSQLAlchemy

            @returns: a (possibly non-reiterable) sequence of UserGroup.
            @rtype: col.Iterable
            """
            rows = rdbw.query(cls.SELECT_GROUPS_FOR_USER,
                              {'user_name': username})

            return (cls.__db_ugroup_row_to_usergroup(row)
                        for row in rows)


        SELECT_USER_BY_NAME = _dd("""\
            SELECT
                    name, digest
                FROM "user"
                WHERE upper(name) = :user_name
            """)

        @classmethod
        @contract_epydoc
        def get_user_with_groups(cls, username, rdbw):
            """
            Given just the user name, return the User with its groups bound.
            It is a error if the user does not exist in the database!

            @type username: str

            @type rdbw: DatabaseWrapperSQLAlchemy

            @rtype: UserWithGroups
            """
            # Read the (yet unknown) user information
            user_row = rdbw.query_value(cls.SELECT_USER_BY_NAME,
                                        {'user_name': username.upper()})
            assert user_row is not None

            # Now read all the groups, and especially find the base group
            gr_rows = rdbw.query(cls.SELECT_GROUPS_FOR_USER,
                                 {'user_name': username})

            all_groups, are_base = \
                iunzip((UserGroup(uuid=UserGroupUUID.safe_cast_uuid(row.uuid),
                                  name=str(row.name),
                                  private=bool(row.private),
                                  enc_key=b64decode(row.enc_key)),
                        bool(row.is_base))
                           for row in gr_rows)
            groups = list(all_groups)
            base_groups = (gr
                               for gr, is_base in izip(groups, are_base)
                               if is_base)
            # Base groups must be an one and only.
            base_group = base_groups.next()
            # ... and ensure that.
            if __debug__:
                try:
                    base_groups.next()
                except StopIteration:
                    pass
                else:
                    assert False

            return UserWithGroups(  # User-specific
                                  name=str(user_row.name),
                                  digest=str(user_row.digest),
                                    # UserWithGroups-specific
                                  base_group=base_group,
                                  groups=groups)



    class LocalSettings(AbstractEncodedSettings):
        """
        All DB queries related to the settings specific only to the component
        where the code is being executed.
        """

        __slots__ = tuple()

        _AES = AbstractEncodedSettings

        DB_SCHEMA_VERSION = 'db schema version'
        LAST_DB_MAINTENANCE = 'last db maintenance'

        ALL_SETTINGS = {DB_SCHEMA_VERSION: constants.CURRENT_DB_VERSION,
                        LAST_DB_MAINTENANCE: datetime.min}

        _SETTING_SERIALIZER = defaultdict(
            lambda: AbstractEncodedSettings._TYPE_BASESTRING,
            {
                DB_SCHEMA_VERSION:   _AES._TYPE_DATETIME,
                LAST_DB_MAINTENANCE: _AES._TYPE_DATETIME,
            })


        SELECT_LOCAL_SETTING = _dd("""\
            SELECT value FROM local_setting WHERE name = :setting_name
            """)

        @classmethod
        @contract_epydoc
        def get_setting(cls, setting_name, rdbw):
            """
            Given a setting name, return its value
            (or a default value instead).

            @note: The value of the setting is decoded to the proper type
                   if needed.

            @precondition: setting_name in cls.ALL_SETTINGS # setting_name
            @type rdbw: DatabaseWrapperSQLAlchemy
            """
            row = rdbw.query_value(cls.SELECT_LOCAL_SETTING,
                                   {'setting_name': setting_name})

            return cls._decode(setting_name,
                               row.value) if row is not None \
                                          else cls.ALL_SETTINGS[setting_name]


        # Arguments:
        # :update_time
        # :setting_name
        # :setting_value
        INSERT_LOCAL_SETTING = _dd("""\
            INSERT INTO local_setting(update_time, name, value)
                VALUES (:update_time, :setting_name, :setting_value)
            """)

        UPDATE_LOCAL_SETTING = _dd("""\
            UPDATE local_setting
                SET update_time = :update_time, value = :setting_value
                WHERE name = :setting_name
            """)

        @classmethod
        @contract_epydoc
        def set_local_setting(cls,
                              setting_name, setting_value, rdbw, direct=False):
            """
            Set the local setting, no matter was any value present
            or absent for this setting before.

            @type setting_name: str
            @precondition: setting_name in Queries.LocalSettings.ALL_SETTINGS

            @type rdbw: DatabaseWrapperSQLAlchemy

            @param direct: whether the value should be written
                           without the conversion.
            @type direct: bool
            """
            update_time = datetime.utcnow()
            # Does the setting exist already?
            existing_row = rdbw.query_value(cls.SELECT_LOCAL_SETTING,
                                            {'setting_name': setting_name})
            # INSERT if it doesn't exist, UPDATE otherwise
            rdbw.query(cls.INSERT_LOCAL_SETTING if existing_row is None
                                                else cls.UPDATE_LOCAL_SETTING,
                       {'update_time': update_time,
                        'setting_name': setting_name,
                        'setting_value': setting_value
                                             if direct
                                             else cls._encode(setting_name,
                                                              setting_value)})



    class Settings(AbstractEncodedSettings):
        """All DB queries related to the various host settings.

        @cvar ALL_SETTINGS: The settings which are stored on the Node,
            and cached on the Host.
            It is the mapping from the setting name to the default value.
        @type ALL_SETTINGS: dict

        @cvar CRYPTO_KEY_PATH: the default (suggested) path to the
            chunk encryption key file.
            If None, the per-user encryption key is not used by default.
            If empty string, the user is requested to enter password manually
            to generate the per-user encryption key.
        """
        __slots__ = tuple()
        _AES = AbstractEncodedSettings

        MAX_STORAGE_SIZE_MIB = 'max storage size'  # in Mebibytes
        CHUNK_STORAGE_PATH = 'chunk storage path'
        HEARTBEAT_INTERVAL = 'hearbeat interval'  # in seconds
        NODE_URLS = 'node URLs'
        NODE_UUID = 'node UUID'
        GEEK_MODE = 'geek mode'
        MANUAL_ENCRYPTION_CONTROL = 'encryption control'
        PATHS_TO_BACKUP = 'paths to backup'
        PORT_TO_LISTEN = 'port to listen'
        LANGUAGE = 'language'
        FIRST_LAUNCH = 'first launch'
        CRYPTO_KEY_PATH = 'crypto key path'
        FILL_UNUSED_SPACE = 'fill unused space'
        TRAY_ON_GUI_CLOSE = 'tray on gui close'
        START_MINIMIZED = 'start minimized'
        BACKUP_SCHEDULE = 'backup schedule'
        TIMEZONE = 'timezone'
        LOG_SENDING_METHOD = 'log sending method'
        AUTO_UPDATE = 'auto update'

        # ALL_SETTINGS store the default value for every setting,
        # in its original non-serialized (!) form
        ALL_SETTINGS = {
            # Some setting defaults are initialized on runtime,
            # in host.settings::__init_host_db()
            MAX_STORAGE_SIZE_MIB:       1024,
            CHUNK_STORAGE_PATH:         None,  # on runtime
            HEARTBEAT_INTERVAL:         5.0,
            NODE_URLS:                  None,  # on runtime
            NODE_UUID:                  None,  # on runtime
            GEEK_MODE:                  False,
            MANUAL_ENCRYPTION_CONTROL:  False,
            PATHS_TO_BACKUP:            None,
            PORT_TO_LISTEN:             0xBACC,
            LANGUAGE:                   '',
            FIRST_LAUNCH:               True,
            CRYPTO_KEY_PATH:            None,
            FILL_UNUSED_SPACE:          True,
            TRAY_ON_GUI_CLOSE:          True,
            START_MINIMIZED:            False,
            BACKUP_SCHEDULE:            [],
            TIMEZONE:                   '',
            LOG_SENDING_METHOD:         LogReportingSettings(),
            AUTO_UPDATE:                AutoUpdateSettings(update=True),
        }


        # The mapping from each setting to the store type information.
        # In fact, this is relevant only on the host, as the node always treats
        # all the settings equally, as the strings.
        _SETTING_SERIALIZER = defaultdict(
            lambda: AbstractEncodedSettings._TYPE_BASESTRING,
            {
                MAX_STORAGE_SIZE_MIB:       _AES._TYPE_INT,
                CHUNK_STORAGE_PATH:         _AES._TYPE_BASESTRING,
                HEARTBEAT_INTERVAL:         _AES._TYPE_FLOAT,
                NODE_URLS:                  _AES._TYPE_JSON_LIST,
                NODE_UUID:                  _AES._TYPE_UUID,
                GEEK_MODE:                  _AES._TYPE_BOOL,
                MANUAL_ENCRYPTION_CONTROL:  _AES._TYPE_BOOL,
                PATHS_TO_BACKUP:            _AES._TYPE_PATHS_TO_BACKUP,
                PORT_TO_LISTEN:             _AES._TYPE_INT,
                FIRST_LAUNCH:               _AES._TYPE_BOOL,
                FILL_UNUSED_SPACE:          _AES._TYPE_BOOL,
                TRAY_ON_GUI_CLOSE:          _AES._TYPE_BOOL,
                START_MINIMIZED:            _AES._TYPE_BOOL,
                BACKUP_SCHEDULE:            _AES._TYPE_BACKUP_SCHEDULE,
                TIMEZONE:                   _AES._TYPE_BASESTRING,
                LOG_SENDING_METHOD:         _AES._TYPE_LOG_REPORTING_SETTINGS,
                AUTO_UPDATE:                _AES._TYPE_AUTO_UPDATE_SETTINGS,
            }
        )

        # For these settings, the Node value always takes precedence
        # over the Host one, and the Host even never should update
        # these values.
        SETTINGS_NODE_UPDATE_ONLY = frozenset((
        ))


        CREATE_VIEW_SETTING = _dd("""\
            CREATE VIEW setting_view AS
                SELECT
                    setting.id AS id,
                    setting_name.id AS setting_name_id,
                    inhabitant.id AS host_id,
                    inhabitant.uuid AS host_uuid,
                    setting_name.name AS name,
                    setting.update_time AS update_time,
                    setting.value AS value
                FROM
                    setting_name
                    INNER JOIN setting
                        ON setting.setting_name_id = setting_name.id
                    INNER JOIN inhabitant
                        ON setting.host_id = inhabitant.id
                    INNER JOIN host
                        ON host.id = inhabitant.id
            """)


        @classmethod
        @contract_epydoc
        def get_all_except(cls, host_uuid, exclude, rdbw):
            """
            Get all settings for the host, except the ones
            which names are listed in the "exclude" argument.
            This setting should not have been having a value before!

            @type host_uuid: UUID

            @param exclude: the iterable of names of the settings
                to be excluded.
            @type exclude: col.Iterable

            @type rdbw: DatabaseWrapperSQLAlchemy
            @rtype: dict
            """
            exclude_set = frozenset(exclude)

            rows = rdbw.execute(models.Setting
                                      .select_settings_by_host_uuid_expr(),
                                {'host_uuid': host_uuid})
            return {row.name: (str(row.value), fix_strptime(row.update_time))
                        for row in rows
                        if row.name not in exclude_set}


        @classmethod
        @contract_epydoc
        def get_setting(cls, setting_name, host_uuid,
                        with_time=False, direct=False,
                        rdbw=None):
            """
            Get the setting for the host, except the ones
            which names are listed in the "exclude" argument.
            This setting should not have been having a value before!

            @precondition: setting_name in cls.ALL_SETTINGS # setting_name
            @type host_uuid: UUID
            @param with_time: Whether we need to get the value and update time.

            @type rdbw: DatabaseWrapperSQLAlchemy

            @returns: A tuple of (setting value, update time)
                      if with_time == True;
                      just the setting value, if with_time = False.
            """
            cl_select = models.Setting \
                              .select_setting_by_host_uuid_and_name_expr(
                                   with_time=with_time)

            # Get from DB...
            rows = rdbw.execute(cl_select,
                                {'host_uuid': host_uuid,
                                 'name': setting_name})
            try:
                row = iter(rows).next()
            except StopIteration:
                # EAFP!
                row = None

            # ... use defaults if missing...
            value = row.value if row is not None \
                              else cls.get_default(setting_name, direct=True)

            # ... and decode if not "direct".
            res_value = value if direct \
                              else cls._decode(setting_name, value)

            return (res_value,
                    fix_strptime(row.update_time)
                        if row is not None
                        else datetime.min) if with_time \
                                           else res_value



    class Datasets(object):
        """All DB queries related to the backup datasets."""

        __slots__ = tuple()


        # Warning: some datasets may be missing!
        CREATE_VIEW_DATASET_FILE_STATS = _dd("""\
            CREATE VIEW __dataset_file_stats_view AS
                SELECT
                    f.dataset_id,
                    COUNT(DISTINCT id) AS file_local_count,
                    COUNT(DISTINCT file_id) AS file_count,
                    (
                        SELECT coalesce(sum(f1.size), 0)
                        FROM file_local_view AS f1
                        WHERE f1.dataset_id = f.dataset_id
                    ) AS file_local_size,
                    -1 AS file_size -- not supported
                FROM file_local_view AS f
                GROUP BY dataset_id
            """)

        # How many chunks are in each dataset?
        CREATE_VIEW_DATASET_CHUNK_STATS = _dd("""\
            CREATE VIEW __dataset_chunk_stats_view AS
                SELECT
                    file_local_view.dataset_id AS dataset_id,
                    count(DISTINCT block.chunk_id) AS chunks_count
                FROM
                    file_local_view
                    INNER JOIN block
                        ON block.file_id = file_local_view.file_id
                    GROUP BY file_local_view.dataset_id
            """)

        CREATE_VIEW_DATASET_CHUNK_STATS_ALL = _dd("""\
            CREATE VIEW __dataset_chunk_stats_all_view AS
                SELECT
                    dataset.id AS dataset_id,
                    coalesce(__dataset_chunk_stats_view.chunks_count, 0)
                        AS chunks_count
                FROM
                    dataset
                    LEFT JOIN __dataset_chunk_stats_view
                        ON dataset.id = __dataset_chunk_stats_view.dataset_id
            """)


        @classmethod
        @contract_epydoc
        def add_dataset(cls, ugroup_uuid, name, ds_uuid, sync, merged,
                        time_started, time_completed, rdbw):
            """Add a dataset record to the DB.

            @param ugroup_uuid: the UUID of the User Group, for which
                the dataset is created.
            @type ugroup_uuid: UUID

            @param name: the name of the dataset.
            @type name: basestring

            @param ds_uuid: the UUID of the Dataset.
            @type ds_uuid: UUID

            @param sync: whether the Dataset should be a "sync" one
            @type sync: bool

            @param merged: whether the Dataset should be considered
                a merged one.
            @type merged: bool

            @param time_started: the time of dataset start.
            @param time_started: datetime

            @param time_completed: the time of dataset start. May be missing
            @param time_completed: datetime, NoneType

            @type rdbw: DatabaseWrapperSQLAlchemy
            """
            m = models

            cl_group_id = sql_select([m.user_groups.c.id],
                                     m.user_groups.c.uuid == ugroup_uuid)

            rdbw.execute(m.datasets.insert().values(group=cl_group_id),
                         {'name': name,
                          'uuid': ds_uuid,
                          'sync': sync,
                          'merged': merged,
                          'time_started': time_started,
                          'time_completed': time_completed})


        @classmethod
        @duration_logged()
        @contract_epydoc
        def create_dataset_for_backup(cls, host_uuid, dataset, rdbw):
            """Given a dataset object, store its structure in the database.

            @type host_uuid: UUID, NoneType
            @param host_uuid: the UUID of the host; used to validate.
                May be C{None} (and in such case, the validation
                is not performed) but this feature should not be abused,
                if possible.

            @param dataset: the dataset object with the contents of the backup.
            @type dataset: AbstractDataset

            @type rdbw: DatabaseWrapperSQLAlchemy

            @returns: dataset UUID.
            @rtype: UUID

            @todo: The final code should not prevent using backslashes
                   in the file names!
            """
            m = models

            #
            # [1/3] Insert dataset
            #

            cls.add_dataset(ugroup_uuid=dataset.ugroup_uuid,
                            name=dataset.name, ds_uuid=dataset.uuid,
                            sync=dataset.sync, merged=False,
                            time_started=dataset.time_started,
                            time_completed=dataset.time_completed,
                            rdbw=rdbw)

            # It is possible the dataset is re-inserted.
            # Get the dataset ID explicitly.
            inserted_ds = Queries.Datasets.get_dataset_by_uuid(
                              dataset_uuid=dataset.uuid,
                              host_uuid=host_uuid, rdbw=rdbw)

            assert inserted_ds is not None
            _dataset_id = inserted_ds._id
            _group_id = inserted_ds._ugroup_id

            #
            # [2/3] Insert base directory
            #

            all_base_dirs = \
                [{'dataset': _dataset_id,
                  'path': encode_posix_path(dirpath)}
                     for dirpath in dataset.directories.iterkeys()]

            if all_base_dirs:
                rdbw.execute(m.base_directories.insert(),
                             all_base_dirs)

            #
            # [3/3] Insert file_local (and file as well).
            #

            # Add files - may be done on a separate phase.
            # We are creating new file UUIDs at this moment.
            all_files_iter = \
                ({'fingerprint': file.fingerprint,
                  'crc32': None if file.crc32 is None
                                else crc32_to_signed(file.crc32),
                  'uuid': None if (file.crc32 is None and
                                   file.fingerprint is None)
                               else gen_uuid(),
                  'group_id': _group_id,
                  'base_dir_path': encode_posix_path(dirpath),
                  'ds_id': _dataset_id,
                  'isdir': file.isdir,
                  'rel_path': encode_posix_path(file.rel_path),
                  'attrs': ''}
                     for dirpath, (files_small, files_large)
                         in dataset.directories.iteritems()
                     for file in chain(files_small, files_large))
            # Each "file" value is LocalFileState.
            # Each entry stands for either a new/modified file/directory,
            # or a deleted... something, since we cannot distinguish
            # between the deleted directories and the deleted files
            # *after* they are deleted.

            rdbw.querymany(Queries.Files.INSERT_FILE_LOCAL_VIEW_ENTRY,
                           all_files_iter)

            # We've added the file records; but some of them probably contain
            # the deleted files rather than the created/modified ones.
            # Let's remember the deleted files for a while in a set,
            # so that they can be references subsequently.
            #
            # The set consists of tuples of the following structure:
            #   (base_dir_path, rel_path)
            deleted_paths_set = \
                {(encode_posix_path(dirpath), encode_posix_path(file.rel_path))
                     for dirpath, (files_small, files_large)
                         in dataset.directories.iteritems()
                     for file in chain(files_small, files_large)
                         if file.crc32 is None}

            # Add intermediate directories - should be done after adding files,
            # to be sure that the base directories are added too.
            # ticket:141 - does it need to be fixed to support directories?
            all_dirs_per_basedir_iter = \
                {encode_posix_path(dirpath):
                     {interm_dir
                          for dir_ in chain(files_small, files_large)
                          for interm_dir
                              in get_intermediate_paths_posix(
                                     encode_posix_path(dir_.rel_path))}
                     for dirpath, (files_small, files_large)
                         in dataset.directories.iteritems()}
            # We don't want to add the directories which were deleted before.
            all_dirs_iter = \
                ({'ds_id': _dataset_id,
                  'base_dir_path': basedir,
                  'rel_path': subdir,
                  'attrs': ''}
                     for basedir, subdirs_per_base_dir
                         in all_dirs_per_basedir_iter.iteritems()
                     for subdir in subdirs_per_base_dir
                        if (basedir, subdir) not in deleted_paths_set)

            rdbw.querymany(Queries.Files.INSERT_FILE_LOCAL_DIR_ENTRY,
                           all_dirs_iter)

            return dataset.uuid


        # Arguments:
        # :ds_uuid
        # :host_uuid - for access validation only
        GET_DATASET_BY_UUID_SECURE = _dd("""\
            SELECT
                    dataset.id AS id,
                    dataset.uuid AS uuid,
                    dataset.sync AS sync,
                    dataset.name AS name,
                    dataset.group_id AS ugroup_id,
                    "group".uuid AS ugroup_uuid,
                    dataset.time_started AS time_started,
                    dataset.time_completed AS time_completed
                FROM
                    dataset
                    INNER JOIN "group"
                        ON dataset.group_id = "group".id
                WHERE
                    dataset.uuid = :ds_uuid AND
                    dataset.group_id IN (
                        SELECT membership.group_id
                            FROM
                                membership
                                INNER JOIN "user"
                                    ON "user".id = membership.user_id
                                INNER JOIN host_view
                                    ON host_view.user_id = "user".id
                            WHERE host_view.uuid = :host_uuid
                    )
            """)

        # Arguments:
        # :ds_uuid
        GET_DATASET_BY_UUID_INSECURE = _dd("""\
            SELECT
                    dataset.id AS id,
                    dataset.uuid AS uuid,
                    dataset.sync AS sync,
                    dataset.name AS name,
                    dataset.group_id AS ugroup_id,
                    "group".uuid AS ugroup_uuid,
                    dataset.time_started AS time_started,
                    dataset.time_completed AS time_completed
                FROM
                    dataset
                    INNER JOIN "group"
                        ON dataset.group_id = "group".id
                WHERE
                    dataset.uuid = :ds_uuid
            """)

        @classmethod
        @contract_epydoc
        def get_dataset_by_uuid(cls, dataset_uuid, host_uuid, rdbw):
            """
            Given a dataset UUID, return the basic Dataset structure (or None
            if such a dataset is absent).
            Note that the C{directories} field is empty.

            @param dataset_uuid: the dataset UUID
            @type dataset_uuid: UUID

            @param host_uuid: the UUID of the host; used only to validate
                that the host is allowed to get the information.
                In the cases when the host UUID is not available
                but the dataset is still needed, C{None} may be passed;
                but one should not abuse this feature.

            @type host_uuid: UUID, NoneType

            @type rdbw: DatabaseWrapperSQLAlchemy

            @rtype: BasicDatasetInfoWith_IDs, NoneType

            @todo: instead of validating on the host_uuid, we could validate
                on the group_uuid. Everyone who wants to access the dataset,
                must know the matching group_uuid already.
            """
            # Is the host_uuid present?
            if host_uuid is not None:
                row = rdbw.query_value(cls.GET_DATASET_BY_UUID_SECURE,
                                       {'ds_uuid': dataset_uuid,
                                        'host_uuid': host_uuid})
            else:
                row = rdbw.query_value(cls.GET_DATASET_BY_UUID_INSECURE,
                                       {'ds_uuid': dataset_uuid})

            if row is None:
                return None
            else:
                return BasicDatasetInfoWith_IDs(
                           # AbstractBasicDatasetInfo
                           name=row.name,
                           sync=bool(row.sync),
                           uuid=DatasetUUID.safe_cast_uuid(row.uuid),
                           ugroup_uuid=UserGroupUUID.safe_cast_uuid(
                                           row.ugroup_uuid),
                           time_started=fix_strptime(row.time_started),
                           time_completed=fix_strptime(row.time_completed),
                           # BasicDatasetInfoWith_ID
                           _id=row.id,
                           _ugroup_id=row.ugroup_id)


        @classmethod
        @contract_epydoc
        def update_dataset(cls, host_uuid, dataset, rdbw):
            """Given a dataset object, update it in the database.

            @param host_uuid: the host UUID (used to validate
                the rights to update the dataset).
                May be C{None} (hence not validated), but this feature
                should not be abused for now.
            @type host_uuid: UUID, NoneType

            @type dataset: AbstractDataset

            @type rdbw: DatabaseWrapperSQLAlchemy
            """
            m = models

            # What dataset should we update?
            whereclause = (m.datasets.c.uuid == dataset.uuid)
            if host_uuid is not None:
                # Find all groups where the user belongs
                select_groups_for_host = \
                    sql_select([m.memberships.c.group],
                               from_obj=m.memberships.join(m.users)
                                                     .join(m.hosts)
                                                     .join(m.inhabitants),
                               whereclause=(m.inhabitants.c.uuid == host_uuid))

                # Add extra clause
                whereclause &= (m.datasets.c.group.in_(select_groups_for_host))

            # Finally, the update itself
            update_expr = \
                m.datasets.update(whereclause=whereclause) \
                          .values(name=dataset.name,
                                  time_started=dataset.time_started,
                                  time_completed=dataset.time_completed)

            rdbw.execute(update_expr)


        # Arguments:
        # :host_uuid
        # :ds_uuid
        DELETE_DATASET = _dd("""\
            DELETE FROM dataset
                WHERE
                    group_id IN (
                        SELECT membership.group_id
                        FROM
                            membership
                            INNER JOIN host_view
                                USING(user_id)
                        WHERE host_view.uuid = :host_uuid
                    ) AND
                    uuid = :ds_uuid
            """)

        @classmethod
        @contract_epydoc
        def delete_dataset(cls, host_uuid, ds_uuid, rdbw):
            """Given a dataset UUID, delete it from the database.

            Note it also removes the orphaned chunks afterwards.

            @param host_uuid: The host UUID
            @type host_uuid: UUID

            @param ds_uuid: The dataset UUID
            @type ds_uuid: UUID

            @type rdbw: DatabaseWrapperSQLAlchemy
            """
            rdbw.query(cls.DELETE_DATASET,
                       {'host_uuid': host_uuid, 'ds_uuid': ds_uuid})
            Queries.Chunks.delete_dataset_orphaned_chunks(rdbw)

        @classmethod
        @contract_epydoc
        def delete_datasets(cls, host_uuid, ds_uuids, rdbw):
            """Delete several datasets from the database by their UUIDs.

            @note: it also removes the orphaned chunks afterwards.

            @param host_uuid: the host UUID
            @type host_uuid: UUID

            @param ds_uuids: The dataset UUID
            @type ds_uuids: col.Iterable

            @type rdbw: DatabaseWrapperSQLAlchemy
            """
            rdbw.querymany(cls.DELETE_DATASET,
                           ({'host_uuid': host_uuid, 'ds_uuid': u}
                                for u in ds_uuids))
            Queries.Chunks.delete_dataset_orphaned_chunks(rdbw)


        # Only completed backups are returned (due to INNER JOINs
        # with file_stats and chunk_stats).
        # When need all datasets, change to LEFT JOINs and update the interface
        # and intermediate code.
        # Arguments:
        # :host_uuid
        SELECT_ALL_DATASETS = _dd("""\
            SELECT
                    dataset.name AS name,
                    dataset.sync AS sync,
                    dataset.uuid AS uuid,
                    "group".uuid AS ugroup_uuid,
                    dataset.time_started AS time_started,
                    dataset.time_completed AS time_completed,
                    file_stats.file_local_count AS file_local_count,
                    --file_stats.file_count AS file_count,
                    file_stats.file_local_size AS file_local_size,
                    --file_stats.file_size AS file_size,
                    chunk_stats.chunks_count AS chunks_count
                FROM
                    host_view
                    INNER JOIN membership
                        ON membership.user_id = host_view.user_id
                    INNER JOIN "group"
                        ON "group".id = membership.group_id
                    INNER JOIN dataset
                        ON dataset.group_id = "group".id
                    INNER JOIN __dataset_file_stats_view AS file_stats
                        ON file_stats.dataset_id = dataset.id
                    INNER JOIN __dataset_chunk_stats_all_view AS chunk_stats
                        ON chunk_stats.dataset_id = dataset.id
                WHERE
                    host_view.uuid = :host_uuid
            """)

        @classmethod
        @contract_epydoc
        def get_just_datasets(cls, host_uuid, rdbw):
            """
            Given the host UUID, return all the datasets
            (without any directory information and no matter of the user group)
            for it.

            @todo: would be good to convert the times in the datasets,
                   using the "from" and the "to" tzinfos.
                   See hardcoded C{from_to_tzinfos} variable in the code
                   and change to calculate the tzinfos properly.

            @param host_uuid: The host UUID
            @type host_uuid: UUID

            @type rdbw: DatabaseWrapperSQLAlchemy

            @param from_to_tzinfos: The tuple of "from_tzinfo" and "to_tzinfo",
                                    if the datetime should be converted.
            @type from_to_tzinfos: (NoneType, tuple)
            @precondition: from_to_tzinfos is None or
                           (len(from_to_tzinfos) == 2 and
                            consists_of(from_to_tzinfos, tzinfo))

            @returns: the datasets for such a host
                      (sequence of DatasetWithAggrInfo), maybe non-reiterable.
            @rtype: col.Iterable
            """
            from_to_tzinfos = None

            rows = rdbw.query(cls.SELECT_ALL_DATASETS,
                              {'host_uuid': host_uuid})
            interim_datasets = \
                (DatasetWithAggrInfo(  # DatasetWithAggrInfo-specific
                                     chunks_count=row.chunks_count,
                                     files_count=row.file_local_count,
                                     size=row.file_local_size,
                                       # Dataset-specific
                                     name=row.name,
                                     sync=bool(row.sync),
                                     uuid=DatasetUUID.safe_cast_uuid(row.uuid),
                                     ugroup_uuid=UserGroupUUID.safe_cast_uuid(
                                                     row.ugroup_uuid),
                                     time_started=fix_strptime(
                                                      row.time_started),
                                     time_completed=fix_strptime(
                                                        row.time_completed))
                     for row in rows)

            if from_to_tzinfos is not None:

                @contract_epydoc
                def fix_naive_datetime_inline(dt):
                    """
                    @precondition: dt.tzinfo is None
                    @type dt: datetime
                    @rtype: datetime
                    @postcondition: dt is result
                    @postcondition: dt.tzinfo is None
                    """
                    if from_to_tzinfos is not None:
                        from_, to_ = from_to_tzinfos
                        return dt.replace(tzinfo=from_) \
                                 .astimezone(to_) \
                                 .replace(tzinfo=None)

                @contract_epydoc
                def fix_datetime_inline(ds):
                    """
                    @type ds: AbstractDataset
                    @rtype: AbstractDataset
                    @postcondition: ds is result
                    """
                    fix_naive_datetime_inline(ds.time_started)
                    fix_naive_datetime_inline(ds.time_completed)
                    return ds

                return imap(fix_datetime_inline, interim_datasets)
            else:
                return interim_datasets


        # Arguments:
        # :host_uuid (to validate the access)
        # :max_start_time (max time-of-creation to keep)
        SELECT_OLDER_THAN = _dd("""\
            SELECT uuid
                FROM dataset
                WHERE
                    group_id IN (
                        SELECT membership.group_id
                        FROM
                            membership
                            INNER JOIN host_view
                                USING(user_id)
                        WHERE host_view.uuid = :host_uuid
                    ) AND
                    time_started < :max_start_time
            """)

        @classmethod
        @contract_epydoc
        def get_outdated_ds_uuids(cls, host_uuid, older_than, rdbw):
            """
            For a host (given by its UUID), return the UUIDs of the datasets
            which are older than C{older_than},

            @rtype: col.Iterable
            """
            rows = rdbw.query(cls.SELECT_OLDER_THAN,
                              {'host_uuid': host_uuid,
                               'max_start_time': older_than})
            return (DatasetUUID.safe_cast_uuid(r.uuid)
                        for r in rows)


        @classmethod
        def restore_files_to_dataset_clone(cls,
                                           base_ds_uuid, paths, start_time,
                                           rdbw):
            """
            Given a dataset UUID for a "baseline" dataset,
            and an iterable over paths, create a new dataset which contains
            the latest editions of files (referred by C{paths})
            to the moment of baseline dataset creation.

            @type base_ds_uuid: UUID
            @type paths: col.Iterable
            @type start_time: datetime
            @type rdbw: DatabaseWrapperSQLAlchemy

            @returns: the UUID of new dataset.
            @rtype: DatasetUUID
            """
            # Getting base time_created for restore query.
            base_ds = cls.get_dataset_by_uuid(base_ds_uuid, None, rdbw)
            max_time = base_ds.time_completed

            user_group_uuid = \
                models.Dataset.get_group_uuid_by_ds_uuid(base_ds_uuid)

            # And all file_locals with max time_created before max_time.
            file_locals_sel = models.FileLocal.get_available_on_dt(
                                  max_time, user_group_uuid)
            new_ds_uuid = gen_uuid()

            # Prepare dict with bindings to insert new dataset (group
            # is based on query's dataset info).
            dataset_ins_dict = {
                'uuid': new_ds_uuid,
                'sync': False,
                'merged': False,
                'name': '',
                'time_started': start_time,
                'time_completed': None,
                '_ds_uuid': base_ds_uuid,
            }
            rdbw.execute(
                models.Dataset.insert_dataset_copying_group_from_ds_expr(),
                dataset_ins_dict)
            ds_id_sel = models.Dataset.get_id_by_uuid(new_ds_uuid)

            # Getting summary info of file_locals:
            # file_locals: rel_path, file, isdir;
            # base_directory: path.
            files_sel = models.FileLocal.clone_file_locals(file_locals_sel,
                                                           max_time,
                                                           paths,
                                                           user_group_uuid)

            # And creating new base_directories from file_locals info.
            _new_base_dirs_sel = \
                models.BaseDirectory.clone_basedirs_for_insert(files_sel,
                                                               ds_id_sel)
            new_base_dirs = rdbw.execute(_new_base_dirs_sel).fetchall()
            rdbw.execute(models.base_directories.insert(),
                         new_base_dirs)

            # Select just created base_directories.
            created_bd_sel = models.BaseDirectory.get_from_ds(ds_id_sel)

            # Select and insert new file_locals, based on old files
            # and new base_directories.
            files_to_insert_sel = models.FileLocal.get_file_locals_for_insert(
                                      files_sel, created_bd_sel)

            # TODO: Refactor SQLA views to insert not executed selects.
            files_to_insert = rdbw.execute(files_to_insert_sel).fetchall()
            rdbw.execute(models.file_locals.insert(),
                         files_to_insert)

            # As the final touch, let's update the time_completed
            # in the dataset with an actual time, so the duration between
            # time_started and time_completed now really shows how much
            # did it take to re-fetch the rows and re-insert them.
            rdbw.execute(models.Dataset.update_tc_by_ds_uuid_expr(),
                         {'_ds_uuid': base_ds_uuid,
                          'time_completed': datetime.utcnow()})

            return DatasetUUID.safe_cast_uuid(new_ds_uuid)


        @classmethod
        def is_dataset_permitted_for_user(cls, ds_uuid, username, rdbw):
            """
            Check whether the dataset is accessible to the user according to
            the user's membership in groups.

            @param ds_uuid: The dataset UUID
            @type ds_uuid: UUID
            @type rdbw: DatabaseWrapperSQLAlchemy
            """
            select_cl = sql_select([models.datasets.c.uuid == ds_uuid],
                                   ((models.datasets.c.uuid == ds_uuid) &
                                        (models.users.c.name == username)),
                                   models.datasets
                                       .join(models.user_groups,
                                             models.datasets.c.group ==
                                                 models.user_groups.c.id)
                                       .join(models.memberships,
                                             models.user_groups.c.id ==
                                                 models.memberships.c.group)
                                       .join(models.users,
                                             models.memberships.c.user ==
                                                 models.users.c.user_id))

            return rdbw.execute(select_cl).scalar()


        @classmethod
        def get_last_ds_bd_by_rel_path_group_expr(cls, rel_path, username,
                                                  ds_uuid=None, rdbw=None):
            r"""
            @return: last dataset uuid for rel_path and username, so that
                     time_started of returning ds_uuid is less than
                     time_started of incoming ds_uuid.

            @rtype: sqlalchemy.sql.expression.Executable
            """
            # Get all groups that can access user.
            group_expr = sql_select([models.user_groups.c.id],
                                    (models.users.c.name == username),
                                    models.user_groups
                                        .join(models.memberships)
                                        .join(models.users)).alias()

            max_time = None
            # Set upper time limit for query if ds_uuid is defined.
            if ds_uuid is not None:
                ds_time = sql_select([models.datasets.c.time_started],
                                     (models.datasets.c.uuid == ds_uuid),
                                     models.datasets).alias()
                select_cl = \
                    sql_select([func.max(models.datasets.c.time_started)],
                               (models.datasets.c.time_started <= ds_time),
                               models.datasets)
                max_time = rdbw.execute(select_cl).scalar()

            latest_urp = \
                models.FileLocal\
                    .get_latest_uniq_rel_paths_by_group_expr(max_time)\
                    .alias()

            max_time_for_rel_path = \
                sql_select([latest_urp.c.ds_mts],
                           ((latest_urp.c.rel_path == rel_path) &
                            (latest_urp.c.group == group_expr.c.id)),
                           latest_urp).alias()

            file_last_ds_uuid = \
                sql_select([models.datasets.c.uuid,
                            models.base_directories.c.path],
                           ((models.datasets.c.time_started ==
                                max_time_for_rel_path) &
                            (models.datasets.c.group == group_expr.c.id)),
                           models.datasets
                               .join(models.base_directories))
            for row in rdbw.execute(file_last_ds_uuid):
                return row


        @classmethod
        def get_enc_key_for_dataset(cls, ds_uuid, rdbw):
            """Get an encryption key to use for Dataset.

            @return: the encryption key (if needed).
            @rtype: str, NoneType
            """
            dataset = cls.get_dataset_by_uuid(dataset_uuid=ds_uuid,
                                              host_uuid=None, rdbw=rdbw)
            if dataset is not None:
                group = Queries.Inhabitants.get_ugroup_by_uuid(
                            dataset.ugroup_uuid, rdbw)
                if group is None:
                    return group.enc_key
            return None



    class Files(object):
        """All DB queries related to the particular files in the backups."""

        __slots__ = tuple()


        __CREATE_FILE_VIEW_SQLITE = _dd("""\
            CREATE VIEW file_view AS
                SELECT
                    id,
                    group_id,
                    crc32,
                    uuid,
                    fingerprint,
                    {from_hex} AS size
                FROM file
            """).format(from_hex=hex_convert.get_sqlite3_hex_convert_expr(
                                     'hex(fingerprint)'))

        __CREATE_FILE_VIEW_POSTGRESQL = _dd("""\
            CREATE VIEW file_view AS
                SELECT
                    id,
                    group_id,
                    crc32,
                    uuid,
                    fingerprint,
                    {from_fp} AS size
                FROM file
            """).format(from_fp=hex_convert.get_postgresql_hex_convert_expr(
                                    'fingerprint'))

        CREATE_FILE_VIEW = {
            'sqlite': __CREATE_FILE_VIEW_SQLITE,
            'postgresql': __CREATE_FILE_VIEW_POSTGRESQL
        }


        # The following fields may be used as the keys for insertion:
        # - - file - -
        # * fingerprint
        # * crc32
        # * uuid
        # * file_group_id
        # - - base_directory - -
        # * base_directory_path
        # * dataset_id
        # - - file_local - -
        # * isdir
        # * rel_path
        # * attrs
        CREATE_FILE_LOCAL_VIEW = _dd("""\
            CREATE VIEW file_local_view AS
                SELECT
                    file_local.id AS id,
                    file_local.isdir AS isdir,
                    file_local.rel_path AS rel_path,
                    file_local.attrs AS attrs,
                    base_directory.id AS base_directory_id,
                    base_directory.dataset_id AS dataset_id,
                    base_directory.path AS base_directory_path,
                    file_view.id AS file_id,
                    file_view.group_id AS file_group_id,
                    file_view.crc32 AS crc32,
                    file_view.uuid AS uuid,
                    file_view.fingerprint AS fingerprint,
                    file_view.size AS size
                FROM
                    base_directory
                    INNER JOIN file_local
                        ON file_local.base_directory_id = base_directory.id
                    LEFT JOIN file_view
                        ON file_view.id = file_local.file_id
            """)

        # Don't insert a new "file" record if such fingerprint exists already.
        # Reuse the existing "file" record as a FK
        # from the new "file_local" record.
        __CREATE_FILE_LOCAL_VIEW_INSERT_TRIGGER_SQLITE = _dd("""\
            CREATE TRIGGER IF NOT EXISTS file_local_view_insert_trigger
                INSTEAD OF INSERT ON file_local_view
                FOR EACH ROW
                BEGIN
                    INSERT OR IGNORE INTO file(
                            fingerprint,
                            crc32,
                            uuid,
                            group_id
                        )
                        VALUES (
                            new.fingerprint,
                            new.crc32,
                            new.uuid,
                            new.file_group_id
                        );
                    INSERT OR IGNORE INTO base_directory(path, dataset_id)
                        VALUES (
                            new.base_directory_path,
                            new.dataset_id
                        );
                    INSERT OR ROLLBACK INTO file_local(
                            file_id,
                            base_directory_id,
                            isdir,
                            rel_path,
                            attrs
                        )
                        VALUES (
                            (
                                SELECT id
                                    FROM file
                                    WHERE
                                        group_id = new.file_group_id AND
                                        fingerprint = new.fingerprint
                            ),
                            (
                                SELECT id
                                    FROM base_directory
                                    WHERE
                                        path = new.base_directory_path AND
                                        dataset_id = new.dataset_id
                            ),
                            new.isdir,
                            new.rel_path,
                            new.attrs
                        );
                END
            """)

        __CREATE_FILE_LOCAL_VIEW_DELETE_TRIGGER_SQLITE = _dd("""\
            CREATE TRIGGER IF NOT EXISTS file_local_view_delete_trigger
                INSTEAD OF DELETE ON file_local_view
                FOR EACH ROW
                BEGIN
                    DELETE FROM file_local WHERE id = old.id;
                END
            """)

        __CREATE_FILE_LOCAL_VIEW_TRIGGER_PROC_POSTGRESQL = _dd("""\
            CREATE OR REPLACE FUNCTION file_local_view_trigger_proc()
            RETURNS TRIGGER AS $$
            DECLARE
                _file_id file.id%TYPE;
                _base_directory_id base_directory.id%TYPE;
            BEGIN
                IF (TG_OP = 'INSERT') THEN
                    -- INSERT OR IGNORE INTO file

                    IF (new.fingerprint IS NULL AND
                        new.crc32 IS NULL AND
                        new.uuid IS NULL) THEN
                        -- Do not insert file entry, it is deleted
                        _file_id = NULL;

                    ELSE
                        -- Insert file.
                        SELECT id
                            FROM file
                            WHERE
                                group_id = new.file_group_id AND
                                fingerprint = new.fingerprint
                            INTO _file_id;

                        IF NOT FOUND THEN
                            INSERT INTO file(
                                    fingerprint,
                                    crc32,
                                    uuid,
                                    group_id
                                )
                                VALUES (
                                    new.fingerprint,
                                    new.crc32,
                                    new.uuid,
                                    new.file_group_id
                                )
                                RETURNING id INTO _file_id;
                        END IF;

                    END IF;

                    -- INSERT OR IGNORE INTO base_directory

                    SELECT id
                        FROM base_directory
                        WHERE
                            path = new.base_directory_path AND
                            dataset_id = new.dataset_id
                            INTO _base_directory_id;

                    IF NOT FOUND THEN
                        INSERT INTO base_directory(path, dataset_id)
                            VALUES (
                                new.base_directory_path,
                                new.dataset_id
                            )
                            RETURNING id INTO _base_directory_id;
                    END IF;

                    INSERT INTO file_local(
                            file_id,
                            base_directory_id,
                            isdir,
                            rel_path,
                            attrs
                        )
                        VALUES (
                            _file_id,
                            _base_directory_id,
                            new.isdir,
                            new.rel_path,
                            new.attrs
                        );

                    RETURN new;
                ELSIF (TG_OP = 'DELETE') THEN
                    DELETE FROM file_local WHERE id = old.id;

                    RETURN old;
                END IF;
            END;
            $$ LANGUAGE plpgsql
            """)

        __CREATE_FILE_LOCAL_VIEW_TRIGGER_POSTGRESQL = _dd("""\
            CREATE TRIGGER file_local_view_trigger
                INSTEAD OF INSERT OR DELETE ON file_local_view
                FOR EACH ROW
                EXECUTE PROCEDURE file_local_view_trigger_proc()
            """)

        CREATE_FILE_LOCAL_VIEW_TRIGGERS = {
            'sqlite': [__CREATE_FILE_LOCAL_VIEW_INSERT_TRIGGER_SQLITE,
                       __CREATE_FILE_LOCAL_VIEW_DELETE_TRIGGER_SQLITE],
            'postgresql': [__CREATE_FILE_LOCAL_VIEW_TRIGGER_PROC_POSTGRESQL,
                           __CREATE_FILE_LOCAL_VIEW_TRIGGER_POSTGRESQL]
        }

        # Arguments:
        # :fingerprint
        # :crc32
        # :uuid - is generated randomly by the caller
        # :group_id
        # :base_dir_path
        # :ds_id
        # :isdir
        # :rel_path
        # :attrs
        # We cannot do the usual INSERT OR ROLLBACK here,
        # as it will override the underlying ON CONFLICT clauses
        # which are essential for the operation.
        INSERT_FILE_LOCAL_VIEW_ENTRY = _dd("""\
            INSERT INTO file_local_view(
                    fingerprint,
                    crc32,
                    uuid,
                    file_group_id,
                    base_directory_path,
                    dataset_id,
                    isdir,
                    rel_path,
                    attrs
                )
                VALUES (
                    :fingerprint,
                    :crc32,
                    :uuid,
                    :group_id,
                    :base_dir_path,
                    :ds_id,
                    :isdir,
                    :rel_path,
                    :attrs
                )
            """)

        # Arguments:
        # :ds_id
        # :base_dir_path
        # :rel_path
        # :attrs
        INSERT_FILE_LOCAL_DIR_ENTRY = _dd("""\
            INSERT INTO file_local(
                file_id,
                base_directory_id,
                isdir,
                rel_path,
                attrs
           )
            VALUES (
                NULL,
                (SELECT id
                    FROM base_directory
                    WHERE dataset_id = :ds_id AND path = :base_dir_path),
                CAST (1 AS BOOLEAN),
                :rel_path,
                :attrs
            )
            """)

        # Arguments:
        # :host_uuid - used only to validate the accessibility
        #              of the information
        # :ds_uuid
        SELECT_FILES_FOR_DATASET = _dd("""\
            SELECT
                    file_local_view.base_directory_path AS base_directory_path,
                    file_local_view.crc32 AS crc32,
                    file_local_view.uuid AS uuid,
                    file_local_view.rel_path,
                    file_local_view.attrs AS attrs,
                    file_local_view.fingerprint AS fingerprint
                FROM
                    host_view
                    INNER JOIN membership
                        ON membership.user_id = host_view.user_id
                    INNER JOIN dataset
                        ON dataset.group_id = membership.group_id
                    INNER JOIN file_local_view
                        ON dataset.id = file_local_view.dataset_id
                WHERE
                    host_view.uuid = :host_uuid AND
                    file_local_view.isdir = CAST(0 AS BOOLEAN) AND
                    dataset.uuid = :ds_uuid
                ORDER BY file_local_view.base_directory_path
            """)

        @classmethod
        @duration_logged()
        @contract_epydoc
        def get_files_for_dataset(cls, host_uuid, ds_uuid, rdbw):
            """
            Given the host UUID and the dataset UUID,
            return all the files in this dataset as a list.

            @todo: long operation, takes 7.7 seconds on test dataset.

            @param host_uuid: The host UUID
            @type host_uuid: UUID

            @type ds_uuid: UUID

            @type rdbw: DatabaseWrapperSQLAlchemy

            @returns: the non-reiterable iterable of
                C{LocalPhysicalFileState}-s.
            @rtype: col.Iterable
            """
            rows = rdbw.query(cls.SELECT_FILES_FOR_DATASET,
                              {'host_uuid': host_uuid,
                               'ds_uuid': ds_uuid})
            return (LocalPhysicalFileState(
                            # LocalFileState-specific
                            time_changed=None,
                            # LocalFile-specific
                            rel_path=decode_posix_path(row.rel_path),
                            base_dir=decode_posix_path(
                                         row.base_directory_path),
                            attrs=row.attrs,
                            # File-specifics
                            fingerprint=None if row.fingerprint is None
                                             else Fingerprint.from_buffer(
                                                      row.fingerprint),
                            crc32=None if row.crc32 is None
                                       else crc32_to_unsigned(row.crc32),
                            uuid=None if row.uuid is None
                                      else FileUUID.safe_cast_uuid(row.uuid))
                        for row in rows)


        @classmethod
        @contract_epydoc
        def time_changes_for_file_by_username_rel_path(cls, username, rel_path,
                                                       rdbw):
            """
            Get main info of path changes by username and rel_path. Mostly used
            for file versioning in web.

            @param username: the name of the user.
            @type username: basestring

            @param rel_path: rel_path of the file.
            @type rel_path: basestring

            @type rdbw: DatabaseWrapperSQLAlchemy

            @return: base directory path and size of a rel_path and
                datasets uuid with time_started where this file was changed
                (added or deleted).
            """
            return rdbw.execute(
                models.FileLocal
                    .time_changes_for_file_by_username_rel_path_expr(),
                {'username': username, 'rel_path': rel_path,
                 'is_merged': False})



    class Chunks(object):
        """All DB queries related to the particular chunks of the backups.

        The hash of the chunk may be NULL/None, to reflect the situation
        when the chunk hash failed at the moment of its initial upload,
        hence rendering it probably invalid; though the chunk was still
        uploaded because some of the files in it still may be considered valid.

        Nevertheless, there should never be the chunk with the same pair
        of (hash, size), provided that the chunk is not NULL, and this
        is enforced on the database level.
        """
        __slots__ = tuple()


        # Note about the triggers:
        # It doesn't insert a new "chunk" record if such an UUID exists
        # already, or if such chunk hash/chunk size pair exists already.
        # It reuses the existing "chunk" record as a FK from
        # the new "chunk_per_host" record.
        # Note it does NOT insert a new host record, ever.
        # For insertion/updating, use the host ID rather than host UUID.

        # Chunk insertion keys:
        # Either "chunk_hash" + "chunk_size" or "chunk_uuid"
        #
        # host_id
        #
        # chunk_crc32
        # chunk_uuid
        # chunk_hash
        # chunk_size
        # chunk_maxsize_code
        CREATE_CHUNK_PER_HOST_VIEW = _dd("""\
            CREATE VIEW chunk_per_host_view AS
                SELECT
                    chunk_per_host.id AS id,

                    host_view.id AS host_id,
                    host_view.uuid AS host_uuid,

                    chunk.id AS chunk_id,
                    chunk.crc32 AS chunk_crc32,
                    chunk.uuid AS chunk_uuid,
                    chunk.hash AS chunk_hash,
                    chunk.size AS chunk_size,
                    chunk.maxsize_code AS chunk_maxsize_code
                FROM
                    host_view
                    INNER JOIN chunk_per_host
                        ON host_view.id = chunk_per_host.host_id
                    INNER JOIN chunk
                        ON chunk_per_host.chunk_id = chunk.id
            """)


        __CREATE_CHUNK_PER_HOST_VIEW_INSERT_TRIGGER_SQLITE = _dd("""\
            CREATE TRIGGER IF NOT EXISTS chunk_per_host_view_insert_trigger
                INSTEAD OF INSERT ON chunk_per_host_view
                FOR EACH ROW
                BEGIN
                    INSERT OR IGNORE INTO chunk(
                            crc32,
                            uuid,
                            hash,
                            size,
                            maxsize_code
                        )
                        VALUES (
                            new.chunk_crc32,
                            new.chunk_uuid,
                            new.chunk_hash,
                            new.chunk_size,
                            new.chunk_maxsize_code
                        );
                    -- The chunk with such hash and size may exist already,
                    -- so we don't overwrite its UUID if it exists.
                    INSERT OR IGNORE INTO chunk_per_host(
                            chunk_id,
                            host_id
                        )
                        VALUES (
                            (
                                SELECT id
                                    FROM chunk
                                    WHERE
                                        (
                                            new.chunk_hash IS NOT NULL AND
                                            new.chunk_size IS NOT NULL AND
                                            hash = new.chunk_hash AND
                                            size = new.chunk_size
                                        )
                                        OR
                                        (
                                            uuid = new.chunk_uuid
                                        )
                            ),
                            new.host_id
                        );
                END;
            """)

        __CREATE_CHUNK_PER_HOST_VIEW_DELETE_TRIGGER_SQLITE = _dd("""\
            CREATE TRIGGER IF NOT EXISTS chunk_per_host_view_delete_trigger
                INSTEAD OF DELETE ON chunk_per_host_view
                FOR EACH ROW
                BEGIN
                    DELETE FROM chunk_per_host WHERE id = old.id;
                END
            """)

        __CREATE_CHUNK_PER_HOST_VIEW_TRIGGER_PROC_POSTGRESQL = _dd("""\
            CREATE OR REPLACE FUNCTION chunk_per_host_view_trigger_proc()
            RETURNS TRIGGER AS $$
            DECLARE
                _chunk_id chunk.id%TYPE;
            BEGIN
                IF (TG_OP = 'INSERT') THEN
                    -- INSERT OR IGNORE INTO chunk

                    SELECT id
                        FROM chunk
                        WHERE
                            (
                                new.chunk_hash IS NOT NULL AND
                                new.chunk_size IS NOT NULL AND
                                hash = new.chunk_hash AND
                                size = new.chunk_size
                            )
                            OR
                            (
                                uuid = new.chunk_uuid
                            )
                        INTO _chunk_id;

                    IF NOT FOUND THEN
                        INSERT INTO chunk(
                                crc32,
                                uuid,
                                hash,
                                size,
                                maxsize_code
                            )
                            VALUES (
                                new.chunk_crc32,
                                new.chunk_uuid,
                                new.chunk_hash,
                                new.chunk_size,
                                new.chunk_maxsize_code
                            )
                            RETURNING id INTO _chunk_id;
                    END IF;

                    -- INSERT OR IGNORE INTO chunk_per_host
                    BEGIN
                        INSERT INTO chunk_per_host(chunk_id, host_id)
                            VALUES (_chunk_id, new.host_id);
                    EXCEPTION
                        WHEN unique_violation THEN
                            RAISE NOTICE 'Duplicate in chunk_per_host';
                    END;

                    RETURN new;
                ELSIF (TG_OP = 'DELETE') THEN
                    DELETE FROM chunk_per_host WHERE id = old.id;

                    RETURN old;
                END IF;
            END;
            $$ LANGUAGE plpgsql
            """)

        __CREATE_CHUNK_PER_HOST_VIEW_TRIGGER_POSTGRESQL = _dd("""\
            CREATE TRIGGER chunk_per_host_view_trigger
                INSTEAD OF INSERT OR DELETE ON chunk_per_host_view
                FOR EACH ROW
                EXECUTE PROCEDURE chunk_per_host_view_trigger_proc()
            """)

        CREATE_CHUNK_PER_HOST_VIEW_TRIGGERS = {
            'sqlite':
                [__CREATE_CHUNK_PER_HOST_VIEW_INSERT_TRIGGER_SQLITE,
                 __CREATE_CHUNK_PER_HOST_VIEW_DELETE_TRIGGER_SQLITE],
            'postgresql':
                [__CREATE_CHUNK_PER_HOST_VIEW_TRIGGER_PROC_POSTGRESQL,
                 __CREATE_CHUNK_PER_HOST_VIEW_TRIGGER_POSTGRESQL]
        }


        # Arguments:
        # :host_id
        # :crc32
        # :uuid
        # :hash
        # :size
        # :maxsize_code
        INSERT_CHUNK_PER_HOST_VIEW = _dd("""\
            INSERT INTO chunk_per_host_view(
                    host_id,

                    chunk_crc32,
                    chunk_uuid,
                    chunk_hash,
                    chunk_size,
                    chunk_maxsize_code
                )
                VALUES (:host_id, :crc32, :uuid, :hash, :size, :maxsize_code)
            """)


        @classmethod
        def find_duplicate_chunk_uuids(cls, chunks, rdbw):
            """
            Given original chunks, find the UUIDs of the already existing
            chunks which match the given ones by hash and size.

            @type chunks: col.Iterable

            @type rdbw: DatabaseWrapperSQLAlchemy

            @return: the mapping from no-duplicate-tested chunk UUID
                to the already existing chunk UUIDs.
            @rtype: col.Mapping
            """
            m = models
            chunks = list(chunks)  # will be iterated multiple times

            if chunks:
                find_dupes_expr = \
                    m.Chunk.select_duplicates_expr(chunks)
                bound_param_values = \
                    m.Chunk.select_duplicates_bindparam_values(chunks)

                rows = rdbw.execute(find_dupes_expr, bound_param_values)

                return {ChunkUUID.safe_cast_uuid(row.orig_uuid):
                                ChunkUUID.safe_cast_uuid(row.uuid)
                            for row in rows}
            else:
                return {}


        @classmethod
        def get_chunks_by_uuids(cls, uuids, rdbw):
            """Get the chunks by their UUIDs.

            @note: runs a single query with a large amount of IN arguments.

            @param uuids: the iterable over chunk UUIDs.
            @type uuids: col.Iterable

            @type rdbw: DatabaseWrapperSQLAlchemy

            @return: the iterable over C{ChunkInfo} objects.
            @rtype: col.Mapping
            """
            m = models

            cl_select = sql_select([m.chunks.c.crc32,
                                    m.chunks.c.uuid,
                                    m.chunks.c.hash,
                                    m.chunks.c.size,
                                    m.chunks.c.maxsize_code],
                                   whereclause=m.chunks.c.uuid.in_(uuids))

            return (ChunkInfo(  # ChunkInfo-specific
                              crc32=row.crc32,
                                # Chunk-specific
                              uuid=row.uuid,
                              hash=str(row.hash),
                              size=row.size,
                              maxsize_code=int(row.maxsize_code))
                        for row in rdbw.execute(cl_select))


        @classmethod
        def get_chunks_count(cls, rdbw):
            """Get the number of chunks in the system.

            @type rdbw: DatabaseWrapperSQLAlchemy

            @return: the number of chunks
            @rtype: numbers.Integral
            """
            cl_select_count = sql_select([func.count('*')],
                                         from_obj=models.chunks)

            return rdbw.execute(cl_select_count).scalar()


        @classmethod
        def delete_chunks(cls, chunk_uuids, rdbw):
            """Given an iterable of chunk UUIDs, delete them from the DB.

            @type chunk_uuids: col.Iterable
            @type rdbw: DatabaseWrapperSQLAlchemy
            """
            # Prepare delete expression early, so it doesn't get recreated
            # on every loop iteration
            delete_expr = models.chunks.delete(models.chunks.c.uuid ==
                                                   bindparam('chunk_uuid'))
            assert (str(delete_expr) ==
                        'DELETE FROM chunk WHERE chunk.uuid = :chunk_uuid'), \
                   str(delete_expr)

            for u in chunk_uuids:
                try:
                    rdbw.execute(delete_expr, {'chunk_uuid': u})
                except:
                    logger.error("Couldn't delete chunk %r, ignoring", u)


        @classmethod
        def add_chunks(cls, chunks_to_add, rdbw):
            """
            Given an iterable of chunks, add them to the DB (no matter if they
            already existed before, with such unique UUIDs).

            The chunks should NOT have internal duplication! It may cause
            loss of the information: the duplicated chunks won't be added,
            though they probably still be referred by the later code.

            @type chunks_to_add: col.Iterable
            @type rdbw: DatabaseWrapperSQLAlchemy

            @todo: this function is unsafe for large volumes, and will
                definitely fail if the dataset contains too many chunks.
            """
            m = models

            chunks_to_add_by_uuid = {ch.uuid: ch
                                         for ch in chunks_to_add}
            logger.verbose('Want to unconditionally add %i chunk(s)',
                           len(chunks_to_add_by_uuid))

            # Take all the wanted chunks UUIDs from the DB which are already
            # present.
            if chunks_to_add_by_uuid:
                existing_uuids_expr = \
                    sql_select([m.chunks.c.uuid],
                               whereclause=m.chunks.c.uuid.in_(
                                               chunks_to_add_by_uuid.keys()))
                existing_uuids = rdbw.execute(existing_uuids_expr)
            else:
                existing_uuids = []

            # Are there any chunk UUIDs which are required but not yet present?
            new_uuids = set(chunks_to_add_by_uuid.iterkeys()) \
                        - set(u[0] for u in existing_uuids)
            logger.verbose('Among %i chunk(s), %i chunk UUID(s) are new',
                           len(chunks_to_add_by_uuid), len(new_uuids))

            if new_uuids:
                new_chunks_candidates = (chunks_to_add_by_uuid[u]
                                             for u in new_uuids)

                # We need to deduplicate the chunks upon
                # the (hash, size) tuple.
                unique_chunks = {(c.hash, c.size()): c
                                     for c in new_chunks_candidates}

                # We'll use it in multiple iterations, so let's use convenient
                # .viewvalues()
                new_chunks = unique_chunks.viewvalues()
                del unique_chunks  # help GC

                new_unique_chunk_uuids = frozenset(c.uuid for c in new_chunks)
                if new_uuids != new_unique_chunk_uuids:
                    logger.error('Inner duplication found, these chunks '
                                     'may be lost: %r',
                                 new_uuids - new_unique_chunk_uuids)
                del new_unique_chunk_uuids  # help GC

                if __debug__:
                    # new_chunks are too large, let's log just their number
                    logger.debug('Force adding %i chunk(s)',
                                 len(new_chunks))

                chunk_rows = [{'crc32': ch.crc32,
                               'uuid': ch.uuid,
                               'hash': ch.hash,
                               'size': ch.size(),
                               'maxsize_code': ch.maxsize_code}
                                  for ch in new_chunks]
                del new_chunks  # help GC

                if chunk_rows:
                    rdbw.execute(m.chunks.insert(), chunk_rows)


        @classmethod
        def delete_dataset_orphaned_chunks(cls, rdbw):
            """Delete all the chunks which are no more referred by any dataset.

            @type rdbw: DatabaseWrapperSQLAlchemy
            """
            rdbw.execute(models.Chunk.delete_orphaned_chunks())


        @classmethod
        @contract_epydoc
        def store_backup_chunks_blocks(cls,
                                       my_uuid, dataset_uuid, chunks, rdbw):
            """
            Given the chunks sequence, store their structure
            (together) in the DB.

            @param my_uuid: the host UUID; used to validate.
                Can be C{None} if validation is not required, but this
                should not be abused.
            @type my_uuid: UUID, NoneType

            @param dataset_uuid: the UUID of the dataset
            @type dataset_uuid: UUID

            @param chunks: the iterable/sequence of chunks for the dataset.
                Among them, there may be some pairs of chunks which share
                the same size and hash.
                The INSERT trigger should deal with them properly.
            """
            if my_uuid is not None:
                # Use extra validation
                host_row = rdbw.query_value(Queries.Inhabitants
                                                   .SELECT_HOST_BY_UUID,
                                            {'uuid': my_uuid})
                assert host_row is not None
                _host_id = host_row.id
            else:
                _host_id = None

            ds = Queries.Datasets.get_dataset_by_uuid(
                     dataset_uuid=dataset_uuid,
                     host_uuid=my_uuid, rdbw=rdbw)

            assert ds is not None
            _group_id = ds._ugroup_id

            # Add chunks and chunk-to-file mappings altogether
            all_chunks_iter = ({'host_id': _host_id,
                                'crc32': crc32_to_signed(chunk.crc32),
                                'uuid': chunk.uuid,
                                'hash': buffer(chunk.hash),
                                'size': chunk.size(),
                                'maxsize_code': chunk.maxsize_code}
                                   for chunk in chunks)

            rdbw.querymany(Queries.Chunks.INSERT_CHUNK_PER_HOST_VIEW,
                           all_chunks_iter)

            # Add blocks
            all_blocks_iter = ({'offset_in_file': block.offset_in_file,
                                'offset_in_chunk': block.offset_in_chunk,
                                'block_size': block.size,
                                'chunk_hash': buffer(chunk.hash),
                                'chunk_size': chunk.size(),
                                'chunk_uuid': chunk.uuid,
                                'file_group_id': _group_id,
                                'file_fp': block.file.fingerprint}
                                   for chunk in chunks
                                   for block in chunk.blocks)

            rdbw.querymany(Queries.Blocks.INSERT_BLOCK_VIEW,
                           all_blocks_iter)



    class Blocks(object):
        """
        All DB queries related to the particular chunk blocks of the backups.
        """

        __slots__ = tuple()


        CREATE_BLOCK_VIEW = _dd("""\
            CREATE VIEW block_view AS
                SELECT
                    block.id AS id,
                    block.offset_in_file AS offset_in_file,
                    block.offset_in_chunk AS offset_in_chunk,
                    block.size AS size,

                    chunk.uuid AS chunk_uuid,

                    file.group_id AS file_group_id,
                    file.fingerprint AS file_fingerprint
                FROM
                    block
                    INNER JOIN chunk
                        ON block.chunk_id = chunk.id
                    INNER JOIN file
                        ON block.file_id = file.id
            """)

        __CREATE_BLOCK_VIEW_INSERT_TRIGGER_SQLITE = _dd("""\
            CREATE TRIGGER IF NOT EXISTS block_view_insert_trigger
                INSTEAD OF INSERT ON block_view
                FOR EACH ROW
                BEGIN
                    INSERT OR ROLLBACK INTO block(
                            chunk_id,
                            file_id,
                            offset_in_file,
                            offset_in_chunk,
                            size
                        )
                        VALUES (
                            (
                                SELECT id
                                    FROM chunk
                                    WHERE uuid = new.chunk_uuid
                            ),
                            (
                                SELECT id
                                    FROM file
                                    WHERE
                                        group_id = new.file_group_id AND
                                        fingerprint = new.file_fingerprint
                            ),
                            new.offset_in_file,
                            new.offset_in_chunk,
                            new.size
                        );
                END
            """)

        __CREATE_BLOCK_VIEW_DELETE_TRIGGER_SQLITE = _dd("""\
            CREATE TRIGGER IF NOT EXISTS block_view_insert_trigger
                INSTEAD OF DELETE ON block_view
                FOR EACH ROW
                BEGIN
                    DELETE FROM block WHERE id = old.id;
                END
            """)

        __CREATE_BLOCK_VIEW_TRIGGER_PROC_POSTGRESQL = _dd("""\
            CREATE OR REPLACE FUNCTION block_view_trigger_proc()
            RETURNS TRIGGER AS $$
            BEGIN
                IF (TG_OP = 'INSERT') THEN
                    INSERT INTO block(
                            chunk_id,
                            file_id,
                            offset_in_file,
                            offset_in_chunk,
                            size
                        )
                        VALUES (
                            (
                                SELECT id
                                    FROM chunk
                                    WHERE uuid = new.chunk_uuid
                            ),
                            (
                                SELECT id
                                    FROM file
                                    WHERE
                                        group_id = new.file_group_id AND
                                        fingerprint = new.file_fingerprint
                            ),
                            new.offset_in_file,
                            new.offset_in_chunk,
                            new.size
                        );

                    RETURN new;
                ELSIF (TG_OP = 'DELETE') THEN
                    DELETE FROM block WHERE id = old.id;

                    RETURN old;
                END IF;
            END;
            $$ LANGUAGE plpgsql
            """)

        _CREATE_BLOCK_VIEW_TRIGGER_POSTGRESQL = _dd("""\
            CREATE TRIGGER block_view_trigger
                INSTEAD OF INSERT OR DELETE ON block_view
                FOR EACH ROW
                EXECUTE PROCEDURE block_view_trigger_proc()
            """)

        CREATE_BLOCK_VIEW_TRIGGERS = {
            'sqlite':
                [__CREATE_BLOCK_VIEW_INSERT_TRIGGER_SQLITE,
                 __CREATE_BLOCK_VIEW_DELETE_TRIGGER_SQLITE],
            'postgresql':
                [__CREATE_BLOCK_VIEW_TRIGGER_PROC_POSTGRESQL,
                 _CREATE_BLOCK_VIEW_TRIGGER_POSTGRESQL]
        }


        # Arguments:
        # :offset_in_file - block offset_in_file
        # :offset_in_chunk - block offset_in_chunk
        # :block_size - block size
        # :chunk_hash - chunk hash | either UUID or hash+size are used for key
        # :chunk_size - chunk size |
        # :chunk_uuid - chunk UUID |
        # :group_id - file reference to user group id
        # :file_fp - file fingerprint
        INSERT_BLOCK_VIEW = _dd("""\
            INSERT INTO block_view(
                    offset_in_file,
                    offset_in_chunk,
                    size,
                    chunk_uuid,
                    file_group_id,
                    file_fingerprint
                )
                VALUES (
                    :offset_in_file,
                    :offset_in_chunk,
                    :block_size,
                    (
                        SELECT uuid
                            FROM chunk
                            WHERE
                                (:chunk_hash IS NOT NULL AND
                                 hash = :chunk_hash AND
                                 size = :chunk_size)
                                OR
                                (:chunk_hash IS NULL AND
                                 uuid = :chunk_uuid)
                    ),
                    :file_group_id,
                    :file_fp
                )
            """)

        @classmethod
        @duration_logged()
        @contract_epydoc
        def bind_blocks_to_files(cls, uploader_host_uuid, dataset_uuid, blocks,
                                 rdbw):
            """
            Given the list of the blocks referring to some files,
            mark the bindings between them and files and chunks.

            @param uploader_host_uuid: the host UUID which uploaded the chunks;
                used to validate; may be C{None} if validation is not needed,
                but this behaviour should not be abused.
            @type uploader_host_uuid: UUID, NoneType

            @param dataset_uuid: the UUID of the dataset.
            @type dataset_uuid: UUID

            @param blocks: the (possibly non-reiterable) Iterable of the
                           block/chunk pairs.
            @type blocks: col.Iterable

            @type rdbw: DatabaseWrapperSQLAlchemy
            """
            # The user group is obviously common for all the files
            # (referred by the blocks).
            validated_ds = Queries.Datasets.get_dataset_by_uuid(
                               dataset_uuid=dataset_uuid,
                               host_uuid=uploader_host_uuid,
                               rdbw=rdbw)

            assert validated_ds is not None
            _group_id = validated_ds._ugroup_id

            # Add blocks
            all_blocks = ({'offset_in_file': block.offset_in_file,
                           'offset_in_chunk': block.offset_in_chunk,
                           'block_size': block.size,
                           'chunk_hash': buffer(chunk.hash),
                           'chunk_size': chunk.size(),
                           'chunk_uuid': chunk.uuid,
                           'file_group_id': _group_id,
                           'file_fp': block.file.fingerprint}
                              for block, chunk in blocks)

            rdbw.querymany(cls.INSERT_BLOCK_VIEW,
                           all_blocks)


        # Arguments:
        # :host_uuid - used only to validate the access to the data
        # :ds_uuid
        SELECT_BLOCKS_FOR_DATASET = _dd("""\
            SELECT DISTINCT
                    file_local_view.uuid AS file_local_uuid,
                    block.offset_in_file AS offset_in_file,
                    block.offset_in_chunk AS offset_in_chunk,
                    block.size AS size,
                    chunk.uuid AS chunk_uuid
                FROM
                    host_view
                    INNER JOIN membership
                        ON membership.user_id = host_view.user_id
                    INNER JOIN dataset
                        ON dataset.group_id = membership.group_id
                    INNER JOIN file_local_view
                        ON dataset.id = file_local_view.dataset_id
                    INNER JOIN block
                        ON file_local_view.file_id = block.file_id
                    INNER JOIN chunk
                        ON block.chunk_id = chunk.id
                WHERE
                    host_view.uuid = :host_uuid AND
                    dataset.uuid = :ds_uuid
            """)

        @classmethod
        @contract_epydoc
        def get_blocks_for_dataset(cls,
                                   host_uuid, ds_uuid,
                                   file_uuid_map, chunk_uuid_map,
                                   rdbw):
            """
            Given the host UUID and the dataset UUID,
            return all the blocks in this dataset as a list.

            @param host_uuid: the host UUID
            @type host_uuid: UUID

            @param ds_uuid: the dataset UUID.
            @type ds_uuid: UUID

            @param file_uuid_map: The map which allows to find a file
                                  by its UUID.
            @type file_uuid_map: col.Mapping

            @type chunk_uuid_map: col.Mapping

            @type rdbw: DatabaseWrapperSQLAlchemy

            @returns: the (non-reiterable) iterable of the Chunk.Block-s.
            @rtype: col.Iterable

            @todo: long operation, takes 2.5 seconds on test dataset.
                   (though it is lazy now, may be not so long).
            """
            rows = rdbw.query(cls.SELECT_BLOCKS_FOR_DATASET,
                              {'host_uuid': host_uuid, 'ds_uuid': ds_uuid})
            return (Chunk.Block(file=file_uuid_map[row.file_local_uuid],
                                offset_in_file=row.offset_in_file,
                                offset_in_chunk=row.offset_in_chunk,
                                size=row.size,
                                chunk=chunk_uuid_map[row.chunk_uuid])
                        for row in rows)



    ALL_CREATE_QUERIES = (
        # * Inhabitants
        Inhabitants.CREATE_VIEW_USER_IN_GROUP,
        Inhabitants.CREATE_HOST_VIEW,
        Inhabitants.CREATE_HOST_VIEW_TRIGGERS,
        Inhabitants.CREATE_VIEW_USER_HOSTS,
        # * Local settings
        Settings.CREATE_VIEW_SETTING,
        # * Datasets
        # * Files
        Files.CREATE_FILE_VIEW,
        Files.CREATE_FILE_LOCAL_VIEW,
        Files.CREATE_FILE_LOCAL_VIEW_TRIGGERS,
        # * Chunks
        Chunks.CREATE_CHUNK_PER_HOST_VIEW,
        Chunks.CREATE_CHUNK_PER_HOST_VIEW_TRIGGERS,
        # * Blocks
        Blocks.CREATE_BLOCK_VIEW,
        Blocks.CREATE_BLOCK_VIEW_TRIGGERS,
        # * Some more views
        Datasets.CREATE_VIEW_DATASET_FILE_STATS,
        Datasets.CREATE_VIEW_DATASET_CHUNK_STATS,
        Datasets.CREATE_VIEW_DATASET_CHUNK_STATS_ALL,
    )



class DatabaseWrapperSQLAlchemy(IPrintable):
    """
    Use it as a "with"-context, to issue multiple queries at once
    (by query() method) with a common commit.

    Use its C{.session} object variable to access the SQLAlchemy session
    inside the context;
    using the whole class as a context manager will cause the session to be
    automatically committed as soon as leaving the context.
    """
    __slots__ = ('_factory', '__session_cls', 'session',
                 '__uuid', '__enter_time')


    def __init__(self, factory, session_cls, timeout):

        logger_db.verbose('Creating connection to %r with timeout %r sec',
                          factory, timeout)

        self._factory = factory
        self.__session_cls = session_cls

        self.session = self.__session_cls()

        self.__uuid = gen_uuid()
        self.__enter_time = datetime.utcnow()
        logger_db.verbose('%s::create', self.__uuid)

        # But is the database available in fact?
        if self.dialect_name == 'sqlite':
            file_path = self.db_name
            if file_path != ':memory:':
                # The file path refers to a physical disk.
                file_path = os.path.abspath(file_path)  # for safety

                # Can we use the file path?
                file_dir = os.path.dirname(file_path)
                if not os.path.exists(file_dir):
                    # Try to create
                    os.makedirs(file_dir)


    @property
    def dialect_name(self):
        return self._factory._engine.dialect.name


    @property
    def rdb_url(self):
        """Return the url used to connect to the DB.

        @rtype: basestring
        """
        return unicode(self._factory._engine.url)


    @property
    def db_name(self):
        """
        Return the used database name.

        Under SQLite3, it returns the path to the file.

        @rtype: basestring
        """
        return self._factory._engine.url.database


    def __str__(self):
        return u'factory={!r}'.format(self._factory)


    def __enter__(self):
        return self


    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is exc_val is exc_tb is None:
            self.commit()
        else:
            logger_db.exception('Leaving %r: (%r, %r, %r)',
                                self,
                                exc_type, exc_val, exc_tb)
        self.close()


    def commit(self):
        self.session.commit()


    def rollback(self):
        self.session.rollback()


    def execute(self, *args, **kwargs):
        """Call the C{.execute()} method of the inner C{.session}."""
        return self.session.execute(*args, **kwargs)

    def close(self):
        self.session.close()

    def q(self, *args, **kwargs):
        """
        Call the C{.query()} method of the inner C{.session}.
        """
        return self.session.query(*args, **kwargs)


    def query(self, query_str, *args, **kwargs):
        """
        Perform a raw SQL query.
        """
        return self.execute(query_str, *args, **kwargs)


    def querymany(self, query_str, kwargs_seq):
        for kwargs in kwargs_seq:
            # _conn.execute(query_str, *args)
            self.execute(query_str, kwargs)


    def query_value(self, query_str, *args):
        """
        Execute an SQL query similarly to C{.query()}, but assume
        that the result contains either a single row or result
        or doesn't contain the rows as all.
        As a side-effect, in the debug mode it is asserted that the result
        indeed contains 0 or 1 rows.

        @param query_str: The SQL query to query the DB with.
        @param *args: All the remaining arguments will be passed unchanged
                      to the cursor .execute() method (thus, for example,
                      substituted into the query by the SQL binding).

        @returns: a single row of the result, or None if there is no results.
        @rtype: cursor, NoneType

        @raises ValueError: (in Debug-mode only) if there is more than 1
                            results in the response.
        """
        logger_db.verbose('%s::query_value: %r (%r)',
                          self.__uuid, query_str, args)

        rows = self.query(query_str, *args)
        row = rows.fetchone()

        # Now make sure there is no double results
        if __debug__:
            if row is None and rows.closed:
                pass  # we have 0 results so we shall return None
            else:
                # We have at least 1 result, so let's try to fetch one more
                more_row = rows.fetchone()
                if more_row is None and rows.closed:
                    pass  # couldn't fetch the second result
                else:
                    raise ValueError('More than 1 results for {!r} / {!r}'
                                         .format(query_str, args))

        return row



class DatabaseWrapperFactory(IPrintable):
    """
    A factory that is capable of creating the database wrappers
    as soon as it is initialized.

    @note: do NOT log C{self._engine} inside the code, it may cause issues
        on Win32 for cyrillic-named users.
    """
    __slots__ = ('_engine', '__session_cls')

    __adapters_registered = False


    @contract_epydoc
    def __init__(self, url, sqlalchemy_echo):
        """
        @param url: what url should be used to connect to SQLAlchemy.
        @type url: basestring, sqlalchemy.engine.url.URL

        @param sqlalchemy_echo: whether SQLAlchemy should echo
            all SQL requests to the logs.
        @type sqlalchemy_echo: bool
        """
        cls = self.__class__

        if isinstance(url, basestring):
            try:
                url = sqlalchemy.engine.url.make_url(url)
            except:
                raise Exception(u'rel-db-url setting must be formatted as '
                                u'rel-db-url=engine://username:password@'
                                u'host:port/database '
                                u'but now it contains %r',
                                    url)

        assert isinstance(url, sqlalchemy.engine.url.URL), repr(url)

        dialect_name = url.get_dialect().name

        if not cls.__adapters_registered:
            cls.__register_adapters_converters(dialect_name)
            cls.__adapters_registered = True

        # Some backend-specific operations
        if dialect_name == 'sqlite':
            # TODO: native_datetime is needed for SQLAlchemy to work together
            # with sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES,
            # so as long as these are removed, native_datatime
            # can be removed too.
            _connect_args = {'detect_types': sqlite3.PARSE_DECLTYPES |
                                             sqlite3.PARSE_COLNAMES,
                             'timeout': DEFAULT_DB_TIMEOUT}
            _extra_create_engine_args = {'native_datetime': True}
            _extra_connect_listener = \
                lambda connect_db, connect_record: \
                    connect_db.execute('PRAGMA foreign_keys = ON')
        else:
            # TODO: temporary; fix to proper pool
            _connect_args = {}
            _extra_create_engine_args = {'poolclass': sqlalchemy.pool.NullPool}
            _extra_connect_listener = None
        # End of backend-specific operations

        _extra_create_engine_args.update(echo=sqlalchemy_echo)

        # Let the default SQLAlchemy model classes be used with this database
        self._engine = models.Base.metadata.bind \
                     = sqlalchemy.create_engine(url,
                                                connect_args=_connect_args,
                                                **_extra_create_engine_args)

        if _extra_connect_listener is not None:
            event.listen(self._engine, 'connect', _extra_connect_listener)

        self.__session_cls = \
            orm.scoped_session(orm.sessionmaker(bind=self._engine))


    @property
    def is_schema_initialized(self):
        """
        Returns a bool indicating whether the tables in the DB are initialized.
        """
        table_names = self._engine.table_names()
        table_names_present = bool(table_names)
        logger.debug('Checking whether any tables present: %r',
                     table_names_present)
        return table_names_present


    def create_schema(self):
        """
        Create all the tables in the database according to
        the SQLAlchemy models derived from C{Base}.
        """
        models.Base.metadata.create_all()


    @classmethod
    def __register_adapters_converters(cls, dialect_name):
        _uuid_classes_to_adapt = [UUID] + typed_uuids.ALL_TYPED_UUIDS

        if dialect_name == 'sqlite':
            # TimeEx <-> "time" conversion
            sqlite3.register_adapter(TimeEx, TimeEx.sqlite3_adapt)
            sqlite3.register_converter('TIME', TimeEx.sqlite3_convert)

            # UUID (and various typed ones) <-> "uuid"
            for u_class in _uuid_classes_to_adapt:
                sqlite3.register_adapter(u_class, uuid_adapt_sqlite)
            sqlite3.register_converter('UUIDTEXT', uuid_convert_sqlite)

            # FINGERPRINT <-> "Fingerprint"
            sqlite3.register_adapter(Fingerprint,
                                     Fingerprint.sqlite3_adapt)
            sqlite3.register_converter('FINGERPRINT',
                                       Fingerprint.sqlite3_convert)

            # CRC32 -> int
            sqlite3.register_converter('CRC32', crc32_convert)

        elif dialect_name == 'postgresql':
            if psycopg2 is None:
                raise Exception('Please install python-psycopg2!')

            # UUID
            psycopg2.extras.register_uuid()

            # Fingerprint
            psycopg2.extensions.register_adapter(
                Fingerprint,
                lambda fp: psycopg2.extensions.Binary(fp.as_buffer))

        else:
            raise Exception('Unsupported RelDB dialect {!r}'.format(
                                dialect_name))


    @property
    def url(self):
        return self._engine.url


    def __str__(self):
        return u'url={!r}'.format(self.url)


    def __call__(self, timeout=DEFAULT_DB_TIMEOUT):
        """Create a new database wrapper with the pre-stored settings.

        @param timeout: timeout for DB access operation (in seconds).
        @type timeout: numbers.Real

        @returns: A new database wrapper instance.
        @rtype: DatabaseWrapperSQLAlchemy
        """
        return DatabaseWrapperSQLAlchemy(self,
                                         self.__session_cls,
                                         timeout)


@contract_epydoc
def run_init_query_on_wrapper(rdbw, query):
    """Given a single initialization query, run it over a wrapper.

    Made a public function so it can be used by the migration scripts as well.

    Each query may contain:
      * either a single string with a raw query,
      * or a dictionary mapping the dialect name to the dialect-specific query,
      * or a sequence of queries.

    @type rdbw: DatabaseWrapperSQLAlchemy

    @type query: basestring, col.Mapping
    """
    if isinstance(query, basestring):
        # Run the query as is
        rdbw.query(query)
    elif isinstance(query, col.Mapping):
        # Get the proper query for this dialect (if it exists)
        per_dialect_query = query.get(rdbw.dialect_name)
        if per_dialect_query is not None:
            run_init_query_on_wrapper(rdbw, per_dialect_query)
    elif isinstance(query, col.Iterable):
        for sub_query in query:
            run_init_query_on_wrapper(rdbw, sub_query)
    else:
        raise TypeError('Unsupported query {!r}'.format(query))



#
# Functions
#

@contract_epydoc
def create_db_schema(rdbw, extra_queries):
    """
    Initialize the database schema and structure.

    @type rdbw: DatabaseWrapperSQLAlchemy

    @param extra_queries: An iterable of additional queries which must
                          be executed for the database creation.
    @type extra_queries: col.Iterable
    """
    logger.debug('Creating the schema from SQLAlchemy...')
    # First of all, initialize all the tables upon the SQLAlchemy models.
    rdbw._factory.create_schema()

    logger.debug('Creating the schema from raw queries...')
    # Run the queries initializing the database schema
    run_init_query_on_wrapper(rdbw,
                              chain(Queries.ALL_CREATE_QUERIES,
                                    extra_queries))

    _session = rdbw.session

    logger.debug('Configuring settings')
    # Initialize the local settings
    _session.add_all(models.LocalSetting(name=k, value=v)
                         for k, v in Queries.LocalSettings
                                            .ALL_SETTINGS
                                            .iteritems())

    # Initialize the default setting names
    _session.add_all(models.SettingName(name=name)
                         for name in Queries.Settings.ALL_SETTINGS.iterkeys())





