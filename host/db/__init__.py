#!/usr/bin/python
"""Host-specific database access layer.

@var RDB: A database-access wrapper factory.
@type RDB: DatabaseWrapperFactory
"""

#
# Imports
#

from __future__ import absolute_import
import collections as col
import sys
import logging
import numbers
import os
import posixpath
import random
from base64 import b64decode
from collections import namedtuple, defaultdict
from datetime import datetime, timedelta
from itertools import chain, imap
from operator import attrgetter, itemgetter
from textwrap import dedent as _dd
from types import NoneType
from uuid import UUID

import sqlalchemy
from sqlalchemy.sql.expression import (
    select as sql_select, update as sql_update,
    insert as sql_insert, delete as sql_delete,
    bindparam)

from contrib.dbc import contract_epydoc, consists_of

from common.abstract_dataset import AbstractBasicDatasetInfo
from common.chunks import (
    Chunk, ChunkInfo, ChunkFromFilesFinal, LocalPhysicalFileStateRel)
from common.crypto import gen_empty_key
from common.datasets import DatasetWithDirectories, DatasetOnChunks
from common.db import (
    DatabaseWrapperSQLAlchemy, DatabaseWrapperFactory, Queries,
    AbstractEncodedSettings, create_db_schema)
from common.db.types import crc32_to_signed, crc32_to_unsigned
from common.inhabitants import Host, UserGroup, UserWithGroups
from common.itertools_ex import sorted_groupby
from common.path_ex import decode_posix_path, encode_posix_path
from common.typed_uuids import DatasetUUID, UserGroupUUID
from common.utils import (
    coalesce, NULL_UUID, gen_uuid, open_wb, duration_logged, fix_strptime)

from host.datasets import MyDatasetOnChunks

from .settings_cache import HostSettingsCache

from . import models



#
# Constants and logging
#

logger = logging.getLogger(__name__)
logger_settings = logging.getLogger('db.settings')

SETTINGS_CACHE_PERIOD = timedelta(minutes=1)

RDB = None

SETTINGS_CACHE = None
NON_TABLE_SETTINGS_CACHE = None



#
# Classes
#


class NonDBSettings(object):
    MY_URLS = 'my urls'

    ALL_SETTINGS = [MY_URLS]



class HostQueries(Queries):
    """A dummy namespace for all the common host-specific queries.

    @note: you do not ever need to instance this class.
    """

    __slots__ = tuple()


    class HostSettings(Queries.Settings):
        """All RDB (host-only) queries related to the various host settings.

        @cvar ALL_LOCAL_SETTINGS: The settings which are stored only locally,
                                  but never migrated to the Node.
                                  It is the mapping from the setting name
                                  to the default value.
        @type ALL_LOCAL_SETTINGS: dict
        """
        _AES = AbstractEncodedSettings
        base = Queries.Settings

        __slots__ = tuple()

        SSL_PRIVATE_KEY = 'SSL private key'
        SSL_CERTIFICATE = 'SSL certificate'

        ALL_LOCAL_SETTINGS = {SSL_PRIVATE_KEY: None,
                              SSL_CERTIFICATE: None}

        ALL_SETTINGS = dict(base.ALL_SETTINGS.items() +
                            ALL_LOCAL_SETTINGS.items())

        _SETTING_SERIALIZER = defaultdict(
            base._SETTING_SERIALIZER.default_factory,
            dict(base._SETTING_SERIALIZER.items() +
                 {
                   SSL_PRIVATE_KEY: _AES._TYPE_SSL_PKEY,
                   SSL_CERTIFICATE: _AES._TYPE_SSL_CERT,
                 }.items())
        )


        INSERT_MY_SETTING = _dd("""\
            INSERT OR ROLLBACK INTO setting(
                    setting_name_id,
                    host_id,
                    value,
                    update_time
                )
                VALUES
                (
                    (SELECT id FROM setting_name WHERE name = :name),
                    (SELECT id FROM my_host),
                    :value,
                    :update_time
                )
            """)

        @classmethod
        def init(cls, setting_name, setting_value, rdbw):
            """
            Initialize the setting.
            This setting should not have been having a value before!
            """
            global SETTINGS_CACHE

            time = datetime.utcnow()

            # Force cache update
            if SETTINGS_CACHE is not None:
                SETTINGS_CACHE[setting_name] = \
                    HostSettingsCache.Item(value=setting_value,
                                           last_update_time=time)

            # Write the value to the RDB
            res = rdbw.query(cls.INSERT_MY_SETTING,
                             {'name': setting_name,
                              'value': cls._encode(setting_name,
                                                   setting_value),
                              'update_time': time})
            logger_settings.debug('initializing %r to %r %s the cache',
                                  setting_name,
                                  setting_value,
                                  'using' if SETTINGS_CACHE is not None
                                          else 'not using')


        UPSERT_MY_SETTING = _dd("""\
            INSERT OR REPLACE INTO setting(
                    setting_name_id,
                    host_id,
                    value,
                    update_time
                )
                VALUES
                (
                    (SELECT id FROM setting_name WHERE name = :name),
                    (SELECT id FROM my_host),
                    :value,
                    :update_time
                )
            """)

        @classmethod
        @contract_epydoc
        def set(cls,
                setting_name, setting_value,
                setting_time=None, direct=False,
                rdbw=None):
            """
            Set the setting, no matter was any value present
            or absent for this setting before.

            @param direct: whether the value should be written
                           without the conversion.
            @type direct: bool

            @type rdbw: DatabaseWrapperSQLAlchemy
            """
            global SETTINGS_CACHE
            assert rdbw is not None

            if direct:
                encoded_value = setting_value
                value = cls._decode(setting_name, setting_value)
            else:
                encoded_value = cls._encode(setting_name, setting_value)
                value = setting_value

            # If setting_time is empty, it will be set to the current time.
            setting_time = coalesce(setting_time, datetime.utcnow())

            # Force cache update
            if SETTINGS_CACHE is not None:
                SETTINGS_CACHE[setting_name] = \
                    HostSettingsCache.Item(value=value,
                                           last_update_time=setting_time)

            # Write to the RDB.
            res = rdbw.query(cls.UPSERT_MY_SETTING,
                             {'name': setting_name,
                              'value': encoded_value,
                              'update_time': setting_time})

            logger_settings.debug('setting %r to %r %s the cache',
                                  setting_name,
                                  value,
                                  'using' if SETTINGS_CACHE is not None
                                          else 'not using')


        SELECT_MY_SETTING = _dd("""\
            SELECT
                    setting_view.value AS value,
                    setting_view.update_time AS update_time
                FROM
                    setting_view
                    INNER JOIN my_host
                        ON setting_view.host_id = my_host.id
                    WHERE setting_view.name = :name
            """)

        @classmethod
        def get(cls, setting_name, default_value=None):
            """
            Given a setting name, return its value (or a default value
            instead).

            @note: The value of the setting is decoded to the proper type
                   if needed.
            """
            global SETTINGS_CACHE

            found_value = False
            value = None

            # First, let's try to read the value from the cache
            if SETTINGS_CACHE is None:
                logger_settings.debug('Cache disabled!')
            else:
                try:
                    item = SETTINGS_CACHE[setting_name]
                    value = item.value
                    found_value = True
                except KeyError:
                    pass  # cache miss; will read from RDB

            # If it was missing from the cache (or the cache was missing
            # itself), read it from the RDB.
            if not found_value:
                assert RDB is not None
                with RDB() as rdbw:
                    row = rdbw.query_value(cls.SELECT_MY_SETTING,
                                           {'name': setting_name})
                    logger_settings.debug('reading setting %r from RDB',
                                          setting_name)

                    if row is not None:
                        value = cls._decode(setting_name, row.value)
                        found_value = True
                        # Update the cache as well
                        if SETTINGS_CACHE is not None:
                            SETTINGS_CACHE[setting_name] = \
                                HostSettingsCache.Item(value,
                                                       fix_strptime(
                                                           row.update_time))

            # Even after reading from the RDB, it still may be missing (None).
            # Use the defaults then.
            if value is None:
                value = default_value

            return value


        SELECT_ALL_MY_SETTINGS = _dd("""\
            SELECT
                    setting_view.name AS name,
                    setting_view.value AS value,
                    setting_view.update_time AS update_time
                FROM
                    setting_view
                    INNER JOIN my_host
                        ON setting_view.host_id = my_host.id
            """)

        @classmethod
        def get_all(cls, rdbw):
            """
            Get all settings name/value/last_update_time tuples.
            If the settings are not in the RDB, they are

            @note: The values of the settings are raw and not decoded,
                   i.e. are always strings.
            """
            global SETTINGS_CACHE
            assert SETTINGS_CACHE is not None

            try:
                # First try to read everything from the cache...
                settings_map = SETTINGS_CACHE.get_all()
            except KeyError as e:
                # ... but on any error, read everything from the RDB.

                # Used twice, hence the list below
                _rows = rdbw.query(cls.SELECT_ALL_MY_SETTINGS)

                rows = [(row.name,
                         row.value,
                         fix_strptime(row.update_time))
                            for row in _rows]

                _map = {name: HostSettingsCache.Item(cls._decode(name, value),
                                                     update_time)
                            for name, value, update_time in rows}
                SETTINGS_CACHE.set_all(_map)

                results = rows

            else:
                results = [(name,
                            cls._encode(name, item.value),
                            item.last_update_time)
                               for name, item in settings_map.iteritems()]

            return results


        SELECT_MY_HOST_URLS = _dd("""\
            SELECT host_view.urls AS urls
                FROM
                    my_host
                    INNER JOIN host_view
                        USING(id)
            """)

        @classmethod
        def get_my_urls(cls):
            """
            Get the URLs announced by this host.
            """
            global NON_TABLE_SETTINGS_CACHE

            value = None

            # First, try to read from the cache
            if NON_TABLE_SETTINGS_CACHE is not None:
                try:
                    value = NON_TABLE_SETTINGS_CACHE[NonDBSettings.MY_URLS] \
                                .value
                except KeyError:
                    pass

            # If not in cache, try to read from the RDB
            if value is None:
                assert RDB is not None
                with RDB() as rdbw:
                    row = rdbw.query_value(cls.SELECT_MY_HOST_URLS)
                    value = row.urls if row is not None else None

                # And update the cache.
                if NON_TABLE_SETTINGS_CACHE is not None and value is not None:
                    NON_TABLE_SETTINGS_CACHE[NonDBSettings.MY_URLS] = \
                        HostSettingsCache.Item(value, datetime.utcnow())

            return value



    class HostUsers(Queries.Inhabitants):
        """All RDB (host-only) queries related to the cloud users."""

        __slots__ = tuple()


        @classmethod
        def add_user_to_groups(cls, username, groups, rdbw):
            """Add a user (by their username) to some user groups.

            @type groups: col.Iterable
            @type rdbw: DatabaseWrapperSQLAlchemy
            """
            records = ({'user_name': username, 'ugroup_uuid': group.uuid}
                           for group in groups)

            rdbw.querymany(cls.INSERT_MEMBERSHIP, records)


        @classmethod
        def remove_user_from_groups(cls, username, group_uuids, rdbw):
            """Remove the user from some user groups.

            @type groups: group_uuids
            @type rdbw: DatabaseWrapperSQLAlchemy
            """
            records = ({'user_name': username, 'ugroup_uuid': gr_uuid}
                           for gr_uuid in group_uuids)

            rdbw.querymany(cls.DELETE_MEMBERSHIP, records)


        @classmethod
        def set_my_user_groups(cls, username, new_user_groups, rdbw):
            """Set the user groups for the current user.

            @type rdbw: DatabaseWrapperSQLAlchemy
            """
            present_user_groups = Queries.Inhabitants.get_groups_for_user(
                                      username, rdbw)

            present_gr_uuids = frozenset(g.uuid for g in present_user_groups)
            new_gr_uuids = frozenset(g.uuid for g in new_user_groups)

            to_delete_uuids = present_gr_uuids - new_gr_uuids
            common_uuids = present_gr_uuids | new_gr_uuids

            # Used twice, hence a list rather than generator
            to_add_groups = [gr
                                 for gr in new_user_groups
                                 if gr.uuid not in present_gr_uuids]

            to_update_groups = (gr
                                    for gr in new_user_groups
                                    if gr.uuid in common_uuids)

            logger.debug('Adding groups %r', to_add_groups)
            Queries.Inhabitants.add_ugroups(to_add_groups, rdbw)
            cls.add_user_to_groups(username, to_add_groups, rdbw)

            logger.debug('Updating groups %r', to_update_groups)
            Queries.Inhabitants.update_ugroups(to_update_groups, rdbw)

            logger.debug('Deleting groups %r', to_delete_uuids)
            Queries.Inhabitants.delete_ugroups(to_delete_uuids, rdbw)
            cls.remove_user_from_groups(username, to_delete_uuids, rdbw)


        SELECT_MY_USER_NAME = _dd("""\
            SELECT "user".name AS name
                FROM
                    my_host
                    INNER JOIN host
                        USING(id)
                    INNER JOIN "user"
                        ON host.user_id = "user".id
            """)

        @classmethod
        def get_my_user(cls, rdbw):
            """Find the current user.

            @rtype: UserWithGroups
            """
            row = rdbw.query_value(cls.SELECT_MY_USER_NAME)
            assert row is not None  # as must always have "my user"

            return Queries.Inhabitants.get_user_with_groups(str(row.name),
                                                            rdbw)



    class MyHost(Queries.Inhabitants):
        """
        All RDB (host-only) queries related to the host where the code
        is being executed.
        """

        __slots__ = tuple()


        INSERT_MY_HOST = _dd("""\
            INSERT OR ROLLBACK INTO my_host(id)
                VALUES (:id)
            """)

        @classmethod
        @contract_epydoc
        def configure_my_host(cls,
                              host_uuid, group_uuid, username, digest, port):
            """
            Given an UUID and username of the host, add it to the database
            as the default one for the running host.

            @type host_uuid: UUID
            @type group_uuid: UUID
            @type port: int
            """
            assert RDB is not None

            # TODO: the function must require the argument to be
            # of UserGroupUUID type already
            group_uuid = UserGroupUUID.safe_cast_uuid(group_uuid)

            logger.debug('Configuring my host with UUID %s and name %r',
                         host_uuid, username)

            with RDB() as rdbw:
                _id = Queries.Inhabitants.add_inhabitant(host_uuid, None, rdbw)

                # Encryption key is only temporary now and will be
                # replaced by the real one later.
                dummy_enc_key = gen_empty_key()

                group = UserGroup(uuid=group_uuid,
                                  name=username,
                                  private=True,
                                  enc_key=dummy_enc_key)

                _user_id = Queries.Inhabitants.add_user_with_group(username,
                                                                   group,
                                                                   digest,
                                                                   rdbw=rdbw)

                hostname = '{}/{}'.format(username, random.randint(0, 32767))

                rdbw.query(Queries.Inhabitants.INSERT_HOST,
                           {'id': _id,
                            'user_name': username,
                            'host_name': hostname})
                rdbw.query(cls.INSERT_MY_HOST, {'id': _id})

                # Fill the default settings
                for name, value in Queries.Settings.ALL_SETTINGS.iteritems():
                    if value is not None:
                        HostQueries.HostSettings.init(name, value, rdbw=rdbw)

                HostQueries.HostSettings.set(
                    Queries.Settings.PORT_TO_LISTEN, port, rdbw=rdbw)
            logger.debug('Added my host %s with username %s, port %i',
                         host_uuid, username, port)


        @classmethod
        @contract_epydoc
        def save_last_logged_in(cls, uuid, username):
            """Store the last logged-in user in NULL database.

            @type uuid: UUID
            """
            assert RDB is not None

            with RDB() as rdbw:
                Queries.Inhabitants.add_user(username, '_' * 40, rdbw)
                _id = Queries.Inhabitants.add_inhabitant(uuid, None, rdbw)

                hostname = '{}/{}'.format(username, random.randint(0, 32767))
                rdbw.query(Queries.Inhabitants.INSERT_HOST,
                           {'id': _id,
                            'user_name': username,
                            'host_name': hostname})
                rdbw.query(cls.INSERT_MY_HOST, {'id': _id})


        SELECT_LAST_LOGGED_IN = _dd("""\
            SELECT
                    host_view.uuid AS uuid,
                    "user".name AS name
                FROM my_host
                INNER JOIN host_view
                    USING(id)
                INNER JOIN "user"
                    ON my_host.id = "user".id
        """)

        @classmethod
        def get_last_logged_in(cls, rdbw):
            """
            Return last logged-in user name and UUID, or None if nothing
            cannot be found.

            @return: a tuple of user name and host UUID.
            """
            row = rdbw.query_value(cls.SELECT_LAST_LOGGED_IN)
            return (row.name, row.uuid) if row is not None else None


        SELECT_HOST_UUID = _dd("""\
            SELECT uuid
                FROM
                    host_view
                    INNER JOIN my_host
                        USING(id)
            """)

        @classmethod
        @contract_epydoc
        def get_my_host_uuid(cls, rdbw):
            """
            Find the current host UUID.
            This function may be used for a non-bound database;
            in this case, the specific wrapper factory must be passed.

            @type rdbw: DatabaseWrapperSQLAlchemy

            @rtype: UUID
            @raises StopIteration: if the host is not marked in
                                   "my_host" RDB table.
            """
            row = rdbw.query_value(cls.SELECT_HOST_UUID)
            assert row is not None  # we must always have "my host"

            return row.uuid



    class HostChunks(Queries.Chunks):
        """All RDB (host-only) queries related to the backup chunks."""

        __slots__ = tuple()


        CREATE_VIEW_DUMMY_CHUNK = _dd("""\
            CREATE VIEW IF NOT EXISTS dummy_chunk_view AS
                SELECT chunk.*
                FROM
                    chunk
                    INNER JOIN dummy_chunk
                        USING(id)
            """)

        CREATE_VIEW_DUMMY_CHUNK_INSERT_TRIGGER = _dd("""\
            CREATE TRIGGER IF NOT EXISTS dummy_chunk_view_insert_trigger
                INSTEAD OF INSERT ON dummy_chunk_view
                FOR EACH ROW
                BEGIN
                    INSERT OR ROLLBACK INTO chunk(
                            uuid,
                            hash,
                            size,
                            maxsize_code
                        )
                        VALUES (
                            new.uuid,
                            new.hash,
                            new.size,
                            0
                        );
                    INSERT OR ROLLBACK INTO dummy_chunk(id)
                        VALUES (
                            (SELECT id FROM chunk WHERE uuid == new.uuid)
                        );
                END
            """)

        CREATE_VIEW_DUMMY_CHUNK_DELETE_TRIGGER = _dd("""\
            CREATE TRIGGER IF NOT EXISTS dummy_chunk_view_delete_trigger
                INSTEAD OF DELETE ON dummy_chunk_view
                FOR EACH ROW
                BEGIN
                    DELETE FROM dummy_chunk WHERE id = old.id;
                    DELETE FROM chunk WHERE id = old.id;
                END;
            """)


        CREATE_VIEW_EXPECTED_CHUNK = _dd("""\
            CREATE VIEW IF NOT EXISTS expected_chunk_view AS
                SELECT
                    expected_chunk.id AS id,
                    expected_chunk.reason_restore AS reason_restore,
                    expected_chunk.since AS since,
                    chunk.id AS chunk_id,
                    chunk.crc32 AS chunk_crc32,
                    chunk.uuid AS chunk_uuid,
                    chunk.hash AS chunk_hash,
                    chunk.size AS chunk_size,
                    chunk.maxsize_code AS chunk_maxsize_code,
                    host_view.id AS host_id,
                    host_view.uuid AS host_uuid
                FROM
                    chunk
                    INNER JOIN expected_chunk
                        ON expected_chunk.chunk_id = chunk.id
                    INNER JOIN host_view
                        ON expected_chunk.host_id = host_view.id
            """)

        CREATE_VIEW_EXPECTED_CHUNK_INSERT_TRIGGER = _dd("""\
            CREATE TRIGGER IF NOT EXISTS expected_chunk_view_insert_trigger
                INSTEAD OF INSERT ON expected_chunk_view
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
                    INSERT OR ROLLBACK INTO expected_chunk(
                            chunk_id,
                            host_id,
                            reason_restore,
                            since
                        )
                        VALUES (
                            (
                                SELECT id
                                FROM chunk
                                WHERE uuid == new.chunk_uuid
                            ),
                            new.host_id,
                            new.reason_restore,
                            new.since
                        );
                END
            """)

        CREATE_VIEW_EXPECTED_CHUNK_DELETE_TRIGGER = _dd("""\
            CREATE TRIGGER IF NOT EXISTS expected_chunk_view_delete_trigger
                INSTEAD OF DELETE ON expected_chunk_view
                FOR EACH ROW
                BEGIN
                    DELETE FROM expected_chunk WHERE id = old.id;
                END
            """)


        # Given the dataset, create a list of all chunks used in this dataset.
        SELECT_ALL_CHUNKS_FOR_DATASET = _dd("""\
            SELECT DISTINCT
                    chunk.maxsize_code AS maxsize_code,
                    chunk.size AS size,
                    chunk.uuid AS uuid,
                    chunk.crc32 AS crc32,
                    chunk.hash AS hash
                FROM
                    dataset
                    INNER JOIN file_local_view
                        ON file_local_view.dataset_id = dataset.id
                    INNER JOIN block
                        ON file_local_view.file_id = block.file_id
                    INNER JOIN chunk
                        ON block.chunk_id = chunk.id
                WHERE
                    dataset.uuid = :ds_uuid
            """)

        @classmethod
        @duration_logged()
        @contract_epydoc
        def __get_unbound_chunks_for_dataset(cls, ds_uuid, rdbw):
            """
            Get the chunks for the dataset; the chunks are not bound
            to the files/blocks.

            @type ds_uuid: UUID
            @type rdbw: DatabaseWrapperSQLAlchemy

            @rtype: col.Iterable
            """
            rows = rdbw.query(cls.SELECT_ALL_CHUNKS_FOR_DATASET,
                              {'ds_uuid': ds_uuid})

            return (ChunkFromFilesFinal(maxsize_code=row.maxsize_code,
                                        uuid=row.uuid,
                                        hash=row.hash,
                                        size=row.size,
                                          # ChunkFromFilesFinal-specific
                                        expected_crc32=crc32_to_unsigned(
                                                           row.crc32))
                        for row in rows)


        @classmethod
        @duration_logged()
        @contract_epydoc
        def get_all_chunks_for_dataset(cls, host_uuid, ds_uuid, local_files,
                                       rdbw):
            """Get the chunks for the dataset,

            @todo: long operation, takes 2.5 seconds on test dataset.

            @type host_uuid: UUID
            @type ds_uuid: UUID
            @type local_files: col.Iterable
            @type rdbw: DatabaseWrapperSQLAlchemy

            @returns: a (non-reiterable) Iterable with Chunk-s.
            @rtype: col.Iterable
            """
            # Create the list of the chunks, which are, though,
            # not yet bound to the particular files/blocks.
            chunks = cls.__get_unbound_chunks_for_dataset(ds_uuid, rdbw)
            chunks_by_uuid = {ch.uuid: ch for ch in chunks}

            # Create the list of all the local files which are used
            # in any of the chunks for the dataset.
            local_files_by_uuid = {f.uuid: f for f in local_files}

            # Now let's get all the blocks for the dataset,
            # and map the chunks to the files.
            blocks_iter = \
                Queries.Blocks.get_blocks_for_dataset(host_uuid,
                                                      ds_uuid,
                                                      local_files_by_uuid,
                                                      chunks_by_uuid,
                                                      rdbw)

            # Create a dictionary mapping the chunk
            # to the list of its blocks (ordered by
            # the "offset_in_chunk" field)
            chunk_getter = attrgetter('chunk')
            offset_in_chunk_getter = attrgetter('offset_in_chunk')

            # Order all blocks by parent chunk, then group by parent chunk;
            # within each group, order by offset.
            blocks_by_chunk = \
                {k: sorted(g, key=offset_in_chunk_getter)
                     for k, g in sorted_groupby(blocks_iter,
                                                key=chunk_getter)}

            # Now, for each chunk, we may map it to the appropriate blocks
            for _chunk, _blocks in blocks_by_chunk.iteritems():
                assert not (_chunk.blocks)
                _chunk.blocks = list(_blocks)

            # Return the list explicitly, so that the intermediate
            # blocks_by_chunk variable is not kept in the memory.
            return list(blocks_by_chunk.iterkeys())


        MARK_CHUNK_UPLOADED = _dd("""\
            INSERT OR REPLACE INTO uploaded_chunk(id)
                VALUES
                (
                    (SELECT id FROM chunk WHERE uuid = :chunk_uuid)
                )
            """)

        @classmethod
        @contract_epydoc
        def mark_chunk_uploaded(cls, chunk_uuid):
            """
            @type chunk_uuid: UUID
            """
            assert RDB is not None

            with RDB() as rdbw:
                rdbw.query(cls.MARK_CHUNK_UPLOADED,
                           {'chunk_uuid': chunk_uuid})


        # Note: dummy chunk is always of maxsize_code 0
        INSERT_DUMMY_CHUNK = _dd("""\
            INSERT OR ROLLBACK INTO dummy_chunk_view(uuid, size)
                VALUES (:chunk_uuid, 1048576)
            """)

        @classmethod
        @contract_epydoc
        def create_dummy_chunk(cls, chunk_uuid):
            """
            @type chunk_uuid: UUID
            """
            assert RDB is not None

            with RDB() as rdbw:
                rdbw.query(HostQueries.HostChunks.INSERT_DUMMY_CHUNK,
                           {'chunk_uuid': chunk_uuid})


        DELETE_DUMMY_CHUNK = _dd("""\
            DELETE FROM dummy_chunk_view WHERE uuid = :chunk_uuid
            """)

        @classmethod
        @contract_epydoc
        def delete_dummy_chunks(cls, chunk_uuids):
            """
            @type chunk_uuids: col.Iterable
            """
            assert RDB is not None

            with RDB() as rdbw:
                rdbw.querymany(cls.DELETE_DUMMY_CHUNK,
                               ({'chunk_uuid': u} for u in chunk_uuids))


        # Get all the dummy chunks
        SELECT_ALL_DUMMY_CHUNKS = _dd("""\
            SELECT uuid FROM dummy_chunk_view
            """)

        @classmethod
        def get_all_dummy_chunk_uuids(cls, rdbw):
            """
            @returns: the (non-reiterable) Iterable of C{UUID} objects
                      for dummy chunks
            @rtype: col.Iterable
            """
            return (r.uuid
                        for r in rdbw.query(cls.SELECT_ALL_DUMMY_CHUNKS))


        # Arguments:
        # :crc32 - chunk CRC32
        # :uuid - chunk UUID
        # :hash - chunk hash
        # :size - chunk size
        # * chunk maxsize code is omitted and always 0
        # :host_uuid - host UUID,
        # :is_restore - expected reason is restore
        # :since

        # Note: expected chunks are always of maxsize_code 0
        INSERT_EXPECTED_CHUNK = _dd("""\
            INSERT OR IGNORE INTO expected_chunk_view(
                    chunk_crc32,
                    chunk_uuid,
                    chunk_hash,
                    chunk_size,
                    chunk_maxsize_code,
                    host_id,
                    reason_restore,
                    since
                )
                VALUES
                (
                    :crc32,
                    :uuid,
                    :hash,
                    :size,
                    0,
                    (SELECT id FROM host_view WHERE uuid == :host_uuid),
                    :is_restore,
                    :since
                )
            """)

        @classmethod
        @contract_epydoc
        def expect_chunks(cls, exp_chunk_data, rdbw):
            """
            Given an iterable of information about expected chunks
            (a tuple of sender UUID, Chunk, and a flag indicating
            that the chunk will be a restore-related), mark accordingly
            in the database (ignoring the potential errors).

            @type exp_chunk_data: col.Iterable

            @type rdbw: DatabaseWrapperSQLAlchemy
            """
            _now = datetime.utcnow()

            data_iter = ({'crc32': crc32_to_signed(chunk.crc32),
                          'uuid': chunk.uuid,
                          'hash': buffer(chunk.hash),
                          'size': chunk.size(),
                          'host_uuid': host_uuid,
                          'is_restore': restore_flag,
                          'since': _now}
                             for (host_uuid, chunk, restore_flag)
                                 in exp_chunk_data)

            rdbw.querymany(cls.INSERT_EXPECTED_CHUNK,
                           data_iter)


        SELECT_EXPECTED_CHUNK = _dd("""\
            SELECT
                    chunk_crc32,
                    chunk_hash,
                    chunk_size,
                    chunk_maxsize_code,
                    reason_restore,
                    since
                FROM expected_chunk_view
                WHERE
                    chunk_uuid = :chunk_uuid AND
                    host_uuid = :host_uuid
            """)

        GetExpectedChunkType = \
            namedtuple('namedtuple',
                       ('chunk', 'restore_flag', 'expected_since'))

        @classmethod
        @contract_epydoc
        def get_expected_chunk(cls, chunk_uuid, host_uuid):
            """
            Given a chunk UUID and the host UUID, get the expected chunk
            information, as a named tuple of
            (chunk, restore_flag, expected_since);
            if the chunk is absent, returns None.

            @type chunk_uuid: UUID
            @type host_uuid: UUID
            @rtype: HostQueries.HostChunks.GetExpectedChunkType, NoneType
            """
            assert RDB is not None

            with RDB() as rdbw:
                row = rdbw.query_value(cls.SELECT_EXPECTED_CHUNK,
                                       {'chunk_uuid': chunk_uuid,
                                        'host_uuid': host_uuid})

                if row is not None:
                    return cls.GetExpectedChunkType(
                               chunk=ChunkInfo(
                                         crc32=crc32_to_unsigned(
                                                   row.chunk_crc32),
                                         uuid=chunk_uuid,
                                         hash=row.chunk_hash,
                                         size=row.chunk_size,
                                         maxsize_code=row.chunk_maxsize_code),
                               restore_flag=bool(row.reason_restore),
                               expected_since=fix_strptime(row.since))
                else:
                    return None


        MARK_CHUNK_AS_RESTORE = _dd("""\
            INSERT OR IGNORE INTO restore_chunk(id, since)
                VALUES
                (
                    (SELECT id FROM chunk WHERE uuid = :chunk_uuid),
                    :since
                )
            """)

        DONT_EXPECT_CHUNK = _dd("""\
            DELETE FROM expected_chunk
                WHERE
                    chunk_id = (SELECT id
                                    FROM chunk
                                    WHERE uuid = :chunk_uuid) AND
                    host_id = (SELECT id
                                   FROM host_view
                                   WHERE uuid = :host_uuid) AND
                    reason_restore = :is_restore
            """)

        @classmethod
        @contract_epydoc
        def meet_expectations(cls, chunk_uuid, host_uuid, is_restore):
            """
            Given the information about the expected chunk,
            mark in the database (ignoring the potential errors)
            that it was received.

            @param chunk_uuid: the UUID of the Chunk
                               (must be already present in the RDB!).
            @type chunk_uuid: UUID
            @param host_uuid: the UUID of the Host
                              (must be already present in the RDB!).
            @type host_uuid: UUID
            @param is_restore: whether the chunk was expected
                               as a restore chunk.
            @type is_restore: bool
            """
            assert RDB is not None

            with RDB() as rdbw:
                if is_restore:
                    # Restore chunk: mark it in the restore_chunk table
                    rdbw.query(cls.MARK_CHUNK_AS_RESTORE,
                               {'chunk_uuid': chunk_uuid,
                                'since': datetime.utcnow()})
                else:
                    # Replication chunk: do nothing
                    pass

                # For any type of chunk, remove it from the list
                # of the expected ones
                rdbw.query(cls.DONT_EXPECT_CHUNK,
                          {'chunk_uuid': chunk_uuid,
                           'host_uuid': host_uuid,
                           'is_restore': is_restore})


        # Given the dataset, create a list of all chunks
        # already uploaded for this dataset.
        SELECT_UPLOADED_CHUNKS = _dd("""\
            SELECT DISTINCT
                    chunk.maxsize_code AS maxsize_code,
                    chunk.crc32 AS crc32,
                    chunk.uuid AS uuid,
                    chunk.hash AS hash,
                    chunk.size AS size
                FROM
                    dataset
                    INNER JOIN file_local_view
                        ON file_local_view.dataset_id = dataset.id
                    INNER JOIN block
                        ON file_local_view.file_id = block.file_id
                    INNER JOIN uploaded_chunk
                        ON block.chunk_id = uploaded_chunk.id
                    INNER JOIN chunk
                        ON uploaded_chunk.id = chunk.id
                WHERE
                    dataset.uuid = :ds_uuid
            """)


        @classmethod
        @contract_epydoc
        def get_uploaded_chunks(cls, ds_uuid, rdbw):
            """
            Given a dataset uuid, return all the chunks
            which were already uploaded for some dataset.

            @type ds_uuid: UUID

            @returns: the (non-reiterable) Iterable over the Chunk objects,
                      containing the chunks which were uploaded.
            @rtype: col.Iterable
            """
            res = rdbw.query(cls.SELECT_UPLOADED_CHUNKS,
                             {'ds_uuid': ds_uuid})
            return (ChunkInfo(crc32=crc32_to_unsigned(r.crc32),
                              uuid=r.uuid,
                              hash=r.hash,
                              size=r.size,
                              maxsize_code=r.maxsize_code)
                        for r in res)


        RENAME_CHUNK = _dd("""\
            UPDATE chunk
                SET uuid = :new_uuid
                WHERE uuid = :old_uuid
            """)

        @classmethod
        @contract_epydoc
        def mark_chunks_as_already_existing(cls, chunk_map):
            """
            Given a map from chunk UUID (of the local chunk)
            to chunk UUID (of the chunk as it is already stored in the cloud),
            rename the chunk UUIDs locally, and mark them as already existing.

            @type chunk_map: col.Mapping
            @precondition: consists_of(chunk_map.iterkeys(), UUID) # chunk_map
            @precondition: consists_of(chunk_map.itervalues(), UUID)
            """
            assert RDB is not None

            with RDB() as rdbw:
                # Mark all the chunk_map.iterkeys() chunks as uploaded.
                # Rename some chunks, but only those that NEED to be renamed.
                iter_for_mark = ({'chunk_uuid': u}
                                     for u in chunk_map.iterkeys())
                iter_for_rename = ({'old_uuid': k, 'new_uuid': v}
                                       for k, v in chunk_map.iteritems()
                                       if k != v)

                rdbw.querymany(cls.MARK_CHUNK_UPLOADED,
                               iter_for_mark)
                rdbw.querymany(cls.RENAME_CHUNK,
                               iter_for_rename)



    class HostDatasets(Queries.Datasets):
        """All RDB (host-only) queries related to the backup datasets."""

        __slots__ = tuple()


        INSERT_MY_DATASET = _dd("""\
            INSERT OR IGNORE INTO my_dataset(id, paused)
                VALUES (
                    (SELECT id FROM dataset WHERE uuid == :ds_uuid),
                    0
                )
            """)

        @classmethod
        @contract_epydoc
        def create_dataset_for_backup(cls, host_uuid, dataset, rdbw):
            """
            @todo: C{dataset} actually should be some kind of dataset
                that contains the directory information, either scanned from
                the FS or received from the Node during the sync-restore.

            @type host_uuid: UUID

            @type dataset: DatasetWithDirectories, AbstractBasicDatasetInfo

            @type rdbw: DatabaseWrapperSQLAlchemy
            """
            # Create the dataset itself,.. (calling superclass!)
            ds_uuid = Queries.Datasets.create_dataset_for_backup(host_uuid,
                                                                 dataset,
                                                                 rdbw)
            # ... mark it as local for this host,...
            res = rdbw.query(cls.INSERT_MY_DATASET,
                             {'ds_uuid': ds_uuid})
            # ... and also, if the dataset contains some file states, bind
            # them to the file_local's from the dataset.

            file_states_to_bind = \
                (fstate
                     for dirpath, (files_small, files_large)
                         in dataset.directories.iteritems()
                     for fstate in chain(files_small, files_large)
                     if fstate.time_changed is not None)

            HostQueries.HostFiles.bind_file_states_to_files(
                ds_uuid, file_states_to_bind,
                rdbw)

            return ds_uuid


        DELETE_MY_DATASET = _dd("""\
            DELETE FROM my_dataset
                WHERE id = (SELECT id FROM dataset WHERE uuid = :ds_uuid)
            """)

        DELETE_DATASET = _dd("""\
            DELETE FROM dataset WHERE uuid = :ds_uuid
            """)

        @classmethod
        def delete_dataset(cls, ds_uuid, rdbw):
            rdbw.query(HostQueries.HostDatasets.DELETE_MY_DATASET,
                       {'ds_uuid': ds_uuid})
            rdbw.query(HostQueries.HostDatasets.DELETE_DATASET,
                       {'ds_uuid': ds_uuid})


        SELECT_MY_DATASET = _dd("""\
            SELECT
                    dataset.name AS name,
                    dataset.sync AS sync,
                    dataset.time_started AS time_started,
                    dataset.time_completed AS time_completed,
                    my_dataset.paused AS paused,
                    "group".uuid AS ugroup_uuid
                FROM
                    dataset
                    INNER JOIN my_dataset
                        ON dataset.id = my_dataset.id
                    INNER JOIN "group"
                        ON dataset.group_id = "group".id
                WHERE
                    dataset.uuid = :ds_uuid
            """)

        @classmethod
        @duration_logged()
        @contract_epydoc
        def get_my_ds_in_progress(cls, host_uuid, ds_uuid, rdbw):
            """Given a dataset UUID, return all the dataset data from the RDB.

            @type host_uuid: UUID
            @type ds_uuid: DatasetUUID

            @type rdbw: DatabaseWrapperSQLAlchemy

            @return: The dataset object, or None if such dataset is not present
                     in the RDB for some reason.
            @rtype: DatasetOnChunks, NoneType

            @todo: this may be a long operation, needs to be optimized
                   to be executed lazily.
            """
            # Read files.
            # We need the files anyway later, as a part of the directories,
            # so we cannot avoid the list for now.
            files_res_list = list(Queries.Files
                                         .get_files_for_dataset(host_uuid,
                                                                ds_uuid,
                                                                rdbw))

            directories = {}
            files_count = 0
            for f in files_res_list:
                files_count += 1
                first, second = directories.setdefault(f.base_dir,
                                                       ([], []))
                first.append(f)

            assert files_count == \
                   sum(len(dir_files[0])
                           for dir_files in directories.itervalues()), \
                   'Not all files were bound to directories: \n' \
                       'files_count = {}, \n' \
                       'directories = {!r}'.format(files_count,
                                                   directories)

            # Now read dataset
            row = rdbw.query_value(cls.SELECT_MY_DATASET,
                                   {'ds_uuid': ds_uuid})
            if row is not None:
                chunks_iter = \
                    HostQueries.HostChunks \
                               .get_all_chunks_for_dataset(host_uuid,
                                                           ds_uuid,
                                                           files_res_list,
                                                           rdbw)
                return MyDatasetOnChunks(
                           name=row.name,
                           sync=bool(row.sync),
                           directories=directories,
                           uuid=DatasetUUID.safe_cast_uuid(ds_uuid),
                           ugroup_uuid=UserGroupUUID.safe_cast_uuid(
                                           row.ugroup_uuid),
                           time_started=fix_strptime(row.time_started),
                           time_completed=fix_strptime(row.time_completed),
                           # DatasetOnChunks-specific
                           chunks=list(chunks_iter),
                           # MyDatasetOnChunks-specific
                           paused=bool(row.paused))

            else:
                return None



    class HostFiles(Queries.Files):
        """All RDB (host-only) queries related to the files of the backup."""

        __slots__ = tuple()


        CREATE_VIEW_FILE_LOCAL_STATE_AT_HOST = _dd("""\
            CREATE VIEW IF NOT EXISTS file_local_state_at_host_view AS
                SELECT
                    f_l_s_ah.id AS id,
                    f_l_s_ah.time_changed AS time_changed,
                    f_l_s_ah.isdir AS isdir,
                    f_l_s_ah.size AS size,
                    base_directory_at_host.id AS base_dir_ah_id,
                    base_directory_at_host.group_id AS base_dir_ah_group_id,
                    base_directory_at_host.path AS base_dir_ah_path,
                    f_l_ah.id AS file_local_ah_id,
                    f_l_ah.rel_dir_path AS file_local_ah_rel_dir_path,
                    f_l_ah.rel_file_path AS file_local_ah_rel_file_path,
                    file_local.id AS file_local_id
                FROM
                    base_directory_at_host
                    INNER JOIN file_local_at_host AS f_l_ah
                        ON base_directory_at_host.id =
                            f_l_ah.base_directory_at_host_id
                    INNER JOIN file_local_state_at_host AS f_l_s_ah
                        ON f_l_ah.id =
                            f_l_s_ah.file_local_at_host_id
                    LEFT JOIN file_local
                        ON f_l_s_ah.file_local_id =
                            file_local.id
            """)

        CREATE_VIEW_FILE_LOCAL_STATE_AT_HOST_INSERT_TRIGGER = _dd("""\
            CREATE TRIGGER IF NOT EXISTS
                    file_local_state_at_host_view_insert_trigger
                INSTEAD OF INSERT ON file_local_state_at_host_view
                FOR EACH ROW
                BEGIN
                    INSERT OR IGNORE INTO file_local_at_host(
                            base_directory_at_host_id,
                            rel_dir_path,
                            rel_file_path
                        )
                        VALUES (
                            -- For the first argument, use the base dir ID
                            -- if provided, otherwise find it dynamically.
                            coalesce(
                                new.base_dir_ah_id,
                                (
                                    SELECT id
                                        FROM base_directory_at_host
                                        WHERE path = new.base_dir_ah_path
                                )
                            ),
                            new.file_local_ah_rel_dir_path,
                            new.file_local_ah_rel_file_path
                        );
                    INSERT OR IGNORE INTO file_local_state_at_host(
                            file_local_at_host_id,
                            time_changed,
                            isdir,
                            size
                        )
                        VALUES (
                            -- For the first argument, use the file local AH ID
                            -- if provided, otherwise find it dynamically.
                            coalesce(
                                new.file_local_ah_id,
                                (
                                    SELECT f_l_ah.id
                                        FROM
                                            base_directory_at_host
                                            INNER JOIN file_local_at_host
                                                    AS f_l_ah
                                                ON base_directory_at_host.id =
                                                    f_l_ah.base_directory_at_host_id
                                        WHERE
                                            base_directory_at_host.id =
                                                coalesce(
                                                    new.base_dir_ah_id,
                                                    (
                                                        SELECT id
                                                            FROM base_directory_at_host
                                                            WHERE path = new.base_dir_ah_path
                                                    )
                                                ) AND
                                            f_l_ah.rel_dir_path =
                                                new.file_local_ah_rel_dir_path
                                                    AND
                                            f_l_ah.rel_file_path =
                                                new.file_local_ah_rel_file_path
                                )
                            ),
                            new.time_changed,
                            new.isdir,
                            new.size
                        );
                END;
            """)

        CREATE_VIEW___LATEST_FILE_LOCAL_STATE_AT_HOST = _dd("""\
            CREATE VIEW IF NOT EXISTS
                    __latest_file_local_state_at_host_view AS
                SELECT
                    file_local_ah_id AS file_local_at_host_id,
                    max(time_changed) AS max_time_changed
                FROM file_local_state_at_host_view
                GROUP BY file_local_at_host_id
            """)

        CREATE_VIEW_LATEST_FILE_LOCAL_STATE_AT_HOST = _dd("""\
            CREATE VIEW IF NOT EXISTS
                    latest_file_local_state_at_host_view AS
                SELECT
                        f_l_s_ah.id AS id,
                        f_l_s_ah.time_changed AS time_changed,
                        f_l_s_ah.isdir AS isdir,
                        f_l_s_ah.size AS size,
                        base_directory_at_host.id AS base_dir_ah_id,
                        base_directory_at_host.group_id
                            AS base_dir_ah_group_id,
                        base_directory_at_host.path AS base_dir_ah_path,
                        f_l_ah.id AS file_local_ah_id,
                        f_l_ah.rel_dir_path AS file_local_ah_rel_dir_path,
                        f_l_ah.rel_file_path AS file_local_ah_rel_file_path,
                        file_local.id AS file_local_id
                    FROM
                        base_directory_at_host
                        INNER JOIN file_local_at_host AS f_l_ah
                            ON base_directory_at_host.id =
                                f_l_ah.base_directory_at_host_id
                        INNER JOIN file_local_state_at_host AS f_l_s_ah
                            ON f_l_ah.id = f_l_s_ah.file_local_at_host_id
                        INNER JOIN __latest_file_local_state_at_host_view
                                AS latest_state
                            ON
                                f_l_s_ah.file_local_at_host_id =
                                    latest_state.file_local_at_host_id AND
                                f_l_s_ah.time_changed =
                                    latest_state.max_time_changed
                        LEFT JOIN file_local
                            ON f_l_s_ah.file_local_id = file_local.id
            """)


        @classmethod
        @contract_epydoc
        def add_or_get_base_directory(cls, base_dir, ugroup_uuid, rdbw):
            """
            Returns the base directory (at host) ID
            (for further use in the queries)
            of C{base_dir} as stored in the RDB in the base_directory_at_host;
            also inserts the C{base_dir} if it was missing.

            @type ugroup_uuid: UserGroupUUID
            """
            m = models

            base_dir = encode_posix_path(base_dir)

            cl_base_dir_id = \
                sql_select([m.base_directories_at_host.c.id],
                           (m.base_directories_at_host.c.path == base_dir) &
                               (m.user_groups.c.uuid == ugroup_uuid),
                           from_obj=[m.base_directories_at_host
                                      .join(m.user_groups)])

            row_base_dir_id = rdbw.execute(cl_base_dir_id).scalar()

            if row_base_dir_id is not None:
                # The base directory exists already
                return row_base_dir_id
            else:
                # The base directory doesn't exist, let's add it
                cl_group_id = sql_select([m.user_groups.c.id],
                                         m.user_groups.c.uuid == ugroup_uuid)

                cl_insert_base_dir_ah = \
                    m.base_directories_at_host.insert() \
                        .values(path=base_dir, group=cl_group_id)
                res = rdbw.execute(cl_insert_base_dir_ah)
                assert res.rowcount == 1, res.rowcount

                return res.inserted_primary_key[0]


        SELECT_BASE_DIRS_WITH_UGROUPS = _dd("""\
            SELECT
                    base_dir_ah.path AS path,
                    "group".uuid AS ugroup_uuid,
                    "group".name AS ugroup_name,
                    "group".private AS ugroup_private,
                    "group".enc_key AS ugroup_enc_key
                FROM
                    base_directory_at_host AS base_dir_ah
                    INNER JOIN "group"
                        ON base_dir_ah.group_id = "group".id
            """)

        @classmethod
        @contract_epydoc
        def get_base_directories_with_ugroups(cls, rdbw):
            """
            Get all base directories together with their corresponding
            User Groups.

            @rtype: col.Iterable
            @returns: the iterator over the tuples like
                      (base_dir_path, user_group)
            """
            rows = rdbw.query(cls.SELECT_BASE_DIRS_WITH_UGROUPS)
            return ((decode_posix_path(row.path),
                     UserGroup(uuid=UserGroupUUID.safe_cast_uuid(
                                        row.ugroup_uuid),
                               name=str(row.ugroup_name),
                               private=bool(row.ugroup_private),
                               enc_key=b64decode(row.ugroup_enc_key)))
                        for row in rows)


        SELECT_BASE_DIR_BY_UGROUP_UUID = _dd("""\
            SELECT base_dir_ah.path AS path
                FROM
                    base_directory_at_host AS base_dir_ah
                    INNER JOIN "group"
                        ON base_dir_ah.group_id = "group".id
                WHERE
                    "group".uuid = :ugroup_uuid
            """)

        @classmethod
        @contract_epydoc
        def get_base_directory_for_ugroup(cls, ugroup_uuid, rdbw):
            """
            Given the user group UUID, return the appropriate base directory
            path bound to this user group.
            (or None if there is no matching directory for some reason).

            @type ugroup_uuid: UserGroupUUID

            @returns: basestring, NoneType
            """
            row = rdbw.query_value(cls.SELECT_BASE_DIR_BY_UGROUP_UUID,
                                   {'ugroup_uuid': ugroup_uuid})
            return None if row is None else row.path


        FileState = \
            namedtuple('FileState',
                       ('rel_dir', 'rel_file', 'time_changed', 'size'))

        _FileStateKey = \
            namedtuple('_FileStateKey',
                       ('rel_dir', 'rel_file'))

        _FileStateValue = \
            namedtuple('_FileStateValue',
                       ('base_dir_id',
                        'rel_dir',
                        'rel_file',
                        'time_changed',
                        'size'))

        GET_LATEST_FILE_STATES_FOR_DIR = _dd("""\
            SELECT
                    file_local_ah_rel_dir_path,
                    file_local_ah_rel_file_path,
                    time_changed,
                    size
                FROM latest_file_local_state_at_host_view
                WHERE
                    base_dir_ah_id = :base_dir_ah_id AND
                    file_local_ah_rel_dir_path = :file_local_ah_rel_dir_path
            """)

        @classmethod
        def __get_file_states_for_insert_iter_iter(
                cls, ts, base_dir_ah_id, file_states, rdbw):
            """
            Get an iterator over the iterators over the files on FS (grouped
            by directory) that should be used with C{cls.INSERT_FILE_STATES}
            to update the latest file states.

            @returns: an iterator over the iterators over the
                C{_FileStateValue} objects.
            """
            _fstate = cls.FileState
            _fstatekey = cls._FileStateKey
            _fstatevalue = cls._FileStateValue

            for (_rel_dirpath, per_dir_states) in file_states:
                rel_dirpath = encode_posix_path(_rel_dirpath)

                # Before this moment, file_states contains the path in local FS
                # notation. Let's convert it to the POSIX way.
                _cur_values_iter = \
                    (_fstate(rel_dir=encode_posix_path(state.rel_dir),
                             rel_file=encode_posix_path(state.rel_file),
                             time_changed=state.time_changed,
                             size=state.size)
                         for state in per_dir_states)

                cur_values = {_fstatekey(rel_dir=v.rel_dir,
                                         rel_file=v.rel_file):
                                  _fstatevalue(base_dir_id=base_dir_ah_id,
                                               rel_dir=v.rel_dir,
                                               rel_file=v.rel_file,
                                               time_changed=v.time_changed,
                                               size=v.size)
                                  for v in _cur_values_iter}

                rows = rdbw.query(cls.GET_LATEST_FILE_STATES_FOR_DIR,
                                  {'base_dir_ah_id': base_dir_ah_id,
                                   'file_local_ah_rel_dir_path': rel_dirpath})
                old_values = \
                    {_fstatekey(rel_dir=row.file_local_ah_rel_dir_path,
                                rel_file=row.file_local_ah_rel_file_path):
                         _fstatevalue(base_dir_id=base_dir_ah_id,
                                      rel_dir=row.file_local_ah_rel_dir_path,
                                      rel_file=row.file_local_ah_rel_file_path,
                                      time_changed=fix_strptime(
                                                       row.time_changed),
                                      size=row.size)
                         for row in rows}

                # Which files are absent now but were present before?
                # Contains the actual previous values.

                # Missing values:
                # 1. they were present before,
                # 2. their last state was not "deleted",
                # 3a. they don't exist now, or
                # 3b. they are now marked as explicitly deleted.
                _deleted_values = \
                    (v
                         for k, v in old_values.iteritems()
                         if v.size is not None and
                            (k not in cur_values or
                             cur_values[k].size is None))

                # Contrary to deleted_values, this iterable loops over the
                # deleted values as they should be written to the RDB,
                # i.e. with size = None (NULL)
                deleted_values_for_db = ({'base_dir_id': v.base_dir_id,
                                          'rel_dir': v.rel_dir,
                                          'rel_file': v.rel_file,
                                          'time_changed': ts,
                                          'isdir': False,
                                          'size': None}
                                             for v in _deleted_values)


                # Which values were added or altered,
                # and are more actual than in RDB?
                _refreshed_values = \
                    (v
                         for k, v in cur_values.iteritems()
                         if k not in old_values or
                            v.time_changed > old_values[k].time_changed)
                refreshed_values_for_db = \
                    ({'base_dir_id': v.base_dir_id,
                      'rel_dir': v.rel_dir,
                      'rel_file': v.rel_file,
                      'time_changed': v.time_changed,
                      'isdir': False,
                      'size': v.size}
                         for v in _refreshed_values)

                yield chain(deleted_values_for_db, refreshed_values_for_db)


        INSERT_FILE_STATES = _dd("""\
            INSERT OR IGNORE INTO file_local_state_at_host_view(
                    base_dir_ah_id,
                    file_local_ah_rel_dir_path,
                    file_local_ah_rel_file_path,
                    time_changed,
                    isdir,
                    size
                )
                VALUES (
                    :base_dir_id,
                    :rel_dir,
                    :rel_file,
                    :time_changed,
                    :isdir,
                    :size
                )
            """)

        @classmethod
        @contract_epydoc
        def add_file_states(cls, base_dir_ah_id, file_states, rdbw):
            """Write the file states unconditionally.

            @param file_states: contains C{LocalPhysicalFileState}
                or C{LocalPhysicalFileStateRel} objects.

            @type rdbw: DatabaseWrapperSQLAlchemy
            """
            rows = ({'base_dir_id': base_dir_ah_id,
                     'rel_dir': encode_posix_path(fstate.rel_dir),
                     'rel_file': encode_posix_path(fstate.rel_file),
                     'time_changed': fstate.time_changed,
                     'isdir': fstate.isdir,
                     'size': fstate.size}
                        for fstate in file_states)

            rdbw.querymany(cls.INSERT_FILE_STATES,
                           rows)


        @classmethod
        @contract_epydoc
        def update_file_states(cls, ts, base_dir_ah_id, file_states, rdbw):
            """
            Synchronizes all the current file states with the RDB; adds missing
            states, marks files nowadays absent as deleted.

            @param ts: timestamp of the directory structure scan;
                       in particular, it is used to mark the deleted paths.
            @type ts: datetime

            @type base_dir_ah_id: numbers.Integral
            @precondition: base_dir_ah_id > 0

            @param file_states: the (possibly non-reiterable) Iterable
                                of C{HostQueries.HostFiles.FileState} objects
            @type file_states: col.Iterable
            @type rdbw: DatabaseWrapperSQLAlchemy
            """
            with RDB() as inner_wrapper:
                file_states_iter_iter = \
                    cls.__get_file_states_for_insert_iter_iter(
                        ts, base_dir_ah_id, file_states,
                        inner_wrapper)
                file_states_iter = chain.from_iterable(file_states_iter_iter)

                rdbw.querymany(cls.INSERT_FILE_STATES,
                               file_states_iter)


        # Arguments:
        # - - file_local part - -
        # :ds_uuid
        # :base_dir_path
        # :file_local_rel_path
        # - - file_local_state_at_host part - -
        # :time_changed
        # :isdir
        # :size
        # *  base_directory_at_host.path (=2)
        # :rel_dir_path
        # :rel_file_path
        BIND_FILE_STATES_TO_FILES = _dd("""\
            UPDATE OR IGNORE file_local_state_at_host
                SET
                    file_local_id = (
                        SELECT file_local.id
                            FROM
                                file_local
                                INNER JOIN base_directory
                                    ON base_directory.id =
                                        file_local.base_directory_id
                                INNER JOIN dataset
                                    ON dataset.id = base_directory.dataset_id
                            WHERE
                                dataset.uuid = :ds_uuid AND
                                base_directory.path = :base_dir_path AND
                                isdir = :isdir AND
                                file_local.rel_path = :file_local_rel_path)
                WHERE
                    time_changed = :time_changed AND
                    isdir = :isdir AND
                    ((:size IS NULL AND size IS NULL) OR
                     (size = :size)) AND
                    file_local_at_host_id = (
                        SELECT file_local_ah.id
                            FROM
                                file_local_at_host AS file_local_ah
                                INNER JOIN base_directory_at_host
                                        AS base_dir_ah
                                    ON base_dir_ah.id =
                                        file_local_ah.base_directory_at_host_id
                            WHERE
                                base_dir_ah.path = :base_dir_path AND
                                file_local_ah.rel_dir_path = :rel_dir_path AND
                                file_local_ah.rel_file_path = :rel_file_path
                    )
            """)

        @classmethod
        @contract_epydoc
        def bind_file_states_to_files(cls, ds_uuid, file_states, rdbw):
            """
            @param ds_uuid: dataset UUID.
            @type ds_uuid: UUID

            @param file_states: the (possibly non-reiterable) Iterable
                of C{LocalPhysicalFileState} objects
            @type file_states: col.Iterable
            @type rdbw: DatabaseWrapperSQLAlchemy
            """
            file_states_iter = \
                ({'ds_uuid': ds_uuid,
                  'base_dir_path': encode_posix_path(fstate.base_dir),
                  'file_local_rel_path': encode_posix_path(fstate.rel_path),
                  'time_changed': fstate.time_changed,
                  'isdir': False,
                  'size': fstate.size,
                  'rel_dir_path': encode_posix_path(fstate.rel_dir),
                  'rel_file_path': encode_posix_path(fstate.rel_file)}
                     for fstate in file_states)

            rdbw.querymany(cls.BIND_FILE_STATES_TO_FILES,
                           file_states_iter)


        # Arguments:
        # :time_changed - the latest time_changed to backup.
        # :ugroup_uuid - the UUID of the user group to check
        SELECT_NON_BACKED_FILES_CHANGED_BEFORE = _dd("""\
            SELECT
                    state.file_local_ah_rel_dir_path AS rel_dir_path,
                    state.file_local_ah_rel_file_path AS rel_file_path,
                    state.size AS size,
                    state.time_changed AS time_changed,
                    state.isdir
                FROM
                    latest_file_local_state_at_host_view AS state
                    INNER JOIN "group"
                        ON "group".id = state.base_dir_ah_group_id
                WHERE
                    state.time_changed < :time_changed AND
                    state.file_local_id IS NULL AND
                    "group".uuid = :ugroup_uuid
                ORDER BY
                    state.file_local_ah_rel_dir_path,
                    state.file_local_ah_rel_file_path
            """)

        @classmethod
        @contract_epydoc
        def get_files_for_backup_older_than(cls, ts, ugroup_uuid, rdbw):
            """Find files which are not yet backed up and are old enough.

            @param ugroup_uuid: the UUID of the User Group to check.
            @type ugroup_uuid: UserGroupUUID

            @type rdbw: DatabaseWrapperSQLAlchemy

            @returns: the non-reiterable iterator over the per file information
                (C{LocalPhysicalFileStateRel} objects) for the files
                whose last edit time is older than C{ts} and which are not
                yet backed up.
                The result is sorted by the base directory, then by the
                relative directory, then by the file name.
            @rtype: col.Iterable
            """
            # Let's test whether at least a single item exists
            rows = rdbw.query(cls.SELECT_NON_BACKED_FILES_CHANGED_BEFORE,
                              {'time_changed': ts,
                               'ugroup_uuid': ugroup_uuid})

            # Loop over the rows, take path components, join them
            # and do the POSIX decode. Also, take the size (so that the files
            # which size is None are marked as deleted).
            return (LocalPhysicalFileStateRel(
                            decode_posix_path(row.rel_dir_path),
                            decode_posix_path(row.rel_file_path),
                            row.size,
                            fix_strptime(row.time_changed),
                            row.isdir)
                        for row in rows)


        @classmethod
        @contract_epydoc
        def is_file_going_to_backup(cls, path, rdbw):
            """
            Checks whether the path was recently changed and will be
            backupped soon.

            @param path: os dependent representation of path.
            @type path: basestring

            @type rdbw: DatabaseWrapperSQLAlchemy

            @returns: whether the path was recently changed and will be
                backupped soon.
            @rtype: bool

            """
            f_l_s_ah_view = \
                models.FileLocalStateAtHost \
                      .file_local_state_at_host_view().alias('f_l_s_ah_view')
            f_l_ah_rel_dir_path = f_l_s_ah_view.c.file_local_ah_rel_dir_path
            f_l_ah_rel_file_path = f_l_s_ah_view.c.file_local_ah_rel_file_path

            whereclause = (f_l_ah_rel_dir_path == bindparam('dir_path'))
            whereclause &= (f_l_ah_rel_file_path == bindparam('file_path'))
            whereclause &= (f_l_s_ah_view.c.file_local_id == None)

            cl_select = sql_select(
                            [f_l_ah_rel_dir_path,
                             f_l_ah_rel_file_path,
                             f_l_s_ah_view.c.file_local_id],
                            from_obj=f_l_s_ah_view,
                            whereclause=whereclause)

            # file_path is file_name, really :-\
            os_dep_dir_path,  os_dep_file_path = \
                os.path.split(os.path.abspath(path))
            os_indep_dir_path = encode_posix_path(os_dep_dir_path)
            os_indep_file_path = encode_posix_path(os_dep_file_path)

            return rdbw.execute(cl_select,
                                {'dir_path': os_indep_dir_path,
                                 'file_path': os_indep_file_path}).scalar() \
                    is not None


    ALL_CREATE_HOST_QUERIES = (
        HostChunks.CREATE_VIEW_DUMMY_CHUNK,
        HostChunks.CREATE_VIEW_DUMMY_CHUNK_INSERT_TRIGGER,
        HostChunks.CREATE_VIEW_DUMMY_CHUNK_DELETE_TRIGGER,
        HostChunks.CREATE_VIEW_EXPECTED_CHUNK,
        HostChunks.CREATE_VIEW_EXPECTED_CHUNK_INSERT_TRIGGER,
        HostChunks.CREATE_VIEW_EXPECTED_CHUNK_DELETE_TRIGGER,
        HostFiles.CREATE_VIEW_FILE_LOCAL_STATE_AT_HOST,
        HostFiles.CREATE_VIEW_FILE_LOCAL_STATE_AT_HOST_INSERT_TRIGGER,
        HostFiles.CREATE_VIEW___LATEST_FILE_LOCAL_STATE_AT_HOST,
        HostFiles.CREATE_VIEW_LATEST_FILE_LOCAL_STATE_AT_HOST,
    )



    #
    # Methods
    #

    @classmethod
    @contract_epydoc
    def create_host_db_schema(cls):
        """
        Initialize the structure of host database..
        """
        assert RDB is not None

        logger.debug('Creating host database schema')

        with RDB() as rdbw:
            create_db_schema(rdbw=rdbw,
                             extra_queries=cls.ALL_CREATE_HOST_QUERIES)

            # Initialize the default setting names
            rdbw.session.add_all(models.SettingName(name=name)
                                     for name in cls.HostSettings
                                                    .ALL_LOCAL_SETTINGS
                                                    .iterkeys())

        logger.debug('Created host database schema')


    @staticmethod
    @contract_epydoc
    def wipe_host_db(file_path):
        """
        Given a file path to the database file, wipe the database file

        @type file_path: basestring
        """
        with open_wb(file_path) as db_file:
            db_file.truncate()
            db_file.flush()
            os.fsync(db_file.fileno())



#
# Functions
#
def init(*args, **kwargs):
    """
    Initialize the RDB factory singleton.
    Arguments are the same as ones for the DatabaseWrapperFactory constructor.
    """
    global RDB
    assert RDB is None, 'RDB is not none'

    RDB = DatabaseWrapperFactory(*args, **kwargs)

    if RDB.is_schema_initialized:
        logger.debug('Starting maintenance...')
        with RDB() as rdbw:
            Queries.System.maintenance(rdbw)
        logger.debug('Maintenance done')

    global SETTINGS_CACHE, NON_TABLE_SETTINGS_CACHE
    assert SETTINGS_CACHE is None, 'SETTINGS_CACHE is not None'
    assert NON_TABLE_SETTINGS_CACHE is None, 'NON_TABLE_SETTINGS_CACHE is not None'
    SETTINGS_CACHE = HostSettingsCache(HostQueries.HostSettings.ALL_SETTINGS,
                                       SETTINGS_CACHE_PERIOD)
    NON_TABLE_SETTINGS_CACHE = HostSettingsCache(NonDBSettings.ALL_SETTINGS,
                                                 SETTINGS_CACHE_PERIOD)


def uninit():
    """
    Deinitialize the RDB factory singleton.
    """
    global RDB
    assert RDB is not None
    RDB = None

    global SETTINGS_CACHE, NON_TABLE_SETTINGS_CACHE
    assert SETTINGS_CACHE is not None and NON_TABLE_SETTINGS_CACHE is not None
    SETTINGS_CACHE = None
    NON_TABLE_SETTINGS_CACHE = None
