#!/usr/bin/python
"""
Common database migration scripts,
and the C{Migrator} framework to execute the migrations.
"""

#
# Imports
#

from __future__ import absolute_import

import collections as col
import logging
from collections import namedtuple
from datetime import datetime
from textwrap import dedent as _dd

from contrib.dbc import contract_epydoc

from common import constants
from common.db import \
    DatabaseWrapperFactory, Queries, run_init_query_on_wrapper
from common.utils import strptime



#
# Constants
#

logger = logging.getLogger(__name__)



#
# Classes
#

Migration = namedtuple('migration', ('forward', 'backward'))


class Migrator(object):
    """The framework to launch the database migrations."""

    __slots__ = ('_rdbw_factory',)

    # This dictionary must be overridden in the subclasses;
    # the overridden value will be used then.
    _migrations = {}


    def __init__(self, rdbw_factory):
        """Constructor.

        @type rdbw_factory: DatabaseWrapperFactory
        """
        self._rdbw_factory = rdbw_factory


    def _get_db_version(self):
        with self._rdbw_factory() as rdbw:
            return Queries.LocalSettings.get_setting(
                       Queries.LocalSettings.DB_SCHEMA_VERSION,
                       rdbw)

    @contract_epydoc
    def _set_db_version(self, value):
        """
        @type value: datetime
        """
        with self._rdbw_factory() as rdbw:
            return Queries.LocalSettings.set_local_setting(
                       Queries.LocalSettings.DB_SCHEMA_VERSION,
                       value,
                       rdbw)


    def __upwards(self):
        """
        To what DB version may we migrate upwards
        from a particular DB version?
        """
        return {from_: to_
                    for (from_, to_), v in self._migrations.iteritems()}


    def __downwards(self):
        """
        To what DB version may we migrate downwards
        from a particular DB version?
        """
        return {to_: from_
                    for (from_, to_), v in self._migrations.iteritems()}


    def _launch_action(self, action):
        """
        Given an action in any of the formats supported
        in the migrations dictionary, execute it.
        """
        if callable(action):
            # Assuming an unbound method: bind it to this instance and invoke
            action(self)
        else:
            # An action may be any sequence/mapping of the queries ones
            # like they are used during the db initialization.
            with self._rdbw_factory() as rdbw:
                run_init_query_on_wrapper(rdbw, action)


    def auto_upgrade(self):
        """
        Perform all the operations to migrate the database
        to the latest state, and, if any migration was performed,
        run the appropriate maintenance queries
        """
        cls = self.__class__

        _upwards = self.__upwards()
        _migrations = self._migrations

        current_version = self._get_db_version()
        upgrade_performed = False
        while current_version < cls.max_version:
            upgrade_performed = True
            next_version = _upwards[current_version]

            logger.debug("Upgrading DB from '%s' to '%s'",
                         current_version, next_version)
            for action in _migrations[(current_version, next_version)].forward:
                self._launch_action(action)

            self._set_db_version(next_version)

            current_version = next_version

        if upgrade_performed:
            logger.debug('Auto-upgrade of the DB schema completed')
            with self._rdbw_factory() as rdbw:
                Queries.System.maintenance(rdbw, force=True)
            logger.debug('Mandatory maintenance done')
        else:
            logger.debug('No auto-upgrade of the DB schema needed')



class MigrationPlan(Migrator):
    """The actual set of actions needed to perform any database migrations."""

    # General constants

    __p = strptime

    min_version = __p('2011-11-14 20:35:00.000000')

    max_version = constants.CURRENT_DB_VERSION

    # Migrations

    _001_recreate_file_local_view_trigger_proc_pg = {
        'postgresql':
            Queries.Files
                   ._Files__CREATE_FILE_LOCAL_VIEW_TRIGGER_PROC_POSTGRESQL,
    }

    __001_create_file_local_view_postgresql_fwd = _dd("""\
        CREATE OR REPLACE VIEW file_local_view AS
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
    __001_create_file_local_view_postgresql_back = _dd("""\
        CREATE OR REPLACE VIEW file_local_view AS
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
                INNER JOIN file_view
                    ON file_view.id = file_local.file_id
        """)

    __001_create_file_local_view_sqlite_fwd = [
        'DROP VIEW file_local_view',
        _dd("""\
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
            """),
    ]
    __001_create_file_local_view_sqlite_back = [
        'DROP VIEW file_local_view',
        _dd("""\
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
                    INNER JOIN file_view
                        ON file_view.id = file_local.file_id
            """),
    ]

    _001_create_file_local_view_fwd = {
        'postgresql': __001_create_file_local_view_postgresql_fwd,
        'sqlite':     __001_create_file_local_view_sqlite_fwd,
    }
    _001_create_file_local_view_back = {
        'postgresql': __001_create_file_local_view_postgresql_back,
        'sqlite':     __001_create_file_local_view_sqlite_back,
    }
    # We need to recreate the triggers (after the view is regenerated)
    # only on SQLite, as PostgreSQL alters the view "inline".
    _001_recreate_file_local_view_sqlite_triggers = {
        'sqlite': Queries.Files.CREATE_FILE_LOCAL_VIEW_TRIGGERS,
    }

    # Create new __dataset_chunk_stats_all_view

    _001_create__dataset_chunk_stats_all_view = \
        Queries.Datasets.CREATE_VIEW_DATASET_CHUNK_STATS_ALL

    _001_drop__dataset_chunk_stats_all_view = \
        'DROP VIEW __dataset_chunk_stats_all_view'

    # Fix __dataset_file_stats_view

    __001_create__dataset_file_stats_view_postgresql_fwd = _dd("""\
        CREATE OR REPLACE VIEW __dataset_file_stats_view AS
            SELECT
                f.dataset_id,
                COUNT(DISTINCT id) AS file_local_count,
                COUNT(DISTINCT file_id) AS file_count,
                (
                    SELECT coalesce(sum(f1.size), 0)
                    FROM file_local_view AS f1
                    WHERE f1.dataset_id = f.dataset_id
                ) AS file_local_size,
                CAST(-1 AS numeric) AS file_size -- not supported
            FROM file_local_view AS f
            GROUP BY dataset_id
        """)
    __001_create__dataset_file_stats_view_postgresql_back = _dd("""\
        CREATE OR REPLACE VIEW __dataset_file_stats_view AS
            SELECT
                f.dataset_id,
                COUNT(DISTINCT id) AS file_local_count,
                COUNT(DISTINCT file_id) AS file_count,
                (
                    SELECT sum(f1.size)
                    FROM file_local_view AS f1
                    WHERE f1.dataset_id = f.dataset_id
                ) AS file_local_size,
                (
                    SELECT sum(f2.size)
                    FROM file_local_view AS f2
                    WHERE f2.dataset_id = f.dataset_id
                    GROUP BY f2.file_id
                ) AS file_size
            FROM file_local_view AS f
            GROUP BY dataset_id
        """)

    __001_create__dataset_file_stats_view_sqlite_fwd = [
        'DROP VIEW __dataset_file_stats_view',
        _dd("""\
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
            """),
    ]
    __001_create__dataset_file_stats_view_sqlite_back = [
        'DROP VIEW __dataset_file_stats_view',
        _dd("""\
            CREATE VIEW __dataset_file_stats_view AS
                SELECT
                    f.dataset_id,
                    COUNT(DISTINCT id) AS file_local_count,
                    COUNT(DISTINCT file_id) AS file_count,
                    (
                        SELECT sum(f1.size)
                        FROM file_local_view AS f1
                        WHERE f1.dataset_id = f.dataset_id
                    ) AS file_local_size,
                    (
                        SELECT sum(f2.size)
                        FROM file_local_view AS f2
                        WHERE f2.dataset_id = f.dataset_id
                        GROUP BY f2.file_id
                    ) AS file_size
                FROM file_local_view AS f
                GROUP BY dataset_id
            """),
    ]

    _001_create__dataset_file_stats_view_fwd = {
        'postgresql': __001_create__dataset_file_stats_view_postgresql_fwd,
        'sqlite':     __001_create__dataset_file_stats_view_sqlite_fwd,
    }
    _001_create__dataset_file_stats_view_back = {
        'postgresql': __001_create__dataset_file_stats_view_postgresql_back,
        'sqlite':     __001_create__dataset_file_stats_view_sqlite_back,
    }


    __002_recreate_chunk_per_host_view_trigger_proc_pg_fwd = _dd("""\
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

                    INSERT INTO chunk_per_host(chunk_id, host_id)
                        VALUES (_chunk_id, new.host_id);

                    RETURN new;
                ELSIF (TG_OP = 'DELETE') THEN
                    DELETE FROM chunk_per_host WHERE id = old.id;

                    RETURN old;
                END IF;
            END;
            $$ LANGUAGE plpgsql
            """)
    __002_recreate_chunk_per_host_view_trigger_proc_pg_back = _dd("""\
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
                                new.chunk_uuid IS NOT NULL AND
                                uuid = new.chunk_uuid
                            )
                            OR
                            (
                                new.chunk_uuid IS NULL AND
                                hash = new.chunk_hash AND
                                size = new.chunk_size
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

                    INSERT INTO chunk_per_host(chunk_id, host_id)
                        VALUES (_chunk_id, new.host_id);

                    RETURN new;
                ELSIF (TG_OP = 'DELETE') THEN
                    DELETE FROM chunk_per_host WHERE id = old.id;

                    RETURN old;
                END IF;
            END;
            $$ LANGUAGE plpgsql
            """)

    __002_recreate_chunk_per_host_view_trigger_proc_sqlite_fwd = _dd("""\
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
    __002_recreate_chunk_per_host_view_trigger_proc_sqlite_back = _dd("""\
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
                    INSERT OR ROLLBACK INTO chunk_per_host(
                            chunk_id,
                            host_id
                        )
                        VALUES (
                            (
                                SELECT id
                                    FROM chunk
                                    WHERE
                                        (
                                            new.chunk_uuid IS NOT NULL AND
                                            uuid = new.chunk_uuid
                                        )
                                        OR
                                        (
                                            new.chunk_uuid IS NULL AND
                                            hash = new.chunk_hash AND
                                            size = new.chunk_size
                                        )
                            ),
                            new.host_id
                        );
                END;
            """)

    _002_fix_chunk_per_host_view_trigger_fwd = {
        'postgresql': __002_recreate_chunk_per_host_view_trigger_proc_pg_fwd,
        'sqlite': ['DROP TRIGGER chunk_per_host_view_insert_trigger',
                   __002_recreate_chunk_per_host_view_trigger_proc_sqlite_fwd]
    }
    _002_fix_chunk_per_host_view_trigger_back = {
        'postgresql': __002_recreate_chunk_per_host_view_trigger_proc_pg_back,
        'sqlite': ['DROP TRIGGER chunk_per_host_view_insert_trigger',
                   __002_recreate_chunk_per_host_view_trigger_proc_sqlite_back]
    }

    __003_recreate_chunk_per_host_view_trigger_proc_pg_fwd = _dd("""\
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
    __003_recreate_chunk_per_host_view_trigger_proc_pg_back = \
        __002_recreate_chunk_per_host_view_trigger_proc_pg_fwd

    _003_fix_chunk_per_host_view_trigger_fwd = {
        'postgresql': __003_recreate_chunk_per_host_view_trigger_proc_pg_fwd,
    }
    _003_fix_chunk_per_host_view_trigger_back = {
        'postgresql': __003_recreate_chunk_per_host_view_trigger_proc_pg_back,
    }


    _004_delete_chunk_in_dataset_view_fwd = _dd("""\
        DROP VIEW chunk_in_dataset_view
        """)

    _004_delete_chunk_in_dataset_view_back = _dd("""\
        CREATE VIEW chunk_in_dataset_view AS
            SELECT DISTINCT
                chunk.id AS id,
                chunk.uuid AS chunk_uuid,
                dataset.id AS dataset_id,
                dataset.uuid AS dataset_uuid
            FROM
                chunk
                INNER JOIN block
                    ON chunk.id = block.chunk_id
                INNER JOIN file_local_view
                    ON block.file_id = file_local_view.file_id
                INNER JOIN dataset
                    ON file_local_view.dataset_id = dataset.id
        """)


    __005_create__dataset__merged_postgresql_fwd = _dd("""\
        ALTER TABLE dataset
            ADD COLUMN merged BOOLEAN
            DEFAULT false NOT NULL
        """)

    __005_create__dataset__merged_sqlite_fwd = _dd("""\
        ALTER TABLE dataset
            ADD COLUMN merged BOOLEAN
            DEFAULT '0' NOT NULL CHECK (merged IN (0, 1))
        """)

    __005_create__dataset__merged_postgresql_back = _dd("""\
        ALTER TABLE dataset
            DROP COLUMN IF EXISTS merged
        """)

    # Dropping columns is not supported on SQLite
    __005_create__dataset__merged_sqlite_back = _dd('')

    _005_create__dataset__merged_fwd = {
        'postgresql':
            __005_create__dataset__merged_postgresql_fwd,
        'sqlite':
            __005_create__dataset__merged_sqlite_fwd,
    }
    _005_create__dataset__merged_back = {
        'postgresql':
            __005_create__dataset__merged_postgresql_back,
        'sqlite':
            __005_create__dataset__merged_sqlite_back,
    }

    _006_alter__block__offs_in_file__bigint_fwd = {
        'postgresql': [
            'DROP VIEW block_view',
            _dd("""\
                ALTER TABLE block
                    ALTER COLUMN offset_in_file
                    SET DATA TYPE bigint
                """),
            Queries.Blocks.CREATE_BLOCK_VIEW,
            Queries.Blocks._CREATE_BLOCK_VIEW_TRIGGER_POSTGRESQL
        ],
    }
    _006_alter__block__offs_in_file__bigint_back = {
        'postgresql': [
            'DROP VIEW block_view',
            _dd("""\
                ALTER TABLE block
                    ALTER COLUMN offset_in_file
                    SET DATA TYPE integer
                """),
            Queries.Blocks.CREATE_BLOCK_VIEW,
            Queries.Blocks._CREATE_BLOCK_VIEW_TRIGGER_POSTGRESQL
        ],
    }


    # This dictionary may be overridden in the subclasses;
    # the overridden value will be used then.
    _migrations = {
        # No backward migration, since this is a bugfix
        (__p('2011-11-14 20:35:00.000000'), __p('2012-05-29 14:35:00.000000')):
            Migration(forward=[_001_recreate_file_local_view_trigger_proc_pg,
                               _001_create_file_local_view_fwd,
                               _001_recreate_file_local_view_sqlite_triggers,
                               _001_create__dataset_chunk_stats_all_view,
                               _001_create__dataset_file_stats_view_fwd],
                      backward=[_001_create_file_local_view_back,
                                _001_recreate_file_local_view_sqlite_triggers,
                                _001_drop__dataset_chunk_stats_all_view,
                                _001_create__dataset_file_stats_view_back]),
        (__p('2012-05-29 14:35:00.000000'), __p('2012-05-31 21:40:00.000000')):
            Migration(forward=[_002_fix_chunk_per_host_view_trigger_fwd],
                      backward=[_002_fix_chunk_per_host_view_trigger_back]),
        (__p('2012-05-31 21:40:00.000000'), __p('2012-06-07 16:31:00.000000')):
            Migration(forward=[], backward=[]),
        (__p('2012-06-07 16:31:00.000000'), __p('2012-06-08 22:25:00.000000')):
            Migration(forward=[], backward=[]),
        (__p('2012-06-08 22:25:00.000000'), __p('2012-06-27 17:25:00.000000')):
            Migration(forward=[], backward=[]),
        (__p('2012-06-27 17:25:00.000000'), __p('2012-07-24 17:57:00.000000')):
            Migration(forward=[], backward=[]),
        (__p('2012-07-24 17:57:00.000000'), __p('2012-08-01 17:45:00.000000')):
            Migration(forward=[_003_fix_chunk_per_host_view_trigger_fwd],
                      backward=[_003_fix_chunk_per_host_view_trigger_back]),
        (__p('2012-08-01 17:45:00.000000'), __p('2012-08-20 17:45:00.000000')):
            Migration(forward=[], backward=[]),
        (__p('2012-08-20 17:45:00.000000'), __p('2012-09-12 21:01:00.000000')):
            Migration(forward=[_004_delete_chunk_in_dataset_view_fwd],
                      backward=[_004_delete_chunk_in_dataset_view_back]),
        (__p('2012-09-12 21:01:00.000000'), __p('2013-01-07 20:35:00.000000')):
            Migration(forward=[], backward=[]),
        (__p('2013-01-07 20:35:00.000000'), __p('2013-03-14 19:18:00.000000')):
            Migration(forward=[_005_create__dataset__merged_fwd],
                      backward=[_005_create__dataset__merged_back]),
        (__p('2013-03-14 19:18:00.000000'), __p('2013-03-15 17:43:00.000000')):
            Migration(forward=[],
                      backward=[]),
        (__p('2013-03-15 17:43:00.000000'), __p('2013-04-05 18:10:00.000000')):
            Migration(forward=[_006_alter__block__offs_in_file__bigint_fwd],
                      backward=[_006_alter__block__offs_in_file__bigint_back]),
    }
