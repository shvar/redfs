#!/usr/bin/python
"""
Trusted-components-specific database migration scripts.
"""

#
# Imports
#

from __future__ import absolute_import
import logging
from textwrap import dedent as _dd

from common.db import Queries
from common.migration import MigrationPlan, Migration
from common.utils import strptime

from trusted.db import TrustedQueries
from trusted.docstore.fdbqueries import FDBQueries



#
# Constants
#

logger = logging.getLogger(__name__)



#
# Classes
#

class TrustedMigrationPlan(MigrationPlan):
    """The migration plan on the trusted peers."""

    __slots__ = ('_fdbw_factory', '_bdbw_factory')


    def __init__(self, fdbw_factory, bdbw_factory, *args, **kwargs):
        """Constructor.

        @type fdbw_factory: DocStoreWrapperFactory
        @type bdbw_factory: DocStoreWrapperFactory
        """
        super(TrustedMigrationPlan, self).__init__(*args, **kwargs)
        self._fdbw_factory = fdbw_factory
        self._bdbw_factory = bdbw_factory


    # Migrations

    # Add the time column
    _003_add_time_column_to_dataset_on_host_fwd = _dd("""\
        ALTER TABLE dataset_on_host
            ADD COLUMN time timestamp without time zone
            NOT NULL DEFAULT now()
        """)

    # By default, the column is initialized with the datetime.now.
    # We need to re-set it to the time of, say, dataset completion.
    _003_set_time_column_from_dataset_fwd = _dd("""\
        UPDATE dataset_on_host
            SET time = (SELECT time_completed
                            FROM dataset
                            WHERE dataset.id = dataset_id)
                       + interval '1 microsecond'
        """)

    # After the time of all dataset_on_host records is set to
    # the dataset completion time, add hosts in the dataset are assumed
    # that they've received the dataset simultaneously.
    # This may break the code which looks for the first host who've
    # uploaded the dataset.
    # Let's artificially set one of them 1 microsecond backward
    # (and all others were set 1 microsecond forward from real time
    # on the previous step).
    _003_set_first_upload_time_fwd = _dd("""\
        UPDATE dataset_on_host
            SET time = time - interval '1 microsecond'
            WHERE id IN (SELECT min(id)
                             FROM dataset_on_host
                             GROUP BY dataset_id)
        """)

    _003_drop_dataset_on_host_view_fwd = _dd("""\
        DROP VIEW IF EXISTS dataset_on_host_view
        """)

    _003_drop_time_column_from_dataset_on_host_back = _dd("""\
        ALTER TABLE dataset_on_host
            DROP COLUMN IF EXISTS time
        """)

    _003_create_dataset_on_host_view_back = _dd("""
        CREATE VIEW dataset_on_host_view AS
            SELECT
                dataset_on_host.id AS id,
                dataset.uuid AS dataset_uuid,
                host_view.uuid AS host_uuid
            FROM
                dataset_on_host
                INNER JOIN dataset
                    ON dataset.id = dataset_on_host.dataset_id
                INNER JOIN host_view
                    ON host_view.id = dataset_on_host.host_id
        """)

    _004_create_table_trusted_host_at_node_fwd = _dd("""
        CREATE TABLE trusted_host_at_node (
            id integer NOT NULL
                PRIMARY KEY
                REFERENCES host_at_node(id)
                    ON UPDATE CASCADE ON DELETE CASCADE,
            for_storage boolean NOT NULL,
            for_restore boolean NOT NULL
        )
        """)

    _004_create_index_trusted_host_at_node_idx_id_restore_fwd = _dd("""
        CREATE INDEX trusted_host_at_node_idx_id_for_restore
            ON trusted_host_at_node USING btree (id, for_restore)
        """)

    _004_create_index_trusted_host_at_node_idx_id_storage_fwd = _dd("""
        CREATE INDEX trusted_host_at_node_idx_id_for_storage
            ON trusted_host_at_node USING btree (id, for_storage)
        """)

    _004_drop_table_trusted_host_at_node_back = _dd("""
        DROP TABLE IF EXISTS trusted_host_at_node
        """)

    _005_create_table_user_at_node_fwd = _dd("""
        CREATE TABLE user_at_node (
            id integer NOT NULL
                PRIMARY KEY
                REFERENCES "user"(id)
                    ON UPDATE CASCADE ON DELETE CASCADE,
            suspended boolean NOT NULL DEFAULT False
        );
        """)

    _005_add_users_to_user_at_node_table_fwd = _dd("""
        INSERT INTO user_at_node SELECT id FROM "user";
        """)

    _005_drop_table_user_at_node_back = _dd("""
        DROP TABLE IF EXISTS user_at_node;
        """)


    def _006_copy_users_and_hosts_from_db_to_ds_fwd(self):
        with self._rdbw_factory() as rdbw:
            users = list(Queries.Inhabitants.get_all_users(rdbw))
            users_by_user_name_uc = {user.name.upper(): user
                                         for user in users}
            hosts = list(TrustedQueries.HostAtNode.get_all_hosts(
                             users_by_user_name_uc, rdbw))
            # List is to get the value out of the cursor
            thosts = list(TrustedQueries.HostAtNode.get_all_trusted_hosts(
                              username_uc_user_map=users_by_user_name_uc,
                              rdbw=rdbw))

        all_usernames = {u.name for u in users}
        trusted_usernames = {th.user.name for th in thosts}
        untrusted_usernames = all_usernames - trusted_usernames

        with self._fdbw_factory() as fdbw:
            logger.debug('Adding trusted users...')
            for uname in trusted_usernames:
                user = users_by_user_name_uc[uname.upper()]
                logger.verbose('Adding %r', user)
                FDBQueries.Users.add_user(username=user.name,
                                          digest=user.digest,
                                          is_trusted=True,
                                          fdbw=fdbw)

            logger.debug('Adding untrusted users...')
            for uname in untrusted_usernames:
                user = users_by_user_name_uc[uname.upper()]
                logger.verbose('Adding %r', user)
                FDBQueries.Users.add_user(username=user.name,
                                          digest=user.digest,
                                          is_trusted=False,
                                          fdbw=fdbw)

            logger.debug('Adding hosts...')
            for host in hosts:
                logger.verbose('Adding %r', host)
                FDBQueries.Users.add_host(username=host.user.name,
                                          host_uuid=host.uuid,
                                          fdbw=fdbw)


    def _007_delete_gfs_passthrough_chunks_fwd(self):
        with self._bdbw_factory() as bdbw:
             bdbw.passthrough_chunks.chunks.drop()
             bdbw.passthrough_chunks.files.drop()


    _008_create_max_storage_space_view_pg_fwd = _dd("""\
        CREATE OR REPLACE VIEW max_storage_space_view AS
            SELECT
                host_id,
                to_number(concat('0', value), 'S999999999999999D99')
                    * 1048576.0 AS max_size
            FROM setting_view
            WHERE name = '{}'
        """).format(Queries.Settings.MAX_STORAGE_SIZE_MIB)

    _008_create_max_storage_space_view_pg_back = _dd("""\
        CREATE OR REPLACE VIEW max_storage_space_view AS
            SELECT
                host_id,
                to_number(value, 'S999999999999999D99') * 1048576.0
                    AS max_size
            FROM setting_view
            WHERE name = '{}'
        """).format(Queries.Settings.MAX_STORAGE_SIZE_MIB)

    _008_create_max_storage_space_view_fwd = {
        'postgresql': _008_create_max_storage_space_view_pg_fwd,
    }
    _008_create_max_storage_space_view_back = {
        'postgresql': _008_create_max_storage_space_view_pg_back,
    }


    _009_set_merged_fwd = _dd("""\
        UPDATE dataset
            SET merged = True
            WHERE id IN (
                SELECT ds_update.id
                    FROM
                        dataset AS ds_update
                        INNER JOIN dataset AS ds_start ON
                            ds_update.id > ds_start.id AND
                            ds_update.group_id = ds_start.group_id AND
                            ds_update.time_started = ds_start.time_started
                        INNER JOIN dataset AS ds_end ON
                            ds_update.id > ds_end.id AND
                            ds_update.group_id = ds_end.group_id AND
                            ds_update.time_completed = ds_end.time_completed
                    WHERE
                        ds_start.id != ds_end.id
            )
        """)


    # General constants

    __p = strptime

    __new_migrations = {
        (__p('2012-05-31 21:40:00.000000'), __p('2012-06-07 16:31:00.000000')):
            Migration(
                forward=[
                    _003_add_time_column_to_dataset_on_host_fwd,
                    _003_set_time_column_from_dataset_fwd,
                    _003_set_first_upload_time_fwd,
                    _003_drop_dataset_on_host_view_fwd,
                ],
                backward=[
                    _003_drop_time_column_from_dataset_on_host_back,
                    _003_create_dataset_on_host_view_back,
                ]
            ),
        (__p('2012-06-07 16:31:00.000000'), __p('2012-06-08 22:25:00.000000')):
            Migration(
                forward=[
                    _004_create_table_trusted_host_at_node_fwd,
                    _004_create_index_trusted_host_at_node_idx_id_restore_fwd,
                    _004_create_index_trusted_host_at_node_idx_id_storage_fwd
                ],
                backward=[
                    _004_drop_table_trusted_host_at_node_back
                ]
            ),
        (__p('2012-06-08 22:25:00.000000'), __p('2012-06-27 17:25:00.000000')):
            Migration(
                forward=[
                    _005_create_table_user_at_node_fwd,
                    _005_add_users_to_user_at_node_table_fwd
                ],
                backward=[_005_drop_table_user_at_node_back]
            ),
        # No backward migration, as this is the introduction of MongoDB.
        (__p('2012-06-27 17:25:00.000000'), __p('2012-07-24 17:57:00.000000')):
            Migration(
                forward=[_006_copy_users_and_hosts_from_db_to_ds_fwd],
                backward=[]
            ),
        # Delete the outdated GridFS collections
        (__p('2012-08-01 17:45:00.000000'), __p('2012-08-20 17:45:00.000000')):
            Migration(
                forward=[_007_delete_gfs_passthrough_chunks_fwd],
                backward=[]
            ),
        (__p('2012-09-12 21:01:00.000000'), __p('2013-01-07 20:35:00.000000')):
            Migration(
                forward=[_008_create_max_storage_space_view_fwd],
                backward=[_008_create_max_storage_space_view_back]
            ),
        (__p('2013-03-14 19:18:00.000000'), __p('2013-03-15 17:43:00.000000')):
            Migration(
                forward=[_009_set_merged_fwd],
                backward=[]
            ),
    }

    # Note that it modifies the values in the common.migration as well
    for key, migr in __new_migrations.iteritems():
        basemigr = MigrationPlan._migrations.setdefault(
                       key, Migration(forward=[], backward=[]))
        basemigr.forward.extend(migr.forward)
        basemigr.backward.extend(migr.backward)
