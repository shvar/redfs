#!/usr/bin/python
"""
Host-specific database migration scripts.
"""

#
# Imports
#

from __future__ import absolute_import
import logging
from textwrap import dedent as _dd

from common.migration import MigrationPlan, Migration
from common.utils import strptime



#
# Logging
#

logger = logging.getLogger(__name__)



#
# Classes
#

class HostMigrationPlan(MigrationPlan):
    """The migration plan on the Host."""


    def __init__(self, *args, **kwargs):
        super(HostMigrationPlan, self).__init__(*args, **kwargs)


    # Migrations

    _001_update_file_local_state_at_host_constraint = [
        _dd("""\
            CREATE TABLE file_local_state_at_host_new(
                    id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
                    file_local_at_host_id INTEGER NOT NULL,
                    file_local_id INTEGER,
                    time_changed DATETIME DEFAULT CURRENT_TIMESTAMP NOT NULL,
                    isdir BOOLEAN DEFAULT '0' NOT NULL,
                    size BIGINT CHECK (size IS NULL or size >= 0),
                    UNIQUE (file_local_at_host_id, time_changed),
                    FOREIGN KEY(file_local_at_host_id)
                        REFERENCES file_local_at_host (id)
                        ON DELETE CASCADE ON UPDATE CASCADE
                        NOT DEFERRABLE INITIALLY IMMEDIATE,
                    FOREIGN KEY(file_local_id)
                        REFERENCES file_local (id)
                        ON DELETE CASCADE ON UPDATE CASCADE
                        NOT DEFERRABLE INITIALLY IMMEDIATE,
                    CHECK (isdir IN (0, 1))
            )
            """),
        _dd("""
            INSERT INTO file_local_state_at_host_new(
                     id,
                     file_local_at_host_id,
                     file_local_id,
                     time_changed,
                     isdir,
                     size
                 )
                SELECT id,
                       file_local_at_host_id,
                       file_local_id,
                       time_changed,
                       isdir,
                       size
                    FROM file_local_state_at_host
            """),
        _dd("""
            DROP TABLE file_local_state_at_host
            """),
        _dd("""
            ALTER TABLE file_local_state_at_host_new
                RENAME TO file_local_state_at_host
            """),
        _dd("""
            CREATE INDEX file_local_state_at_host_file_lch_ch_index
                ON file_local_state_at_host (
                    file_local_at_host_id,
                    time_changed,
                    file_local_id
                );
            """),
        _dd("""
            CREATE INDEX file_local_state_at_host_fk_file_local
                ON file_local_state_at_host (file_local_id);
            """),
    ]

    # General constants

    __p = strptime

    __new_migrations = {
        # No backward migration, since this is a bugfix
        (__p('2011-11-14 20:35:00.000000'), __p('2012-05-29 14:35:00.000000')):
            Migration(
                forward=[
                     # No backward migration, since this is a bugfix
                    _001_update_file_local_state_at_host_constraint,
                ],
                backward=[]
            ),
    }

    # Note that it modifies the values in the common.migration as well
    for key, migr in __new_migrations.iteritems():
        basemigr = MigrationPlan._migrations.setdefault(
                       key, Migration(forward=[], backward=[]))
        basemigr.forward.extend(migr.forward)
        basemigr.backward.extend(migr.backward)
