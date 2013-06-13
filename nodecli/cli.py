#!/usr/bin/python
# -*- coding: utf-8 -*-
"""Command-line interface for the Node."""

#
# Imports
#

from __future__ import absolute_import
import logging
import os
import traceback
from uuid import UUID

from contrib.dbc import contract_epydoc

from common.cli import cli_command, cli_error, try_parse_uuid
from common.crypto import gen_rand_key, Cryptographer
from common.db import Queries
from common.inhabitants import UserGroup
from common.typed_uuids import UserGroupUUID
from common.utils import gen_uuid, _nottest

from trusted import data_operations as data_ops, db, docstore as ds
from trusted.db import TrustedQueries
from trusted.chunk_storage_bigdb import ChunkStorageBigDB
from trusted.docstore.fdbqueries import FDBQueries
from trusted.docstore.bdbqueries import BDBQueries
from trusted.migration import TrustedMigrationPlan

from node import settings
from node.node_process import NodeApp



#
# Constants
#

logger = logging.getLogger(__name__)

try:
    from node_magic import SQLALCHEMY_ECHO
except ImportError:
    SQLALCHEMY_ECHO = False



#
# Functions
#

def proceed_with_node():
    """
    Read the node configuration for the node process.
    If needed, the database file is created and initialized as well.

    If it fails, it returns None and prints the reason of the failure
    (so the caller may not bother doing it).

    @returns: The port-to-node_settings mapping, like
        C{settings.get_my_nodes_settings()}; or None if failed for some reason.
    @rtype: dict, NoneType
    """
    settings.configure_logging(postfix='common')
    node_settings_map = settings.get_my_nodes_settings()

    rel_db_url = settings.get_common().rel_db_url
    db.init(rel_db_url, SQLALCHEMY_ECHO)

    node_common_settings = settings.get_common()
    try:
        ds.init(fast_db_url=node_common_settings.fast_db_url,
                big_db_url=node_common_settings.big_db_url)

    except ds.MongoDBInitializationException as e:
        print(u'MongoDB problem: {}'.format(e))

    else:
        # Create the database if the file is missing or 0 bytes long.
        if not db.RDB.is_schema_initialized:
            print('Initializing database schema...')
            try:
                db.create_node_db_schema()
            except Exception:
                cli_error('cannot create the RDB schema:\n%s',
                          traceback.format_exc())
            else:
                print('Successfully initialized.')

        else:
            # If the database is available, launch maintenance procedures.
            with db.RDB() as rdbw:
                Queries.System.maintenance(rdbw)
            logger.debug('Maintenance done')

            __migrate_if_needed()

        return node_settings_map


def unproceed_with_node():
    """Revert the actions of proceed_with_node()."""
    db.uninit()


def __migrate_if_needed():
    """Migrate the database(s) to the most up-to-date version."""
    mig = TrustedMigrationPlan(rdbw_factory=db.RDB,
                               fdbw_factory=ds.FDB,
                               bdbw_factory=ds.BDB)
    mig.auto_upgrade()


@cli_command(('-l', '--launch'),
             'Launch the Node process.\n'
                 'Can be used without arguments with the same behaviour.\n'
                 'Example:\n'
                 '  {bin} {cmd}\n'
                 '  {bin}')
def launch_node(arguments):
    node_map = proceed_with_node()
    if node_map is not None:
        node_app = NodeApp(node_map)
        node_app.first_start()
        # But it is not yet started, until the reactor is launched as well.

        # Launch reactor
        node_app.start_reactor()


@cli_command(('-i', '--init'),
             'Node initialization (if needed), without launching '
                 'the node process.')
def init_node(db_file_path):
    """
    Just initialize the database (if needed), without launching the reactor.
    """
    dummy_node_map = proceed_with_node()


@cli_command(('-ag', '--add-group'),
             'Add group by its name and, optionally, by its group UUID.\n'
                 'Example:\n'
                 '  {bin} {cmd} VasyaWare'
                 '  {bin} {cmd} VasyaTeam 0000000-AAAA-0000-0000-000000000001')
def add_group(arguments):
    node_map = proceed_with_node()

    if node_map is not None:
        if len(arguments) < 1:
            cli_error('You must pass at least 1 argument to this command: '
                          'the name of the group.')
        else:
            groupname_str = arguments.popleft()

            # Parse group UUID (optional)
            try:
                group_uuid = UUID(arguments[0])
                # Parsed successfully
                arguments.popleft()
            except (IndexError, ValueError):
                group_uuid = gen_uuid()

            user_group = UserGroup(uuid=UserGroupUUID.safe_cast_uuid(
                                            group_uuid),
                                   name=str(groupname_str),
                                   private=False,
                                   enc_key=gen_rand_key())
            NodeApp.add_ugroup(user_group)
            print(u'Added group "{}" with UUID {}'
                      .format(groupname_str, group_uuid))


@cli_command(('-au', '--add-user'),
             'Add user by its username.\n'
             'If "--to-group" is not present, then creates a new group '
                 'with the same name,\n'
             'and requires the password digest, and, optionally, '
                 'the group UUID.\n'
             'If "--to-group" is present, then adds the user to '
                 'the existing group (by its UUID).\n'
                 'Example:\n'
                 '  {bin} {cmd} vasya vasyaspassworddigest\n'
                 '  {bin} {cmd} vasya vasyaspassworddigest '
                     '0000000A-1111-0000-0000-000000000001\n'
                 '  {bin} {cmd} vasya --to-group '
                     '0000000A-2222-0000-0000-000000000001')
def add_user(arguments):
    node_map = proceed_with_node()

    if node_map is not None:
        if not arguments:
            cli_error('No user name passed')
        else:
            username_str = arguments.popleft()

            if arguments and arguments[0] == '--to-group':
                dummy = arguments.popleft()
                if not arguments:
                    cli_error('Attempt to add user to group, '
                                  'but no group specified!')
                else:
                    username = str(username_str)
                    group_uuid = try_parse_uuid(arguments.popleft())

                    # Let's add the user to group
                    NodeApp.add_user_to_group(
                        username=username,
                        group_uuid=UserGroupUUID.safe_cast_uuid(group_uuid))
                    print(u'Added user "{}" to the group "{}"'
                              .format(username, group_uuid))

            else:
                # Let's add the user to the system
                __add_new_regular_user(username_str, arguments)


@contract_epydoc
def __add_new_regular_user(username, arguments):
    if not arguments:
        cli_error('No digest string passed!')
    else:
        # Parse digest
        digest_str = arguments.popleft()
        try:
            digest = '{:040x}'.format(int(digest_str, 16))
        except:
            cli_error('The argument \"{}\" is not a valid hexadecimal digest!'
                          .format(digest_str))
        if len(digest) != 40:
            cli_error('The digest argument should be a hexadecimal number '
                          'up to 40 hexadigits long rather than {}!'
                          .format(digest))

        # Parse group UUID (optional)
        try:
            group_uuid = UUID(arguments[0])
            # Parsed successfully
            arguments.popleft()
        except (IndexError, ValueError):
            group_uuid = gen_uuid()

        # Finally, add user
        NodeApp.add_user(UserGroupUUID.safe_cast_uuid(group_uuid),
                         username=str(username),
                         digest=digest,
                         is_trusted=False)
        print(u'Added user "{}"'.format(username))


@cli_command(('-gu', '--greet-user'), 'send greeting to user')
def greet_user(arguments):
    node_map = proceed_with_node()

    if node_map is not None:
        if not arguments:
            cli_error('No user name passed')
        elif len(arguments) < 1:
            cli_error('You must pass at least 1 arguments to this command: '
                          'the name of the user.')
        else:
            username_str = arguments.popleft()
            with ds.FDB() as fdbw:
                FDBQueries.UserMessages.add_message(
                    username=str(username_str),
                    key='user_greeting {}'.format(gen_uuid()),
                    body={
                        'c': ('<p class="bold">{username}, welcome to our '
                              'system</p>'
                              '<p>This is a test message.</p>').format(
                                 username=username_str),
                        'en': ('<p class="bold">{username}, welcome to our '
                               'system</p>'
                               '<p>This is a test message.</p>').format(
                                  username=username_str),
                        'ru': (u'<p class="bold">Привет, {username}</p>'
                               u'<p>Это тестовое сообщение.</p>').format(
                                  username=username_str)
                    },
                    fdbw=fdbw)
            print(u'The greeting message has been sent to "{}"'
                      .format(username_str))


@cli_command(('-ah', '--add-host'),
             'DEBUG: Add host (by its UUID) to the particular user '
                     '(by username).\n'
                 'Arg. 1: username.\n'
                 'Arg. 2: host UUID\n'
                 'Example:\n'
                 '  {bin} {cmd} vasya '
                     '00000000-1111-0000-0000-000000000001')
def add_host(arguments):
    if len(arguments) < 2:
        cli_error('You must pass at least the user name '
                      'and the host UUID!')
    else:
        username = str(arguments.popleft())
        host_uuid = try_parse_uuid(arguments.popleft())

        node_map = proceed_with_node()

        if node_map is not None:
            NodeApp.add_host(username=username,
                             hostname=str(host_uuid),
                             host_uuid=host_uuid,
                             trusted_host_tuple=None)
            print(u'Added host {} to "{}"'.format(host_uuid, username))


@cli_command(('-at', '--add-trusted-host'),
             'Add trusted host by its UUID, username and '
                     'password digest.\n'
                 'Example:\n'
                 '  {bin} {cmd} trustedvasya trustedvasyaspassworddigest '
                     '00000000-1111-0000-0000-000000000001 ')
def add_trusted_host(arguments):
    if len(arguments) < 3:
        cli_error('You must pass at least the user name, password digest '
                      'and the host UUID!')
    else:
        username = str(arguments.popleft())
        digest = str(arguments.popleft())
        host_uuid = try_parse_uuid(arguments.popleft())

        node_map = proceed_with_node()

        if node_map is not None:
            # Finally, add user
            NodeApp.add_user(UserGroupUUID.safe_cast_uuid(gen_uuid()),
                             username=str(username),
                             digest=digest,
                             is_trusted=True)
            _for_storage = True
            _for_restore = False
            NodeApp.add_host(username=username,
                             hostname='Trusted: {}'.format(host_uuid),
                             host_uuid=host_uuid,
                             trusted_host_tuple=(_for_storage,
                                                 _for_restore))
            NodeApp.change_user(username, digest)
            print(u'Added Trusted Host {!r}'.format(username))


if __debug__:
    @_nottest
    def __test_upload_remove_files(group_uuid, file_paths, chunk_storage,
                                   op_upload, rdbw):
        """Kind of, upload multiple files to the cloud.

        @note: temporary, for tests only.

        @type group_uuid: UserGroupUUID
        @param upload: whether the operation is "upload" (if C{True})
            or "remove" (if C{False}).
        @type upload: bool
        """
        GROUP_ENCRYPTION = False

        if GROUP_ENCRYPTION:
            # Read group key from the user group
            _ugroup = Queries.Inhabitants.get_ugroup_by_uuid(group_uuid,
                                                             rdbw)
            group_key = _ugroup.enc_key
        else:
            group_key = None

        cryptographer = Cryptographer(group_key=group_key,
                                      key_generator=None)

        files = (data_ops.FileToUpload(
                         base_dir=data_ops.DEFAULT_SYNC_BASE_DIR_NAME,
                         rel_path=os.path.basename(path),
                         size=os.stat(path).st_size if op_upload else None,
                         file_getter=lambda p=path: open(p, 'rb'))
                     for path in file_paths)

        ds_uuid = data_ops.upload_delete_files(
                      base_dir=data_ops.DEFAULT_SYNC_BASE_DIR_NAME,
                      files=files,
                      ds_name='DS',
                      group_uuid=group_uuid,
                      sync=True,
                      cryptographer=cryptographer,
                      rdbw=rdbw,
                      chunk_storage=chunk_storage)


    @_nottest
    def __test_web_upload_remove(op_upload, arguments):
        """Common code for CLI operations for web upload/web remove."""
        op_name = 'upload' if op_upload else 'remove'

        if len(arguments) < 2:
            cli_error(u'You must pass at least 2 arguments to this command: '
                      u'the UUID of the group, and at least '
                      u'one filename to {op}.'
                          .format(op=op_name))
        else:
            group_uuid = UserGroupUUID.safe_cast_uuid(
                            try_parse_uuid(arguments.popleft()))

            file_paths = []
            while arguments:
                file_paths.append(arguments.popleft())

            print(u'{} file paths for group {}:\n{}'.format(
                      op_name.capitalize(), group_uuid, '\n'.join(file_paths)))

            node_map = proceed_with_node()

            chunk_storage = ChunkStorageBigDB(bdbw_factory=ds.BDB)

            with db.RDB() as rdbw:
                __test_upload_remove_files(group_uuid, file_paths,
                                           chunk_storage, op_upload, rdbw)


    @_nottest
    @cli_command(('-twu', '--test-web-upload'),
                 'Initiate web upload (of multiple files for some '
                         'user group).\n'
                     'Arg. 1: group UUID.\n'
                     'Arg. 2,..: file paths to upload.\n'
                     'Example:\n'
                     '  {bin} {cmd} 00000000-BBBB-0000-0000-000000000001 '
                         'filepath1 ...')
    def test_web_upload(arguments):
        __test_web_upload_remove(op_upload=True, arguments=arguments)


    @_nottest
    @cli_command(('-twr', '--test-web-remove'),
                 'Initiate web remove (of multiple files for some '
                         'user group).\n'
                     'Arg. 1: group UUID.\n'
                     'Arg. 2,..: file paths to remove.\n'
                     'Example:\n'
                     '  {bin} {cmd} 00000000-BBBB-0000-0000-000000000001 '
                         'filepath1 ...')
    def test_web_remove(arguments):
        __test_web_upload_remove(op_upload=False, arguments=arguments)


    @_nottest
    @cli_command(('-twd', '--test-web-download'),
                 'Initiate web download of a file.\n'
                     'Arg. 1: dataset UUID.\n'
                     'Arg. 2: base directory of the file.\n'
                     'Arg. 3: relative path of the file.\n'
                     'Example:\n'
                     '  {bin} {cmd} 00000000-BBBB-0000-0000-000000000001 '
                         '/home/john/Folder somefile.txt')
    def test_web_download(arguments):
        node_map = proceed_with_node()

        if len(arguments) < 3:
            cli_error(u'You must pass at least 3 arguments to this command: '
                      u'the dataset UUID , '
                      u'base directory of the file, '
                      u'and the relative path of the file.'
                          .format(op=op_name))
        else:
            ds_uuid = try_parse_uuid(arguments.popleft())
            base_dir = arguments.popleft()
            rel_path = arguments.popleft()

            edition = settings.get_common().edition

            print(u'Downloading file from dataset {}, base directory {}, '
                  u'path {}'
                      .format(ds_uuid, base_dir, rel_path))

            with db.RDB() as rdbw, ds.BDB() as bdbw:
                cr = data_ops.get_cryptographer(ds_uuid, edition, rdbw)

                file_data = data_ops.download_file(ds_uuid, base_dir, rel_path,
                                                   edition, rdbw)

                result_filename = os.path.basename(rel_path)

                logger.debug('Writing to file %r %i bytes long',
                             result_filename, file_data.size)
                with open(result_filename, 'wb') as fh:
                    # Preallocate file
                    if file_data.size:
                        fh.seek(file_data.size - 1)
                        fh.write('\x00')
                    fh.seek(0)
                    # Now write the file contents.
                    try:
                        for bl in data_ops.get_file_of_blocks_gen(
                                      file_data.blocks, cr, bdbw=bdbw):
                            fh.write(bl)
                    except BDBQueries.Chunks.NoChunkException as e:
                        logger.error('Problem while downloading the file: %s',
                                     e)
                        print(u'Error: file {!r} cannot be created!'.format(
                                  result_filename))


@cli_command(('-gagsi', '--get-all-groups-space-info'),
             'Get the information about the space utilization by '
                     'every user group.\n'
                 'Example:\n'
                 '  {bin} {cmd}')
def get_all_groups_space_info(arguments):
    node_map = proceed_with_node()
    print('Calculating the utilization info for groups.')

    with db.RDB() as rdbw:
        group_space_info = data_ops.get_group_space_info(rdbw)
        print('\nPer-group space info:')
        for per_group in group_space_info.per_group:
            print('   {!r}'.format(per_group))
        print('\nOverall information:')
        print('   {!r}'.format(group_space_info.overall))


@cli_command(('-guph', '--get-user-presence-history'),
             'Get the presence history for a user.\n'
                 'Example:\n'
                 '  {bin} {cmd} username')
def get_user_presence_history(arguments):
    username = arguments.popleft()

    node_map = proceed_with_node()

    if node_map is not None:
        print('Getting the presence history for user {}...'.format(username))

        with db.RDB() as rdbw:
            presence_history = \
                TrustedQueries.PresenceStat.get_user_presence_history(username,
                                                                      rdbw)
            for host_name, data in presence_history.iteritems():
                print('Host "{}"'.format(host_name))
                for dt, count in data:
                    print('  {}: {} time(s)'.format(dt, count))


@cli_command(('-ggsi', '--get-group-space-info'),
             'Get the information about the space utilization by '
                     'some group.\n'
                 'Example:\n'
                 '  {bin} {cmd} group_uuid')
def get_group_space_info(arguments):
    group_uuid = UUID(arguments.popleft())

    node_map = proceed_with_node()

    if node_map is not None:
        print('Calculating the utilization info for group {}.'
                  .format(group_uuid))

        with db.RDB() as rdbw:
            host_space_stat = TrustedQueries.SpaceStat.get_hosts_space_stat2(
                                  group_uuid, rdbw)

            for per_host in host_space_stat:
                print('  Host {}: max {}, used {}'.format(
                          per_host.host_name,
                          per_host.max_size,
                          per_host.used_size))
