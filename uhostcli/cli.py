#!/usr/bin/python
"""
Command-line interface for the Host component.
"""

#
# Imports
#

from __future__ import absolute_import
import getpass
import locale
import logging
import os
import traceback
from datetime import datetime
from itertools import imap
from uuid import UUID

from twisted.python.failure import Failure

from contrib.dbc import contract_epydoc

from common import crypto, settings as common_settings
from common.cli import cli_command, cli_error, try_parse_uuid
from common.db import Queries
from common.itertools_ex import inonempty
from common.os_ex import safe_stat
from common.path_ex import normpath_nodot
from common.utils import exceptions_logged, gen_uuid

from host import settings as host_settings, db
from host.db import HostQueries

from uhost import settings as uhost_settings
from uhost.chunk_storage_fs import ChunkStorageFS
from uhost.uhost_process import UHostApp



#
# Constants/variables
#

logger = logging.getLogger(__name__)

PASSWORD = None



#
# Functions
#

def cli_host_uuid_error():
    """
    This function is not exported, but used only inside this module.
    """
    cli_error('You must pass at least the host UUID to this command.')


def proceed_with_host_uuid_cli(*args, **kwargs):
    """
    Given the host UUID, run all the preparations which are needed
    to access the logging framework, do the database access, etc.

    The node which is used is (at the moment) ALWAYS taken from
    the default host.conf file.

    @todo: Some day we need to use the node settings from the DB.
           Should it be done here?
    """
    h_i_exc = UHostApp.HostInitExceptions

    try:
        UHostApp.proceed_with_host_uuid(*args, **kwargs)
    except common_settings.UnreadableConfFileException as e:
        cli_error('Cannot read .conf file from {!r}!'.format(e.path))
    except h_i_exc.IncorrectDefaultNodeSettingsException:
        cli_error('Default node settings in the .conf file are incorrect!')
    except h_i_exc.NoNodeUrlsException:
        cli_error('Configuration file should contain '
                      'at least a single node url.')
    except h_i_exc.NoHostDbException as e:
        cli_error('Database is absent: %s', e.base_exc)
    except h_i_exc.DBInitializationIssue as e:
        cli_error('Could not initialize the DB: %s\n%s\n%s',
                  e.base_exc,
                  e.base_exc.base_exc,
                  u'{}\n-------'.format(e.base_exc.msg) if __debug__ else '')
    except h_i_exc.UnknownIssue as e:
        cli_error('Unexpected error during the DB initialization: %s',
                  e.base_exc)
    else:
        print('Successfully initialized.')


def __cli_handle_init_finish(username, login_ack):
    if (not isinstance(login_ack, Failure) and
        not isinstance(login_ack, Exception)):
        # Successful messaging (though the authentication could be rejected)
        (ack_result,
         ack_username,
         ack_host_uuid,
         ack_user_groups,
         ack_ssl_cert,
         ssl_pkey) = login_ack

        if ack_result == 'ok':
            print('For username {} (originally, {}), UUID is {}.'
                      .format(username, ack_username, ack_host_uuid))
            if ack_ssl_cert is not None:
                subj = ack_ssl_cert.get_subject()
                print('Received SSL certificate for {0} UUID {1}'
                          .format(subj.organizationalUnitName,
                                  subj.commonName))
        elif ack_result == 'fail':
            print('Could not authenticate the username {}'.format(username))
        elif ack_result == 'dual login':
            print('The user {} is already logged in to the service.'
                      .format(username))
        else:
            print('UNKNOWN RESULT: {!r}'.format(ack_result))

    else:
        print('Error: could not authenticate at the node '
                  'with the username {0} due to {1!r}!'
                  .format(username, login_ack))


def __create_chunk_storage():
    """
    Create and return an instance of C{ChunkStorageFS} upon the DB settings
    which contain the path to store the chunks.

    @rtype: ChunkStorageFS
    """
    return ChunkStorageFS(host_settings.get_chunk_storage_path())


@cli_command(
    ('-i', '--init'),
    'Host initialization.\n'
        'Arg. 1: port to listen.\n'
        'Arg. 2: username.\n'
        'Arg. 3: password (optional), '
            'requested interactively if omitted.\n'
        'Example:\n'
        '  {bin} {cmd} 47821 johndoe\n'
        '  {bin} {cmd} 47821 johndoe johnlovesbecky')
def init_host(arguments):
    """
    Initialize the host and run the first ever authentication.
    """
    if len(arguments) < 2:
        cli_error('You must pass at least 2 arguments to this command.')
    else:
        my_listen_port_str, username = (arguments.popleft(),
                                        str(arguments.popleft()))
        password = _get_password_from_arguments(arguments)

        try:
            my_listen_port = int(my_listen_port_str)
        except ValueError:
            cli_error('Not an integer port number: %r',
                      my_listen_port_str)

        if username is not None and password is not None:
            digest = \
                crypto.generate_digest(username,
                                       password,
                                       common_settings.HTTP_AUTH_REALM_NODE)
        else:
            digest = None

        edition = uhost_settings.detect_edition()

        UHostApp.init_host(edition=edition,
                           chunk_storage_cb=__create_chunk_storage,
                           proceed_func=proceed_with_host_uuid_cli,
                           on_end_func=__cli_handle_init_finish,
                           username=username,
                           digest=digest,
                           my_listen_port=my_listen_port)


@cli_command(
    ('-l', '--launch'),
    'Launch the Host process.\n'
        'Arg. 1: base host UUID.\n'
        'Example:\n'
        '  {bin} {cmd} 00000000-1111-0000-0000-000000000001')
def launch_host(arguments):
    """Launch main host loop."""
    if len(arguments) < 1:
        cli_host_uuid_error()
    else:
        # This MUST alter the arguments variable, not its copy
        my_uuid = try_parse_uuid(arguments.popleft())

        proceed_with_host_uuid_cli(my_uuid)

        # Launch the main host app
        host_app = UHostApp(my_uuid,
                            uhost_settings.detect_edition(),
                            __create_chunk_storage())
        host_app.first_start()
        # But it is not yet started, until the reactor is launched as well.

        # Launch reactor
        host_app.start_reactor()
        logger.debug('Host reactor completed.')


DATETIME_STRFTIME_FORMAT = '%x %X'


@cli_command(
    ('-riad', '--request-info-all-datasets'),
    'Request the datasets available in the cloud for the host.\n'
        'Arg. 1: base host UUID.\n'
        'Example:\n'
        '  {bin} {cmd} 00000000-1111-0000-0000-000000000001')
def request_info_all_datasets(arguments):
    """
    Request info on datasets.
    """
    if len(arguments) < 1:
        cli_host_uuid_error()
    else:
        my_uuid = try_parse_uuid(arguments.popleft())

        proceed_with_host_uuid_cli(my_uuid)


        @exceptions_logged(logger)
        @contract_epydoc
        def on_reactor_start(app):
            """
            @type app: UHostApp
            """

            @exceptions_logged(logger)
            def on_datasets_received(datasets_list):
                try:
                    print('The following datasets are available:')
                    _format = u'  {0:<36} {1:<5} {2:<6} {3:<12} ' \
                                  '{4:<17} {5:<17} {6:<12}'
                    print(_format.format('Dataset UUID',
                                         'Files',
                                         'Chunks',
                                         'Total size',
                                         'Time started',
                                         'Time completed',
                                         'Dataset name'))
                    print(_format.format('-' * 36, '-' * 5, '-' * 6, '-' * 12,
                                         '-' * 17, '-' * 17, '-' * 12))
                    _encoding = locale.getpreferredencoding()
                    for dataset in datasets_list:
                        _formatted = \
                            _format.format(
                                dataset.uuid,
                                dataset.files_count(),
                                dataset.chunks_count(),
                                dataset.size(),
                                dataset.time_started
                                       .strftime(DATETIME_STRFTIME_FORMAT),
                                'Not completed yet'
                                    if dataset.time_completed is None
                                    else dataset.time_completed.strftime(
                                             DATETIME_STRFTIME_FORMAT),
                                dataset.name)
                        print(_formatted.encode(_encoding))
                except Exception:
                    traceback.print_exc()
                finally:
                    app.terminate_host()


            app.query_datasets(on_datasets_received)


        # Launch the main host app
        host_app = UHostApp(my_uuid,
                            uhost_settings.detect_edition(),
                            __create_chunk_storage(),
                            on_reactor_start=on_reactor_start)
        host_app.first_start()
        # But it is not yet started, until the reactor is launched as well.

        # Launch reactor
        host_app.start_reactor()


@cli_command(
    ('-rid', '--request-info-dataset'),
    'Request the cloud information about the files in the particular backup.\n'
        'Arg. 1: base host UUID.\n'
        'Arg. 2: dataset UUID.\n'
        'Example:\n'
        '  {bin} {cmd} 00000000-1111-0000-0000-000000000001 '
            'befe06ae-3e69-11df-8bdd-001d728e61cb')
def request_info_dataset_files(arguments):
    """
    Request info on particular dataset.
    """
    if len(arguments) < 2:
        cli_error('You must pass at least the host UUID '
                      'and the dataset UUID to this command.')
    else:
        my_uuid, ds_uuid = (try_parse_uuid(arguments.popleft()),
                            try_parse_uuid(arguments.popleft()))

        proceed_with_host_uuid_cli(my_uuid)


        @exceptions_logged(logger)
        @contract_epydoc
        def on_reactor_start(app):
            """
            @type app: UHostApp
            """

            @exceptions_logged(logger)
            @contract_epydoc
            def on_datasets_received(ds_uuid, dataset_files_dict):
                """
                @type ds_uuid: UUID
                @type dataset_files_dict: dict
                """
                try:
                    print('The following files are present in the dataset {}:'
                              .format(ds_uuid))
                    _format = u'    {0:<36s}   {1}'
                    print(_format.format('File UUID', 'File path'))
                    print(_format.format('-' * 36, '-' * len('File path')))

                    for root_dir in sorted(dataset_files_dict.iterkeys()):
                        print(u"  {}".format(root_dir))
                        for f in dataset_files_dict[root_dir]:
                            print(_format.format(f.uuid, f.full_path))
                except Exception:
                    traceback.print_exc()
                finally:
                    app.terminate_host()

            app.query_dataset_files(ds_uuid, on_datasets_received)


        # Launch the main host app
        host_app = UHostApp(my_uuid,
                            uhost_settings.detect_edition(),
                            __create_chunk_storage(),
                            on_reactor_start=on_reactor_start)
        host_app.first_start()
        # But it is not yet started, until the reactor is launched as well.

        # Launch reactor
        host_app.start_reactor()


def _get_password_from_arguments(arguments, repeat=False):
    """
    Utilitary function to get a password either from CLI arguments,
    or interactively.

    @param repeat: Whether the password should be re-requested
                   for confirmation.
    @type repeat: bool

    @rtype: str
    """
    if arguments:
        password = arguments.popleft()
    else:
        password = getpass.getpass('Enter password: ')
        if repeat:
            password2 = getpass.getpass('Repeat password: ')
            if password != password2:
                cli_error('Passwords do not match!')

    return password


@cli_command(
    ('-scp', '--save-connection-password'),
    'Save the connection password for the current user.\n'
        'Arg. 1: base host UUID.\n'
        'Arg. 2: password (optional), requested interactively if omitted.\n'
        'Example:\n'
        '  {bin} {cmd} 00000000-1111-0000-0000-000000000001 '
            'johndoe johnpassword')
def save_connection_password(arguments):
    """
    Generate digest for the connection password (for the current host)
    and save it.
    """
    if len(arguments) < 1:
        cli_error('The host UUID and (optionally) the password '
                      'should be passed as the arguments!')
    else:
        my_uuid = try_parse_uuid(arguments.popleft())
        password = _get_password_from_arguments(arguments, repeat=True)

        proceed_with_host_uuid_cli(my_uuid)

        with db.RDB() as rdbw:
            my_host = Queries.Inhabitants.get_host_by_uuid(my_uuid, rdbw)
            my_username = my_host.username

            _digest = \
                crypto.generate_digest(my_username,
                                       password,
                                       common_settings.HTTP_AUTH_REALM_NODE)

            Queries.Inhabitants.update_user_digest(my_username, _digest, rdbw)

        print(u'For host {host} (with user {user}), '
              u'saving the following digest: {digest}'
                  .format(host=my_uuid,
                          user=my_username,
                          digest=_digest))


@cli_command(
    ('-d', '--digest'),
    'Generate user digest.\n'
        'Arg. 1: username.\n'
        'Arg. 2: password (optional), '
            'requested interactively if omitted.\n'
        'Example:\n'
        '  {bin} {cmd} johndoe johnpassword')
def generate_digest(arguments):
    """
    Generate digest for the username.
    """
    if len(arguments) < 1:
        cli_error('The user name and (optionally) the password should be '
                      'passed as the arguments!')
    else:
        username = arguments.popleft()
        password = _get_password_from_arguments(arguments)
        print('For username {0}, the digest is {1}'
                  .format(username,
                          crypto.generate_digest(
                              str(username),
                              password,
                              common_settings.HTTP_AUTH_REALM_NODE)))


@cli_command(
    ('-dd', '--delete-dataset'),
    'Delete a dataset from the node.\n'
        'Arg. 1: base host UUID.\n'
        'Arg. 2: dataset UUID.\n'
        'Example:\n'
        '  {bin} {cmd} 00000000-1111-0000-0000-000000000001 '
        + str(gen_uuid()))
def delete_dataset(arguments):
    """
    Delete the dataset from the Node.
    """
    global _LOG_ACTIONS

    if len(arguments) < 2:
        cli_error('At least the host UUID and the dataset UUID '
                      'should be passed!')
    else:
        my_uuid, ds_uuid = (try_parse_uuid(arguments.popleft()),
                            try_parse_uuid(arguments.popleft()))

        print('Trying to delete the dataset {}'.format(ds_uuid))

        proceed_with_host_uuid_cli(my_uuid)


        @exceptions_logged(logger)
        @contract_epydoc
        def on_reactor_start(app):
            """
            @type app: UHostApp
            """

            @exceptions_logged(logger)
            def on_datasets_deleted(deleted_ds_uuids):
                print('Deleted the following backups successfully:')
                print('\n'.join('    {}'.format(u) for u in deleted_ds_uuids))
                app.terminate_host()


            app.delete_datasets_from_node(ds_uuids_to_delete=[ds_uuid],
                                          on_completed=on_datasets_deleted)


        # Launch the main host app
        host_app = UHostApp(my_uuid,
                            uhost_settings.detect_edition(),
                            __create_chunk_storage(),
                            on_reactor_start=on_reactor_start)
        host_app.first_start()

        # Launch reactor
        host_app.start_reactor()


@cli_command(
    ('-ss', '--set-setting'),
    'Set the configuration setting to its value.\n'
        'Supported setting names: \n'
        '' + '\n'.join(imap('  "{}"'.format,
                            sorted(Queries.Settings.ALL_SETTINGS)))
           + '\n' +
        'Warning: altering some of the settings may be unsafe.\n'
        'Arg. 1: base host UUID.\n'
        'Arg. 2: setting name.\n'
        'Arg. 3: new setting value.\n'
        'Example:\n'
        '  {bin} {cmd} 00000000-1111-0000-0000-000000000001 '
            '"node URLs" \'["node42.pvt:31337"]\'')
def set_setting(arguments):
    """
    Set some host setting to the value.
    """
    if len(arguments) < 3:
        cli_error('The host UUID, setting name and the value '
                      'should be passed as the arguments!')
    else:
        (my_uuid,
         setting_name,
         setting_value) = (try_parse_uuid(arguments.popleft()),
                           arguments.popleft(),
                           arguments.popleft())

        if setting_name not in Queries.Settings.ALL_SETTINGS:
            cli_error('Setting "%s" unsupported!',
                      setting_name)

        else:
            proceed_with_host_uuid_cli(my_uuid)


            @exceptions_logged(logger)
            @contract_epydoc
            def on_reactor_start(app):
                """
                @type app: UHostApp
                """
                print('Modifying {!r} to {!r}'.format(setting_name,
                                                      setting_value))
                app.set_setting(setting_name,
                                setting_value,
                                on_received=lambda x: app.terminate_host())


            # Launch the main host app
            host_app = UHostApp(my_uuid,
                                uhost_settings.detect_edition(),
                                __create_chunk_storage(),
                                on_reactor_start=on_reactor_start,
                                do_send_messages=False)
            host_app.first_start()
            # But it is not yet started, until the reactor is launched as well.

            # Launch reactor
            host_app.start_reactor()


@cli_command(
    ('-ps', '--print-settings'),
    'Print all configured settings together with their values.\n'
        'Arg. 1: base host UUID.\n'
        'Example:\n'
        '  {bin} {cmd} 00000000-1111-0000-0000-000000000001')
def print_all_settings(arguments):
    """
    Print all available settings together with their values.
    """
    if len(arguments) < 1:
        cli_host_uuid_error()
    else:
        my_uuid = try_parse_uuid(arguments.popleft())

        proceed_with_host_uuid_cli(my_uuid)

        print('Configured settings:')
        _settings = host_settings.get_all_settings_newer_than(datetime.min)
        for name, (value, time) in _settings.iteritems():
            print(u'  {0:<20s}: {1!r} (last update on {2})'
                      .format('{!r}'.format(name),
                              value if not isinstance(value, buffer)
                                    else '<binary {:d} bytes>'
                                             .format(len(value)),
                              time))


@cli_command(
    ('-pcs', '--print-cloud-stats'),
    'Print the overall cloud statistics.\n'
        'Arg. 1: base host UUID.\n'
        'Example:\n'
        '  {bin} {cmd} 00000000-1111-0000-0000-000000000001')
def print_cloud_stats(arguments):
    """
    Print the overall cloud statistics.
    """
    if len(arguments) < 1:
        cli_host_uuid_error()
    else:
        my_uuid = try_parse_uuid(arguments.popleft())

        proceed_with_host_uuid_cli(my_uuid)


        @exceptions_logged(logger)
        @contract_epydoc
        def on_reactor_start(app):
            """
            @type app: UHostApp
            """

            @exceptions_logged(logger)
            @contract_epydoc
            def on_cloud_stats_received(cloud_stats):
                """
                @type cloud_stats: col.Mapping
                """
                try:
                    _total_mb, _used_mb = (cloud_stats['total_mb'],
                                           cloud_stats['used_mb'])
                    print('The following statistics is available: \n'
                          '  Total hosts count: {0:d}\n'
                          '    Alive hosts now: {1:d}\n'
                          '         Cloud size: {2:d} MiB\n'
                          '    Used cloud size: {3:d} MiB ({4: 5.2f}%)\n'
                              .format(cloud_stats['total_hosts_count'],
                                      cloud_stats['alive_hosts_count'],
                                      int(_total_mb),
                                      int(_used_mb),
                                      _used_mb / _total_mb))

                except Exception:
                    traceback.print_exc()
                finally:
                    app.terminate_host()


            app.query_overall_cloud_stats(on_cloud_stats_received)


        # Launch the main host app
        host_app = UHostApp(my_uuid,
                            uhost_settings.detect_edition(),
                            __create_chunk_storage(),
                            on_reactor_start=on_reactor_start)
        host_app.first_start()
        # But it is not yet started, until the reactor is launched as well.

        # Launch reactor
        host_app.start_reactor()


def __print_data_stats(data_stats, ds_uuid, path):
    print('The following statistics is available:\n'
          'For {ds:s} and {path:s}, the following statistics is available:\n'
          '             Files : {file_count:d} ({uniq_file_count:d} unique)\n'
          '  Total file size  : {file_size:d} bytes '
                               '({uniq_file_size:d} bytes in unique files)\n'
          'Full data replicas : {full_replicas_count:d}\n'
          '            Chunks : {chunk_count:d} '
                               '({chunk_replicas_count:d} replicas, '
                                'ratio {chunk_ratio:.2f})\n'
          '        Hosts used : {hosts_count:d}'
              .format(ds='dataset {:s}'.format(ds_uuid)
                             if ds_uuid is not None
                             else 'any dataset',
                      path='path {:s}'.format(path) if path is not None
                                                    else 'any path',
                      **data_stats))


@cli_command(
    ('-pds', '--print-data-stats'),
    'Print the cloud statistics for some (or any possible) data path\n'
        'using the UUID from some dataset (probably any possible).\n'
        'Arg. 1: base host UUID.\n'
        'Arg. 2: dataset UUID, or escaped asterisk for any dataset.\n'
        'Arg. 3: data path, or escaped asterisk for any path.\n'
        'Example:\n'
        '  {bin} {cmd} 00000000-1111-0000-0000-000000000001 '
            'befe06ae-3e69-11df-8bdd-001d728e61cb /home/johndoe\n'
        '  {bin} {cmd} 00000000-1111-0000-0000-000000000001 '
            'befe06ae-3e69-11df-8bdd-001d728e61cb \\*\n'
        '  {bin} {cmd} 00000000-1111-0000-0000-000000000001 \\* /home/johndoe')
def print_data_stats(arguments):
    """
    Print the backup data statistics in the cloud.
    """
    if len(arguments) < 3:
        cli_error('The host UUID, the dataset UUID (or asterisk), '
                      'and the path (or asterisk)\n'
                      'should be passed as the arguments!')
    else:
        my_uuid, ds_uuid, path = (try_parse_uuid(arguments.popleft()),
                                  try_parse_uuid(arguments.popleft()),
                                  arguments.popleft())

        proceed_with_host_uuid_cli(my_uuid)


        @exceptions_logged(logger)
        @contract_epydoc
        def on_reactor_start(app):
            """
            @type app: UHostApp
            """

            def on_data_stats_received(ds_uuid, path, data_stats):
                try:
                    data_stats['chunk_ratio'] = \
                        float(data_stats['chunk_replicas_count']) / \
                        data_stats['chunk_count']

                    __print_data_stats(data_stats, ds_uuid, path)

                except Exception:
                    traceback.print_exc()
                finally:
                    app.terminate_host()

            app.query_data_replication_stats(ds_uuid if ds_uuid != '*'
                                                     else None,
                                             path if path != '*' else None,
                                             on_data_stats_received)


        # Launch the main host app
        host_app = UHostApp(my_uuid,
                            uhost_settings.detect_edition(),
                            __create_chunk_storage(),
                            on_reactor_start=on_reactor_start)
        host_app.first_start()
        # But it is not yet started, until the reactor is launched as well.

        # Launch reactor
        host_app.start_reactor()


def __get_all_remaining_args(arguments):
    """
    Given the list of arguments, get all ones which do not resemble
    any existing command.

    @rtype: list
    """
    doubledash_occured = False  # Is there -- construction present?
    result = []
    while True:
        if arguments:
            next_arg = arguments.popleft()
            if next_arg == '--':
                doubledash_occured = True
            elif next_arg.startswith('-') and not doubledash_occured:
                # Reached the next command
                arguments.appendleft(next_arg)
                break
            else:
                result.append(next_arg)

        else:
            # No more arguments
            break

    return result


@cli_command(
    ('-f', '--select-files'),
    'Prepare the files to the backup, passing the backup name.\n'
    'Currently, the backup is bound to the base user group of the user.\n'
        'Use "--" symbol to select all the remaining files.\n'
        'Arg. 1: base host UUID.\n'
        'Arg. 2: backup name.\n'
        'Arg. 3 and further: paths to backup.\n'
        'Examples:\n'
        '  {bin} {cmd} 00000000-1111-0000-0000-000000000001 '
            '"My first backup" /etc/\n'
        '  {bin} {cmd} 00000000-1111-0000-0000-000000000001 '
            '"nightly data" /home/johndoe -- /etc /var')
def select_files(arguments):
    """
    Generate digest for the username.
    """
    if len(arguments) < 3:
        cli_error('The host UUID, the backup name, and at least one directory '
                      'with the files should be passed!')
    else:
        my_uuid, ds_name = (try_parse_uuid(arguments.popleft()),
                            arguments.popleft())

        proceed_with_host_uuid_cli(my_uuid)

        paths = __get_all_remaining_args(arguments)

        if not paths:
            cli_error('No paths passed!')
        else:
            host_app = UHostApp(my_uuid,
                                uhost_settings.detect_edition(),
                                __create_chunk_storage())
            ugroup_uuid = host_app.host.user.base_group.uuid
            path_map = {k: {'f+': ['all'], 'f-': [], 'stat': safe_stat(k)}
                            for k in paths}

            host_app.select_paths_for_backup(ds_name=ds_name,
                                             ds_uuid=gen_uuid(),
                                             ugroup_uuid=ugroup_uuid,
                                             sync=False,
                                             paths_map=path_map)
            # Since syncing is implemented, we don't set the selected paths
            # as the default directories to be synced.
            #host_app.store_currently_selected_paths(path_map)


_SAS_OPTIONS = ('-sas', '--stay-alive-on-success')
_SAF_OPTIONS = ('-saf', '--stay-alive-on-failure')


@cli_command(
    ('-b', '--backup'),
    'Assuming some directories are preselected already '
        'using -f/--select-files switches,\n'
        'launch the backup process for them.\n'
        'Arg. 1: base host UUID.\n'
        "Arg. 2 (opt): '-sas' or '--stay-alive-on-success' - \n"
        '              to stay alive after the backup '
            'is finished successfully.\n'
        "Arg. 3 (opt): '-saf' or '--stay-alive-on-failure' - \n"
        '              to stay alive after the backup is failed.\n'
        'Examples:\n'
        '  {bin} {cmd} 00000000-1111-0000-0000-000000000001\n'
        '  {bin} {cmd} 00000000-1111-0000-0000-000000000001 -sas\n'
        '  {bin} {cmd} 00000000-1111-0000-0000-000000000001 -saf\n'
        '  {bin} {cmd} 00000000-1111-0000-0000-000000000001 -sas -saf')
def start_backup(arguments):
    """Start backup process.

    @requires: There is at least one incomplete dataset in the DB.
    """
    if len(arguments) < 1:
        cli_host_uuid_error()
    else:
        my_uuid = try_parse_uuid(arguments.popleft())

        if arguments and arguments[0] in _SAS_OPTIONS:
            arguments.popleft()
            stay_alive_on_success = True
        else:
            stay_alive_on_success = False

        if arguments and arguments[0] in _SAF_OPTIONS:
            arguments.popleft()
            stay_alive_on_failure = True
        else:
            stay_alive_on_failure = False

        proceed_with_host_uuid_cli(my_uuid)


        @exceptions_logged(logger)
        @contract_epydoc
        def on_reactor_start(app):
            """
            @type app: UHostApp
            """

            @exceptions_logged(logger)
            @contract_epydoc
            def on_backup_completed(backup_succeeded):
                """
                @param backup_succeeded: whether the backup attempt
                    has succeeded in overall.
                @type backup_succeeded: bool
                """
                if backup_succeeded:
                    print('Backup completed successfully!')
                else:
                    print('The node disallowed the backup.')

                if (stay_alive_on_success if backup_succeeded
                                          else stay_alive_on_failure):
                    print("Stayin' alive. Stayin' alive.")
                else:
                    app.terminate_host()


            with db.RDB() as rdbw:
                all_datasets = Queries.Datasets.get_just_datasets(my_uuid,
                                                                  rdbw)

            incomplete_datasets_exist, incomplete_datasets = \
                inonempty(ds for ds in all_datasets if not ds.completed)

            if not incomplete_datasets_exist:
                # No incomplete datasets to backup
                on_backup_completed(False)
            else:
                # Start the backup of the first dataset in the sequence.
                incomplete_dataset_to_start = incomplete_datasets.next()
                app.auto_start_backup = False
                app.start_backup(incomplete_dataset_to_start.uuid,
                                 on_backup_completed)


        # Launch the main host app
        host_app = UHostApp(my_uuid,
                            uhost_settings.detect_edition(),
                            __create_chunk_storage(),
                            on_reactor_start=on_reactor_start)
        host_app.first_start()

        # Launch reactor
        host_app.start_reactor()


@cli_command(
    ('-r', '--restore'),
    'Start the restore process.\n'
        'Arg. 1: base host UUID.\n'
        "Arg. 2 (opt): '-sas' or '--stay-alive-on-success' - to stay alive\n"
        '              after the restore has finished successfully.\n'
        "Arg. 3 (opt): '-saf' or '--stay-alive-on-failure' - to stay alive\n"
        '              after the restore has failed.\n'
        'Arg. 4: target directory.\n'
        'Arg. 5: backup dataset UUID.\n'
        'Arg. 6 and further: paths to restore.\n'
        'Example:\n'
        '  {bin} {cmd} 00000000-1111-0000-0000-000000000001 ./restored \\\n'
        '    f11707a4-41a6-11df-bc75-001d728e61cb \\\n'
        '    /home/myself/test-backup-1/file1 \\\n'
        '    /home/myself/test-backup-1/dir1/file2 \\\n'
        '    /home/myself/test-backup-1/dir1/dir2/file3')
def start_restore(arguments):
    """
    Start restore process.
    """
    if len(arguments) < 4:
        cli_error("At least the host UUID, the target directory, "
                      "the dataset UUID\n"
                  "and at least one file full path from the dataset "
                      "should be passed!")
    else:
        my_uuid = try_parse_uuid(arguments.popleft())

        if arguments and arguments[0] in _SAS_OPTIONS:
            arguments.popleft()
            stay_alive_on_success = True
        else:
            stay_alive_on_success = False

        if arguments and arguments[0] in _SAF_OPTIONS:
            arguments.popleft()
            stay_alive_on_failure = True
        else:
            stay_alive_on_failure = False

        target_dir, ds_uuid = (arguments.popleft(),
                               try_parse_uuid(arguments.popleft()))

        file_paths_to_restore = map(normpath_nodot,
                                    __get_all_remaining_args(arguments))

        if not file_paths_to_restore:
            cli_error('No files are given!')

        proceed_with_host_uuid_cli(my_uuid)


        @exceptions_logged(logger)
        @contract_epydoc
        def on_reactor_start(app):
            """
            @type app: UHostApp
            """

            @exceptions_logged(logger)
            @contract_epydoc
            def on_restore_completed(restore_succeeded):
                """
                @param restore_succeeded: whether the restore attempt
                    has succeeded in overall.
                @type restore_succeeded: bool
                """
                if restore_succeeded:
                    print('Restore completed successfully!')
                else:
                    print('The node disallowed the restore.')

                if (stay_alive_on_success if restore_succeeded
                                          else stay_alive_on_failure):
                    print("Stayin' alive. Stayin' alive.")
                else:
                    app.terminate_host()


            app.start_restore(file_paths_to_restore=file_paths_to_restore,
                              ds_uuid=ds_uuid,
                              restore_directory=target_dir,
                              on_completed=on_restore_completed)


        # Launch the main host app
        host_app = UHostApp(my_uuid,
                            uhost_settings.detect_edition(),
                            __create_chunk_storage(),
                            on_reactor_start=on_reactor_start,
                            do_auto_start_backup=False)
        host_app.first_start()

        # Launch reactor
        host_app.start_reactor()


_LOG_ACTIONS = set(('delete', 'send_and_delete'))


@cli_command(
    ('-la', '--logs-action'),
    'Perform some action on the logs stored for an account.\n'
        'Arg. 1: base host UUID.\n'
        'Arg. 2: action (currently supported actions: '
            + ', '.join(imap('"{}"'.format, _LOG_ACTIONS))
            + ').\n'
        'Example:\n'
        '  {bin} {cmd} 00000000-1111-0000-0000-000000000001 delete')
def logs_action(arguments):
    """
    Perform some action on the log files.
    """
    global _LOG_ACTIONS

    if len(arguments) < 2:
        cli_error('At least the host UUID and the action should be passed!')
    else:
        my_uuid, action = (try_parse_uuid(arguments.popleft()),
                           arguments.popleft())

        if action not in _LOG_ACTIONS:
            cli_error('Action %r unsupported, only the following actions '
                          'are supported: %s',
                      action,
                      ', '.join(imap('{!r}'.format,
                                     _LOG_ACTIONS)))

        else:
            proceed_with_host_uuid_cli(my_uuid)


            @exceptions_logged(logger)
            @contract_epydoc
            def on_reactor_start(app):
                """
                @type app: UHostApp
                """

                @exceptions_logged(logger)
                def on_action_completed(result):
                    app.terminate_host()


                send_settings = uhost_settings.get_log_reporting_settings()

                app.action_with_error_logs(action=action,
                                           report_settings=send_settings,
                                           on_completed=on_action_completed)


            # Launch the main host app
            host_app = UHostApp(my_uuid,
                                uhost_settings.detect_edition(),
                                __create_chunk_storage(),
                                on_reactor_start=on_reactor_start)
            host_app.first_start()

            # Launch reactor
            host_app.start_reactor()


@cli_command(
    ('-p', '--password'),
    'Use the passed password for directory encryption/decryption.\n'
        'This command is not used by itself, but together with\n'
        'some other backup or restore commands\n'
        'Example:\n'
        '  {bin} {cmd} \"god\" -r ...')
def use_password(arguments):
    """
    Use the password given in the CLI.
    """
    if len(arguments) < 1:
        cli_error('At least the password should be passed!')
    else:
        global PASSWORD
        PASSWORD = arguments.popleft()



if __debug__:
    @cli_command(
        ('-tm', '--test-make-backup-directory'),
        'DEBUG: Create a backup directory.\n'
            'Arg. 1: data variant.\n'
            'Arg. 2: directory path.\n'
            'Supported data variants:\n'
            '  0: 25 files, 80 715 274 bytes\n'
            '  1: 315 files (7 doubles), 2 144 038 bytes\n'
            '  2: 1 file, 957 970 516 bytes\n'
            '  3: 595341 files, ~1.4Gb\n'
            'Example:\n'
            '  {bin} {cmd} 0 ./test-backup')
    def _test_make_backup_dir(arguments):
        """
        Create a directory filled with dummy data files.
        """
        if len(arguments) < 2:
            cli_error('Must pass the data variant and the directory path '
                          'for the test data!')
        data_variant, backup_dir = (int(arguments.popleft()),
                                    arguments.popleft())
        print(u'Making backup data in {:s} with variant {:d}'
                  .format(backup_dir, data_variant))

        # Create fake data
        from common.test.utils import _test_create_permdir
        _test_create_permdir(backup_dir, data_variant)


    @cli_command(
        ('-tts', '--test-take-snapshot'),
        'DEBUG: Take a directory snapshot.\n'
            'Arg. 1: base host UUID.\n'
            'Arg. 2: directory path.\n'
            'Example:\n'
            '  {bin} {cmd} 00000000-1111-0000-0000-000000000001 '
                '/home/myself/test-backup-1/file1')
    def _test_take_snapshot(arguments):
        if len(arguments) < 2:
            cli_error('Must pass the base host UUID and the directory path '
                          'to take snapshot!')
        my_uuid = try_parse_uuid(arguments.popleft())
        dir_path = arguments.popleft()
        print(u'Taking snapshot of {!r}'.format(dir_path))

        proceed_with_host_uuid_cli(my_uuid)

        with db.RDB() as rdbw:
            host = Queries.Inhabitants.get_host_with_user_by_uuid(uuid, rdbw)

        UHostApp.take_base_directory_snapshot(dir_path,
                                              host.user.base_group.uuid)
