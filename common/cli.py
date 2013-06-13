#!/usr/bin/python
"""
Universal command-line interface code.

@var COMMANDS: Contains structure like {'--help': print_help,},
               binding the command line arguments to the function handlers.
@type COMMANDS: dict
"""

#
# Imports
#

from __future__ import absolute_import
import locale
import logging
import sys
from collections import deque
from functools import wraps
from os.path import basename
from uuid import UUID

from contrib.dbc import contract_epydoc

from .utils import exceptions_logged



#
# Logging
#

logger = logging.getLogger(__name__)



#
# Global variables
#

COMMANDS = {}



#
# Functions
#

@contract_epydoc
def cli_command(synonyms, help):
    """
    The function decorator that adds the function to the structure
    of supported command line arguments/commands.

    @note: Every CLI command automatically gets its exceptions logged.

    @param synonyms: The tuple/list of synonyms for this command.
    @type synonyms: tuple, list
    @param help: Help string for a wrapped/decorated function.
    @type help: basestring

    @returns: The function wrapper that binds the passed help string
              to the function as _help field.
    """

    def wrapper(f):

        @wraps(f)
        @exceptions_logged(logger)
        def f_new(*args, **kwargs):
            return f(*args, **kwargs)

        f_new._help = help
        f_new._synonyms = synonyms

        global COMMANDS
        for key in synonyms:
            COMMANDS[key] = f_new

        return f_new

    return wrapper


@cli_command(('-h', '--help'),
             'Prints help message')
def print_help(arguments):
    # Generate help upon the COMMANDS structure.
    # Join the values which share the keys
    global COMMANDS

    # First, calculate the commands_dictionary where the keys are joined.
    keys_to_help = {cmd._synonyms: cmd._help
                        for cmd in COMMANDS.itervalues()}

    max_width = max(len(', '.join(keys))
                        for keys
                        in keys_to_help.iterkeys())

    # Now, print all the keys and values.
    print('Supported commands:\n')
    left_offset = ' ' * (max_width + 5)
    for key_list, value in sorted(keys_to_help.iteritems(),
                                  cmp=lambda x, y: cmp(x[0], y[0])):
        # Value should be altered: a "bin" variable should be bound to it
        value_real = value.format(bin=basename(sys.argv[0]),
                                  cmd=key_list[0])
        # Now, print the keys and the description string
        _key_str = ', '.join(key_list).ljust(max_width)
        _descr = ('\n' + left_offset).join(value_real.split('\n'))
        print('{}  -  {}\n'.format(_key_str,
                                   # The second line should get the additional
                                   # left offset if multiline
                                   _descr))


def cli_error(error, *args, **kwargs):
    """
    The arguments, if passed, are applied to the error text.
    """
    print('Error: {}\n'
          '       Issue --help argument if you need to check '
              'the invocation syntax.'
              .format(error % args))
    sys.exit(-1)


def cli_mode(version_string):
    """
    Process the command line arguments.

    @param version_string: The string describing the instance
                           cli is running on.
    @type version_string: str
    """
    arguments = deque(i.decode(locale.getpreferredencoding())
                          for i in sys.argv[1:])
    while arguments:
        command = arguments.popleft()
        if command in COMMANDS:
            function = COMMANDS[command]

            # Special case for "--help"/"-h" command
            #if function == print_help:
                #arguments.appendleft(version_string)
            function(arguments)
            if arguments:
                logger.debug('Remaining arguments %s', arguments)
        else:
            cli_error(u'Unsupported command {}!'.format(command))


def try_parse_uuid(uuid_str):
    """
    Parse UUID string for the purpose of CLI, and drop a cli_error
    if it is unparseable.

    @returns: the parsed UUID object, or None.
    @rtype: UUID, NoneType
    """
    try:
        return UUID(uuid_str)
    except ValueError:
        cli_error('%r is not a valid UUID string.', uuid_str)
        return None
