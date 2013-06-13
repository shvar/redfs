#!/usr/bin/python
"""Command Line Interface for the Node."""

#
# Imports
#

from __future__ import absolute_import
import sys

from common.cli import cli_mode
from common.system import install_reactor



#
# Functions
#

def main():
    """CLI startup function."""
    if len(sys.argv) == 1:
        print('No arguments passed, assuming --launch')
        sys.argv.append('--launch')

    # Setup reactor, depending upon the operating system:
    # do this before the reactor is imported!
    install_reactor(threadpool_size=25)

    from . import cli  # so that the command lines are initialized
    from node import settings
    #settings.configure_logging(postfix='early')

    # See node.cli for actually executed CLI commands.
    cli_mode(settings.NODE_VERSION_STRING)
