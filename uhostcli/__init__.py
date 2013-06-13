#!/usr/bin/python
"""Command Line Interface for the Host."""
# The code is almost a functional clone of node/__init__.py

#
# Imports
#

from __future__ import absolute_import
import sys

from common.cli import cli_mode, cli_error
from common.system import install_reactor



#
# Functions
#

def main():
    """
    Startup function.
    """
    if len(sys.argv) == 1:
        cli_error('No arguments passed!')

    install_reactor(force_reactor_name='qt4')

    from host import settings as host_settings
    from . import cli

    # See host.cli for actually executed CLI commands.
    cli_mode(host_settings.HOST_VERSION_STRING)
