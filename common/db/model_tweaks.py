#!/usr/bin/python
"""
This file allows to tweak some model generation settings depending on
whether the Host or the Node is importing them.
"""

#
# Variables
#

_FILE_UUID_IS_NULLABLE = None
"""
Everybody who imports this file must set C{_FILE_UUID_IS_NULLABLE} variable
to either C{True} or C{False} to define whether the C{file.uuid} field
can accepts NULL values or not.

For various reasons, it is assumed that on the Node, C{file.uuid} is not
nullable, while on the host, C{file.uuid} is nullable.
"""

_HOST_USER_ID_IS_NULLABLE = None
"""
Everybody who imports this file must set C{_HOST_USER_ID_IS_NULLABLE}
variable to either C{True} or C{False} to define whether the C{host.user_id}
field can accepts NULL values or not.

For various reasons, it is assumed that on the Node, C{host.user_id} is not
nullable (any host always belongs to some user), while on the Host,
C{host.user_id} is nullable (a host may receive the information about other
hosts, but not their user membership).
"""


class LateBooleanBinding(object):
    """
    When creating the models using SQLAlchemy declarative extension, we need to
    pass some boolean setting to a field, but it should be evaluated only
    later, when the database schema is created for the model.

    >>> bool(LateBooleanBinding(lambda: True))
    True

    >>> bool(LateBooleanBinding(lambda: False))
    False
    """

    def __init__(self, value_getter):
        self.__value_getter = value_getter

    def __nonzero__(self):
        value = self.__value_getter()
        assert value is not None
        return value
