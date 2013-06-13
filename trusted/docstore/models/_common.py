#!/usr/bin/python
"""Common stuff for other document models."""

#
# Imports
#

from __future__ import absolute_import
import collections as col
import logging




#
# Variables/constants
#

logger = logging.getLogger(__name__)



#
# Classes
#

class IndexOptions(object):
    """
    A namespace with the enumeration of the options used during
    the index creation.
    """
    __slots__ = ()

    SPARSE = 1
    UNIQUE = 2



class BSONValidatableMixin(object):
    """
    A mixin for multiple inheritance, that may validate the BSON document
    schema.

    @cvar bson_schema: the schema of the document. Must be defined
        in every subclass. Contains a mapping from the name of the field
        to either the class/type of the field, or the tuple of class/types,
        or to the callable which validates the structure deeper.
        In case of callable, the validator must be a function with a single
        argument, accepting the actual value.
    """

    bson_schema = {}


    @classmethod
    def validate_schema(cls, doc):
        """Validate the document schema.

        @param doc: the document to be validated.
        @type doc: col.Mapping

        @returns: whether the document matches the schema.
        @rtype: bool
        """
        valid = True  # overall valid status

        for fname, validator in cls.bson_schema.iteritems():
            ivalid = True  # per-iteration valid status

            value = doc.get(fname)

            # First try to use it for type checking
            try:
                ivalid = isinstance(value, validator)
            except:
                # This should be a callable then.
                if isinstance(validator, col.Callable):
                    ivalid = validator(value)
                else:
                    # What is it then?
                    raise TypeError('Unsupported validator {!r} in {!r}'
                                        .format(validator, cls.bson_schema))
            else:
                # We've used the validator for type checking,
                # and it worked.
                # ivalid now contains the result of the check.
                pass

            # Track all the validation failures
            if not ivalid:
                valid = False
                logger.error('%r fails in %r', fname, doc)

        return valid
