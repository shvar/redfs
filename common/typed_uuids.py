#!/usr/bin/python
"""
Typed versions of regular UUID class.
"""

#
# Imports
#

from __future__ import absolute_import
import sys
from uuid import UUID

from common.utils import gen_uuid



#
# Classes
#

class TypedUUID(UUID):
    """
    Typed version of regular UUID class, capable of type-preserving
    conversions.
    """

    def __repr__(self):
        return "{}('{}')".format(self.__class__.__name__, self)


    @classmethod
    def _from_uuid(cls, uuid):
        """
        Create a new instance of this variant of typed UUID from a given UUID.
        @note: do not abuse this method! Likely you need to use
               C{.safe_case_uuid()} instead.
        """
        assert isinstance(uuid, UUID), repr(uuid)
        return cls(bytes=uuid.bytes)


    @classmethod
    def safe_cast_uuid(cls, uuid):
        """
        Cast an C{uuid} to the destination class.
        The casting allows:
          1. to cast from untyped UUID to any typed one,
          2. to cast some typed UUID to a more specific subtype,
          3. to cast some typed UUID to a more general super-type
             (though makes little sense and the re-casting is not performed),
        but doesn't allow to cast a typed UUID to a different class.
        """
        assert isinstance(uuid, UUID), repr(uuid)

        other_class = uuid.__class__

        if other_class == UUID:
            # 1. from untyped UUID to a typed one.
            # Eg: from an UUID to a PeerUUID
            return cls._from_uuid(uuid)
        elif issubclass(cls, other_class):
            # 2. to a more specific subtype
            # Eg: from a PeerUUID to HostUUID (result: HostUUID)
            return cls._from_uuid(uuid)
        elif issubclass(other_class, cls):
            # 3. to a more general supertype
            # Eg: from a HostUUID to PeerUUID (result: HostUUID,
            #     cause it is a PeerUUID too)
            return uuid
        else:
            raise TypeError(u'{!r} is a {} while should be a {}!'
                                .format(uuid,
                                        other_class.__name__,
                                        cls.__name__))


class DatasetUUID(TypedUUID):
    """The UUID of a dataset."""
    pass


class MessageUUID(TypedUUID):
    """The UUID of a message (or a transaction, what is equal)."""
    pass


TransactionUUID = MessageUUID


class PeerUUID(TypedUUID):
    """The UUID of any peer in the system."""
    pass


class HostUUID(PeerUUID):
    """The UUID of any Host."""
    pass


class NodeUUID(PeerUUID):
    """The UUID of any Node."""
    pass


class UserGroupUUID(TypedUUID):
    """The UUID of any user group."""
    pass


class FileUUID(TypedUUID):
    """The UUID of any file with the unique contents."""
    pass


class ChunkUUID(TypedUUID):
    """The UUID of any chunk."""
    pass


ALL_TYPED_UUIDS = [
    TypedUUID,
    DatasetUUID,
    MessageUUID,
    PeerUUID,
    HostUUID,
    NodeUUID,
    UserGroupUUID,
    FileUUID,
    ChunkUUID,
]

if __debug__:
    if 'nose' in sys.modules:
        import unittest

        class TestBaseUUID(TypedUUID):
            pass

        class TestChildA(TestBaseUUID):
            pass

        class TestChildAA(TestChildA):
            pass

        class TestChildB(TestBaseUUID):
            pass

        class TestChildBB(TestChildB):
            pass

        class TestTypedUUID(unittest.TestCase):
            __slots__ = 'u child_a child_aa child_b'.split()

            def setUp(self):
                self.u = gen_uuid()
                self.child_a = TestChildA(self.u.hex)
                self.child_aa = TestChildAA(self.u.hex)
                self.child_b = TestChildB(self.u.hex)

            def test_from_uuid(self):
                u = gen_uuid()
                u_as_a = TestChildA._from_uuid(u)
                u_as_aa = TestChildAA._from_uuid(u)
                u_as_b = TestChildB._from_uuid(u)
                u_as_bb = TestChildBB._from_uuid(u)

                self.assertEqual(all(i.hex == u.hex
                                         for i in (u_as_a, u_as_aa,
                                                   u_as_b, u_as_bb)),
                                 True)

            def test_safe_cast_uuid(self):
                uuid_str = "'{}'".format(self.u)

                # from untyped UUID to typed
                self.assertEqual(repr(TestChildA.safe_cast_uuid(self.u)),
                                 'TestChildA({})'.format(uuid_str))
                self.assertEqual(repr(TestChildAA.safe_cast_uuid(self.u)),
                                 'TestChildAA({})'.format(uuid_str))
                # from typed UUID to the same-typed UUID
                self.assertEqual(repr(TestChildA.safe_cast_uuid(self.child_a)),
                                 'TestChildA({})'.format(uuid_str))
                self.assertEqual(
                    repr(TestChildAA.safe_cast_uuid(self.child_aa)),
                    'TestChildAA({})'.format(uuid_str))
                # from typed UUID to the child-typed UUID - cast to child
                self.assertEqual(
                    repr(TestChildAA.safe_cast_uuid(self.child_a)),
                    'TestChildAA({})'.format(uuid_str))
                # from typed UUID to the parent-typed UUID - keep original
                self.assertEqual(repr(TestChildA.safe_cast_uuid(self.child_aa)),
                                 'TestChildAA({})'.format(uuid_str))
                # from typed UUID to a differently-typed UUID
                with self.assertRaisesRegexp(
                         TypeError,
                         'TestChildA\({}\) is a TestChildA while should be '
                             'a TestChildB!'.format(uuid_str)):
                    TestChildB.safe_cast_uuid(self.child_a)
                with self.assertRaisesRegexp(
                         TypeError,
                         'TestChildA\({}\) is a TestChildA while should be '
                             'a TestChildBB!'.format(uuid_str)):
                    TestChildBB.safe_cast_uuid(self.child_a)
                with self.assertRaisesRegexp(
                         TypeError,
                         'TestChildAA\({}\) is a TestChildAA while should be '
                             'a TestChildB!'.format(uuid_str)):
                    TestChildB.safe_cast_uuid(self.child_aa)
                with self.assertRaisesRegexp(
                         TypeError,
                         'TestChildAA\({}\) is a TestChildAA while should be '
                             'a TestChildBB!'.format(uuid_str)):
                    TestChildBB.safe_cast_uuid(self.child_aa)
