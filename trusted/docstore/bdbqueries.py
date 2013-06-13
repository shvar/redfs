#!/usr/bin/python
"""Queries to the BigDB.

All the implementation-specific logic regarding the BigDB must be
incapsulated here.
@todo: check indices and create shard keys for UploadedFiles.
"""

#
# Imports
#

from __future__ import absolute_import
import collections as col
import logging
import numbers
from datetime import datetime
from itertools import imap
from functools import partial
from operator import itemgetter
from types import NoneType
from uuid import UUID

import bson
import gridfs
from gridfs.errors import FileExists
import pymongo

from contrib.dbc import contract_epydoc

from common.abstractions import AbstractInhabitant, AbstractMessage
from common.chunks import AbstractChunkWithContents, ChunkInfo
from common.inhabitants import HostAtNode, Node
from common.typed_uuids import HostUUID, PeerUUID, MessageUUID
from common.utils import in_main_thread, suggest_file_copy_name

from trusted.ibsonable import IBSONable
from trusted.docstore import abstract_docstorewrapper, models
from trusted.docstore.models._common import \
    BSONValidatableMixin, IndexOptions



#
# Constants
#

logger = logging.getLogger(__name__)



#
# Classes
#

class MessageFromBigDB(BSONValidatableMixin, IBSONable, AbstractMessage):
    """
    An implementation of C{AbstractMessage}, with all its data taken
    from the BigDB.

    @ivar name: overrides the C{AbstractMessage.name} class variable.
    @type name: basestring
    """
    __slots__ = ('name', '__body')

    bson_schema = {
        'name': basestring,
        'ack': bool,
        'active': bool,
        'uuid': UUID,
        'src': UUID,
        'dst': UUID,
    }


    @contract_epydoc
    def __init__(self, name, body, *args, **kwargs):
        """Constructor.

        @param name: message name, such as 'CHUNKS'.
        @type name: basestring

        @param body: the body of the message
        @type body: str
        """
        super(MessageFromBigDB, self).__init__(*args, **kwargs)
        self.name = name
        self.__body = body


    def __str__(self):
        return u'name={self.name!r}, {super} ({length:d} bytes)'.format(
                   self=self,
                   super=super(MessageFromBigDB, self).__str__(),
                   length=len(self.__body))


    def get_body(self):
        """Overriding C{AbstractMessage.get_body()}."""
        # It must return an iterable over large chunks, so let's just make
        # one huge chunk with the whole body.
        return [self.__body]


    def to_bson(self):
        doc = super(MessageFromBigDB, self).to_bson()

        doc.update({
            'src': self.src,
            'dst': self.dst,
            'ack': self.is_ack,
            'uuid': self.uuid,
            'uploadDate': self.start_time,
            'name': self.name,
        })
        if self.status_code != 0:
            doc['status_code'] = self.status_code

        return doc


    @classmethod
    @contract_epydoc
    def from_bson(cls, doc, my_node, body):
        """
        @param my_node: the current Node.
        @type my_node: Node

        @param body: message body
        @type body: basestring
        """
        assert cls.validate_schema(doc), repr(doc)

        # We need to get the proper object (either C{Host} or C{Node}) for
        # the peer by its UUID, to pass to C{MessageFromBigDB} constructor.
        # For now let's assume that if any peer (src or dst)
        # has the same UUID as our Node, the peer is the C{Node};
        # otherwise, it's a C{Host}.
        peer_from_uuid = \
            lambda u: my_node if u == my_node.uuid \
                              else HostAtNode(uuid=HostUUID.safe_cast_uuid(u))


        return partial(super(MessageFromBigDB, cls).from_bson(doc),
                       # AbstractMessage
                       src=peer_from_uuid(doc['src']),
                       dst=peer_from_uuid(doc['dst']),
                       is_ack=doc['ack'],
                       direct=doc['src'] == my_node.uuid or
                              doc['dst'] == my_node.uuid,
                       uuid=MessageUUID.safe_cast_uuid(doc['uuid']),
                       status_code=doc.get('status_code', 0),
                       start_time=doc['uploadDate'],
                       # MessageFromBigDB
                       name=doc['name'],
                       body=body)



class BDBQueries(object):
    """The namespace for all the BigDB queries."""

    __slots__ = ()


    class Chunks(object):
        """
        The namespace for all the BigDB queries related to
        the "chunks" collection.
        """

        __slots__ = ()

        collection_name = 'chunks'

        indices = {
            # so only 1 version can be stored
            ('uuid',): [IndexOptions.UNIQUE],
            # for deduplication;
            # actually, only hash and size are enough to be unique, but crc32
            # will be unique as well.
            ('hash', 'length', 'crc32'): [IndexOptions.UNIQUE],
        }


        class NoChunkException(Exception):
            pass


        @classmethod
        @contract_epydoc
        def write_chunk(cls, chunk, bdbw):
            """Write the new chunk.

            @param chunk: the chunk with its contents available.
            @type chunk: AbstractChunkWithContents

            @param bdbw: BigDB wrapper.
            @type bdbw: abstract_docstorewrapper.AbstractDocStoreWrapper
            """
            try:
                bdbw.retried_sync(bdbw.gfs_chunks.put,
                                  chunk.get_bytes(),
                                  hash=bson.binary.Binary(chunk.hash),
                                  uuid=chunk.uuid,
                                  crc32=chunk.crc32)
            except gridfs.errors.FileExists:
                # This is more likely a error than a warning:
                # the proper deduplication should not allow
                # to write this chunk at all.
                logger.warning('Chunk %r (hash %r, uuid %s, crc32 %r) '
                                   'exists already',
                               chunk, chunk.hash, chunk.uuid, chunk.crc32)


        @classmethod
        @contract_epydoc
        def get_chunk(cls, chunk_uuid, bdbw):
            """Get the chunk by its UUID.

            @type chunk_uuid: UUID

            @param bdbw: BigDB wrapper.
            @type bdbw: abstract_docstorewrapper.AbstractDocStoreWrapper

            @rtype: gridfs.grid_file.GridOut
            @raise BDBQueries.Chunks.NoChunkException:
                if the file is not found in BigDB.
            """
            try:
                return bdbw.retried_sync(bdbw.gfs_chunks.get_last_version,
                                         uuid=chunk_uuid)
            except gridfs.errors.NoFile as e:
                raise cls.NoChunkException(
                          'Chunk {} cannot be found in storage!'.format(
                              chunk_uuid))


        @classmethod
        @contract_epydoc
        def delete_chunks(cls, chunk_uuids, bdbw):
            """Delete several chunks by their UUIDs.

            @param chunk_uuids: the (possibly non-reiterable) iterable
                over the UUIDs of the chunks which should be deleted.
            @type chunk_uuids: col.Iterable

            @param bdbw: BigDB wrapper.
            @type bdbw: abstract_docstorewrapper.AbstractDocStoreWrapper

            @todo: can we delete multiple UUIDs faster?
            """
            for uuid in chunk_uuids:
                try:
                    gridout = bdbw.retried_sync(
                                  bdbw.gfs_chunks.get_last_version,
                                  uuid=uuid)
                except gridfs.errors.NoFile:
                    logger.warning('Trying to delete chunk %r from BigDB, '
                                       "but it's already deleted",
                                   uuid)
                else:
                    logger.verbose('Deleting chunk %r/%r', uuid, gridout._id)
                    bdbw.retried_sync(bdbw.gfs_chunks.delete,
                                      gridout._id)


        @classmethod
        @contract_epydoc
        def get_all_stored_chunk_uuids(cls, bdbw):
            """Get UUIDs of all chunks stored on BigDB.

            @returns: the (non-reiterable) iterable over the UUIDs
                (C{UUID} objects) of the chunks stored on BigDB.
            @rtype: col.Iterable

            @param bdbw: BigDB wrapper.
            @type bdbw: abstract_docstorewrapper.AbstractDocStoreWrapper
            """
            # Note: this is a low-level hack right into the BigDB collections.
            return imap(itemgetter('uuid'),
                        bdbw.retried_sync(bdbw.chunks.files.find,
                                          fields=['uuid']))



    class Messages(object):
        """
        The namespace for all the BigDB queries related to
        the "messages" collection.
        """

        __slots__ = ()

        collection_name = 'messages'

        indices = {
            ('uuid',): [],  # find by transaction
            ('dst', 'uploadDate'): [],  # find by host
        }


        @classmethod
        @contract_epydoc
        def add_message(cls, msg, bdbw):
            """Add a new outgoing message.

            @param msg: an outgoing message to put in the message pool.
            @type msg: AbstractMessage

            @param bdbw: BigDB wrapper.
            @type bdbw: abstract_docstorewrapper.AbstractDocStoreWrapper
            """
            assert not in_main_thread()  # get_body() may take long

            # Mandatory
            args = {
                'name': msg.name,
                'ack': msg.is_ack,
                'active': True,
                'uuid': msg.uuid,
                'src': msg.src.uuid,
                'dst': msg.dst.uuid,
            }

            # Optional
            if msg.status_code != 0:
                args['status_code'] = msg.status_code

            bdbw.retried_sync(bdbw.gfs_messages.put,
                              ''.join(msg.get_body()),
                              **args)


        @classmethod
        @contract_epydoc
        def take_message_for_peer(cls,
                                  my_node, dst_uuid, prefer_msg_uuid=None,
                                  bdbw=None):
            """Take the message to deliver to the Host.

            Given a peer UUID C{dst_uuid}, take a next message to deliver
            to this UUID (ideally preferring that the message UUID is
            C{prefer_msg_uuid}, otherwise taking the oldest one)
            or return C{None} if no such messages exist.

            @param my_node: the object of the node where the code is working.
            @type my_node: AbstractInhabitant

            @param dst_uuid: an UUID of the destination peer to send to.
            @type dst_uuid: PeerUUID

            @param prefer_msg_uuid: (optional) prefer that the message UUID
                to be this.
            @type prefer_msg_uuid: MessageUUID, NoneType

            @param bdbw: BigDB wrapper.
            @type bdbw: abstract_docstorewrapper.AbstractDocStoreWrapper

            @returns: the message to deliver to the peer, or C{None}.
            @rtype: AbstractMessage, NoneType

            @todo: the first C{find_and_modify} request gives us enough
                information to create C{GridOut} object manually (rather than
                to perform C{.get_last_version()});
                if we create it manually, we can save one BigDB query
                per every message to be delivered.
            """
            doc = {}

            # Try to find the message by the Message UUID
            if prefer_msg_uuid is not None:
                logger.verbose('Trying to get message %r', prefer_msg_uuid)
                doc = cls.__take_doc_by_msg_uuid(msg_uuid=prefer_msg_uuid,
                                                 bdbw=bdbw)
                # What if we've got the message, but it's for different
                # host UUID? Something is seriously wrong.
                if doc is not None and doc['dst'] != dst_uuid:
                    logger.error('Found message %r for %r, but actually '
                                     'it is for %r',
                                 doc, dst_uuid, doc['dst'])
                    doc = {}

            # Otherwise try to find the (oldest) message by the peer UUID.
            if not doc:
                logger.verbose('Trying to get message for %r', dst_uuid)
                doc = cls.__take_doc_by_dst_uuid(dst_uuid=dst_uuid,
                                                 bdbw=bdbw)

            if doc is None:
                return None  # no such message

            else:
                logger.verbose('For %r, taken message: %r', dst_uuid, doc)

                # Find the file on the BigDB
                try:
                    # _id would be sufficient to access the file;
                    # but we also provide (dst, uploadDate) to the query
                    # for the request to be sharded.
                    gridout = \
                        bdbw.retried_sync(bdbw.gfs_messages.get_last_version,
                                          _id=doc['_id'],
                                          dst=doc['dst'],
                                          uploadDate=doc['uploadDate'])

                except gridfs.errors.NoFile:
                    logger.warning('Cannot access file %r/%r on BigDB',
                                   doc['_id'])

                    # Actually, as this is a weird error, we could've
                    # just retried this operation
                    # and do C{cls.take_message_for_peer(...)} here;
                    # but for the sake of clarity and to avoid the possible
                    # infinite recursion, let's just assume
                    # that the message is absent.
                    return None

                else:
                    assert isinstance(gridout, gridfs.grid_file.GridOut), \
                           repr(gridout)
                    body = gridout.read()

                    # Finally, delete it from BigDB
                    bdbw.retried_sync(bdbw.gfs_messages.delete,
                                      gridout._id)

                    return MessageFromBigDB.from_bson(doc,
                                                      my_node=my_node,
                                                      body=body)()


        @classmethod
        @contract_epydoc
        def _spec_for_get_messages_count(cls,
                                         min_ts=None, max_ts=None, hosts=None):
            r"""Generate a MongoDB spec for C{.get_messages_count()} query.

            >>> BDBQueries.Messages._spec_for_get_messages_count(
            ... )
            {'active': True}

            >>> BDBQueries.Messages._spec_for_get_messages_count(
            ...     min_ts=datetime(2012, 9, 28, 17, 20, 10, 449559)
            ... )  # doctest:+NORMALIZE_WHITESPACE
            {'active': True,
             'uploadDate': {'$gt': datetime.datetime(2012, 9, 28,
                                                     17, 20, 10, 449559)}}

            >>> BDBQueries.Messages._spec_for_get_messages_count(
            ...     max_ts=datetime(2012, 9, 28, 17, 20, 50, 856436)
            ... )  # doctest:+NORMALIZE_WHITESPACE
            {'active': True,
             'uploadDate': {'$lt': datetime.datetime(2012, 9, 28,
                                                     17, 20, 50, 856436)}}

            >>> BDBQueries.Messages._spec_for_get_messages_count(
            ...     min_ts=datetime(2012, 9, 28, 17, 20, 10, 449559),
            ...     max_ts=datetime(2012, 9, 28, 17, 20, 50, 856436)
            ... )  # doctest:+NORMALIZE_WHITESPACE
            {'active': True,
             'uploadDate': {'$gt': datetime.datetime(2012, 9, 28,
                                                     17, 20, 10, 449559),
                            '$lt': datetime.datetime(2012, 9, 28,
                                                     17, 20, 50, 856436)}}

            >>> BDBQueries.Messages._spec_for_get_messages_count(
            ...     hosts=iter([])
            ... )
            {'active': True, 'dst': {'$in': []}}

            >>> BDBQueries.Messages._spec_for_get_messages_count(
            ...     min_ts=datetime(2012, 9, 28, 17, 20, 10, 449559),
            ...     max_ts=datetime(2012, 9, 28, 17, 20, 50, 856436),
            ...     hosts=[UUID('c563d669-26cc-4e05-bcfe-2b3cf258dd9f'),
            ...            UUID('6a22c85e-3064-4dfa-a77d-ef3fc8e84c8d'),
            ...            UUID('cec483d9-a55e-47b1-84d1-214149358011')]
            ... )  # doctest:+NORMALIZE_WHITESPACE
            {'active': True,
             'dst': {'$in': [UUID('c563d669-26cc-4e05-bcfe-2b3cf258dd9f'),
                             UUID('6a22c85e-3064-4dfa-a77d-ef3fc8e84c8d'),
                             UUID('cec483d9-a55e-47b1-84d1-214149358011')]},
             'uploadDate': {'$gt': datetime.datetime(2012, 9, 28,
                                                     17, 20, 10, 449559),
                            '$lt': datetime.datetime(2012, 9, 28,
                                                     17, 20, 50, 856436)}}

            >>> BDBQueries.Messages._spec_for_get_messages_count(
            ...     max_ts=datetime(2012, 9, 28, 17, 20, 50, 856436),
            ...     hosts=[UUID('c563d669-26cc-4e05-bcfe-2b3cf258dd9f'),
            ...            UUID('6a22c85e-3064-4dfa-a77d-ef3fc8e84c8d'),
            ...            UUID('cec483d9-a55e-47b1-84d1-214149358011')]
            ... )  # doctest:+NORMALIZE_WHITESPACE
            {'active': True,
             'dst': {'$in': [UUID('c563d669-26cc-4e05-bcfe-2b3cf258dd9f'),
                             UUID('6a22c85e-3064-4dfa-a77d-ef3fc8e84c8d'),
                             UUID('cec483d9-a55e-47b1-84d1-214149358011')]},
             'uploadDate': {'$lt': datetime.datetime(2012, 9, 28,
                                                     17, 20, 50, 856436)}}

            @type min_ts: datetime, NoneType
            @type max_ts: datetime, NoneType
            @type hosts: col.Iterable, NoneType
            """
            spec = {'active': True}
            if min_ts is not None:
                spec.setdefault('uploadDate', {})['$gt'] = min_ts
            if max_ts is not None:
                spec.setdefault('uploadDate', {})['$lt'] = max_ts
            if hosts is not None:
                spec.setdefault('dst', {})['$in'] = list(hosts)

            return spec


        @classmethod
        @contract_epydoc
        def get_messages_count(cls, min_ts=None, max_ts=None, hosts=None,
                               bdbw=None):
            """
            Get the count of active outgoing messages with C{uploadDate} inside
            [min_ts, max_ts] by HostUUID.

            @type min_ts: datetime, NoneType

            @type max_ts: datetime, NoneType

            @param hosts: iterable over HostUUIDs
            @type hosts: col.Iterable, NoneType

            @param bdbw: BigDB wrapper.
            @type bdbw: abstract_docstorewrapper.AbstractDocStoreWrapper

            @rtype: numbers.Integral
            """
            spec = cls._spec_for_get_messages_count(min_ts, max_ts, hosts)

            return bdbw.retried_sync(
                       lambda spec: bdbw.messages.files.find(spec).count(),
                       spec=spec)


        @classmethod
        @contract_epydoc
        def _spec_for_get_message_uuids(cls, class_name=None, dst_uuid=None):
            r"""Generate a MongoDB spec for C{.get_message_uuids()} query.

            >>> BDBQueries.Messages._spec_for_get_message_uuids(
            ... )  # doctest:+NORMALIZE_WHITESPACE
            {}

            >>> BDBQueries.Messages._spec_for_get_message_uuids(
            ...     class_name='BACKUP'
            ... )
            {'name': 'BACKUP'}

            >>> BDBQueries.Messages._spec_for_get_message_uuids(
            ...     dst_uuid=UUID(int=0x42)
            ... )
            {'dst': UUID('00000000-0000-0000-0000-000000000042')}

            >>> BDBQueries.Messages._spec_for_get_message_uuids(
            ...     class_name='BACKUP',
            ...     dst_uuid=UUID(int=0x42)
            ... )  # doctest:+NORMALIZE_WHITESPACE
            {'dst': UUID('00000000-0000-0000-0000-000000000042'),
             'name': 'BACKUP'}

            @type class_name: basestring, NoneType
            @type dst_uuid: UUID, NoneType
            """
            spec = {}
            if class_name is not None:
                spec['name'] = class_name
            if dst_uuid is not None:
                spec['dst'] = dst_uuid
            return spec


        @classmethod
        @contract_epydoc
        def get_message_uuids(cls, class_name=None, dst_uuid=None, bdbw=None):
            """Find the UUIDs of outgoing messages by various criteria.

            @param class_name: (optional) the name of the desired
                message class.
            @type class_name: basestring, NoneType

            @param dst_uuid: (optional) the UUID of the destination peer.
            @type dst_uuid: PeerUUID, NoneType

            @return: the iterable over the the UUIDs of outgoing messages.
            @rtype: col.Iterable
            """
            spec = cls._spec_for_get_message_uuids(class_name=class_name,
                                                   dst_uuid=dst_uuid)
            logger.verbose('Getting transaction states with spec: %r', spec)

            docs = bdbw.retried_sync(bdbw.messages.files.find,
                                     spec=spec)

            return (MessageUUID.safe_cast_uuid(doc['uuid'])
                        for doc in docs)


        @classmethod
        @contract_epydoc
        def __take_doc_by_msg_uuid(cls, msg_uuid, bdbw):
            """Take the message document by its UUID, if it exists.

            @param msg_uuid: the desired message UUID
            @param msg_uuid: MessageUUID

            @param bdbw: BigDB wrapper.
            @type bdbw: abstract_docstorewrapper.AbstractDocStoreWrapper

            @returns: the document with the message to deliver to the peer,
                or C{None}.
            @rtype: col.Mapping, NoneType

            @note: this is the low-level hack right into the BigDB
                collections.
            @todo: the first C{find_and_modify} request gives us enough
                information to create C{GridOut} object manually (rather than
                to perform C{.get_last_version()});
                if we create it manually, we can save one BigDB query
                per every message to be delivered.
            """
            # Take the oldest message waiting to be delivered for dst_uuid.
            # Note: this is a low-level hack right into the BigDB collections.
            return bdbw.retried_sync(bdbw.messages.files.find_and_modify,
                                     query={'active': True,
                                            'uuid': msg_uuid},
                                     update={'$set': {'active': False}},
                                     new=False,
                                     safe=True)

        @classmethod
        @contract_epydoc
        def __take_doc_by_dst_uuid(cls, dst_uuid, bdbw):
            """Take the oldest message for peer UUID, if it exists.

            @param dst_uuid: an UUID of the destination peer to send to.
            @type dst_uuid: PeerUUID

            @param bdbw: BigDB wrapper.
            @type bdbw: abstract_docstorewrapper.AbstractDocStoreWrapper

            @returns: the document with the message to deliver to the peer,
                or C{None}.
            @rtype: col.Mapping, NoneType

            @note: this is the low-level hack right into the BigDB
                collections.

            @todo: the first C{find_and_modify} request gives us enough
                information to create C{GridOut} object manually (rather than
                to perform C{.get_last_version()});
                if we create it manually, we can save one BigDB query
                per every message to be delivered.
            """
            # Take the oldest message waiting to be delivered for dst_uuid.
            return bdbw.retried_sync(bdbw.messages.files.find_and_modify,
                                     query={'active': True,
                                            'dst': dst_uuid},
                                     update={'$set': {'active': False}},
                                     sort={'uploadDate': pymongo.ASCENDING},
                                     new=False,
                                     safe=True)


    class UploadedFiles(object):
        """
        The namespace for all the BigDB queries related to
        the "uploaded_files" collection.
        """
        __slots__ = ()

        collection_name = 'uploaded_files'

        indices = {
            ('group_uuid', 'upload_uuid', 'base_dir', 'rel_path'):
                [IndexOptions.UNIQUE],
        }

        @classmethod
        @contract_epydoc
        def add_new_file(cls, base_dir, rel_path, group_uuid, upload_uuid,
                         bdbw):
            """Add a new file being uploaded.

            @param bdbw: BigDB wrapper.
            @type bdbw: abstract_docstorewrapper.AbstractDocStoreWrapper
            """
            return bdbw.retried_sync(bdbw.gfs_uploaded_files.new_file,
                                     base_dir=base_dir,
                                     rel_path=rel_path,
                                     group_uuid=group_uuid,
                                     upload_uuid=upload_uuid,
                                     upload_started=datetime.utcnow())


        @classmethod
        @contract_epydoc
        def _get_files_for_group_uuid_upload_uuid(cls, group_uuid, upload_uuid,
                                                  bdbw):
            """Only find files with this group and upload uuids."""
            return bdbw.retried_sync(bdbw.uploaded_files.files.find,
                                     spec={'group_uuid': group_uuid,
                                           'upload_uuid': upload_uuid})


        @classmethod
        @contract_epydoc
        def get_files_for_group_uuid_upload_uuid(cls, group_uuid, upload_uuid,
                                                 bdbw):
            """Return files with specified upload_uuid for this group_uuid.

            @param bdbw: BigDB wrapper.
            @type bdbw: abstract_docstorewrapper.AbstractDocStoreWrapper
            """
            files = cls._get_files_for_group_uuid_upload_uuid(
                        group_uuid, upload_uuid, bdbw)
            return (bdbw.retried_sync(bdbw.gfs_uploaded_files.get_last_version,
                                      rel_path=ufile['rel_path'],
                                      base_dir=ufile['base_dir'],
                                      group_uuid=ufile['group_uuid'],
                                      upload_uuid=ufile['upload_uuid'])
                        for ufile in files)


        @classmethod
        @contract_epydoc
        def purge_backed_up_files(cls, group_uuid, upload_uuid, bdbw):
            """Purge files that were just uploaded to datasets.

            @param bdbw: BigDB wrapper.
            @type bdbw: abstract_docstorewrapper.AbstractDocStoreWrapper
            """
            files = cls._get_files_for_group_uuid_upload_uuid(
                        group_uuid, upload_uuid, bdbw)
            for ufile in files:
                bdbw.retried_sync(bdbw.gfs_uploaded_files.delete,
                                  file_id=ufile['_id'])




GFS_COLLECTIONS = [
    BDBQueries.Chunks,
    BDBQueries.Messages,
    BDBQueries.UploadedFiles,
]

GFS_INDICES_PER_COLLECTION = \
    {'{}.files'.format(c.collection_name): c.indices
         for c in GFS_COLLECTIONS}
