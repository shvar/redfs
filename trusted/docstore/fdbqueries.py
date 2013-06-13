#!/usr/bin/python
"""Queries to the FastDB.

All the implementation-specific logic regarding the FastDB must be
incapsulated here.
"""

#
# Imports
#

from __future__ import absolute_import
import collections as col
import logging
import numbers
from datetime import datetime, timedelta
from itertools import chain, imap
from types import NoneType
from uuid import UUID

import bson
import pymongo

from contrib.dbc import consists_of, contract_epydoc

from common import datatypes
from common.base56 import gen_magnet_code
from common.typed_uuids import (
    ChunkUUID, DatasetUUID, HostUUID, PeerUUID, TransactionUUID, UserGroupUUID)
from common.utils import coalesce, gen_uuid, NULL_UUID

from . import abstract_docstorewrapper, models
from .models._common import IndexOptions
from .models.transaction_states._common import NodeTransactionState



#
# Constants
#

logger = logging.getLogger(__name__)

AUTH_TOKENS_TO_KEEP = 10
"""How many auth tokens we are trying to keep for a user."""

AUTH_TOKEN_DECAY_PERIOD = timedelta(minutes=10)
"""How old may tokens become."""

AUTH_TOKEN_DELETE_PERIOD = timedelta(seconds=10)
"""How often can we delete tokens."""

HEAL_REQUEST_PURGE_AGE = timedelta(minutes=15)
"""
How soon after a heal request enters the 'hea' (healing) state,
it can be deleted (thus maybe re-executed).
"""



#
# Classes
#

class FDBQueries(object):
    """The namespace for all the FastDB queries."""

    __slots__ = ()



    class AuthTokens(object):
        """
        The namespace for all the FastDB queries related to
        the "restore_requests" collection.
        """

        __slots__ = ()

        collection_name = 'auth_tokens'

        indices = {
            ('host_uuid',): [IndexOptions.UNIQUE],
        }


        @classmethod
        @contract_epydoc
        def init_host_tokens(cls, host_uuid, fdbw):
            """
            For a given host (by its UUID), create enough
            authentication tokens.

            @type host_uuid: PeerUUID

            @param fdbw: FastDB wrapper.
            @type fdbw: abstract_docstorewrapper.AbstractDocStoreWrapper
            """
            _tokens = (datatypes.AuthToken.create()
                           for i in xrange(AUTH_TOKENS_TO_KEEP))
            bsoned_tokens = [models.AuthToken.from_common(t).to_bson()
                                 for t in _tokens]
            fdbw.retried_sync(
                fdbw.auth_tokens.update,
                spec={'host_uuid': host_uuid},
                document={'$set': {'host_uuid': host_uuid,
                                   'last_delete_ts': datetime.min,
                                   'last_synced_token_ts': datetime.min,
                                   'tokens': bsoned_tokens}},
                upsert=True,
                safe=True)


        @classmethod
        @contract_epydoc
        def update_last_token_sync_ts(cls,
                                      host_uuid, last_token_sync_ts, fdbw):
            """
            For the per-user authentication token data, update the timestamp
            of the last synced token to the "now" value (provided that it was
            previously older than C{last_token_sync_ts}).

            @type host_uuid: PeerUUID
            @type last_token_sync_ts: datetime

            @param fdbw: FastDB wrapper.
            @type fdbw: abstract_docstorewrapper.AbstractDocStoreWrapper
            """
            fdbw.retried_sync(
                fdbw.auth_tokens.update,
                spec={'host_uuid': host_uuid,
                      'last_synced_token_ts': {'$lt': last_token_sync_ts}},
                document={'$set': {'last_synced_token_ts': datetime.utcnow()}},
                safe=True)


        # TODO: strange that it returns an untyped tuple that has
        # almost every record from the document in auth_tokens,
        # and at the same time there is no model for such document,
        # to return it instead.
        @classmethod
        @contract_epydoc
        def _get_info_and_tokens(cls, host_uuid, fdbw):
            """
            Returns a record about the authentication tokens for
            a particular host.

            Read-only function, doesn't cause any alterations to the FastDB.

            @type host_uuid: PeerUUID

            @param fdbw: FastDB wrapper.
            @type fdbw: abstract_docstorewrapper.AbstractDocStoreWrapper

            @returns: a tuple of
                C{(tokens, last_delete_ts, last_synced_token_ts)}
                where C{tokens} is an iterable (might be empty) over
                the tokens, C{last_delete_ts} is the timestamp of last deletion
                and C{last_synced_token_ts} is the timestamp of last token
                that was successfully synced to the host.
                If there are no records about auth_tokens returns None
            @rtype: tuple, NoneType
            """
            result = fdbw.retried_sync(fdbw.auth_tokens.find_one,
                                       spec_or_id={'host_uuid': host_uuid})
            if result is None:
                return None
            else:
                tokens = [models.AuthToken.from_bson(t)()
                             for t in result['tokens']]
                return (tokens,
                        result['last_delete_ts'],
                        result['last_synced_token_ts'])


        # TODO: strange that it returns an untyped tuple that has
        # almost every record from the document in auth_tokens,
        # and at the same time there is no model for such document,
        # to return it instead.
        @classmethod
        def get_info_and_tokens(cls, host_uuid, fdbw):
            """
            Returns a record about the authentication tokens for
            a particular host.

            Maintains the desired constant amount of the tokens.

            @note: actually, it is an Read-Write function that may alter
                the FastDB on its call.

            @type host_uuid: PeerUUID

            @param fdbw: FastDB wrapper.
            @type fdbw: abstract_docstorewrapper.AbstractDocStoreWrapper

            @returns: a tuple of
                C{(tokens, last_delete_ts, last_synced_token_ts)}
                where C{tokens} is an iterable (might be empty) over
                the tokens, C{last_delete_ts} is the timestamp of last deletion
                and C{last_synced_token_ts} is the timestamp of last token
                that was successfully synced to the host.
                If there are no records about auth_tokens returns None
            @rtype: tuple, NoneType
            """
            _now = datetime.utcnow()

            # Delete a token older than this
            token_ts_bound = _now - AUTH_TOKEN_DECAY_PERIOD
            # Delete a token only if previous token was deleted older than this
            delete_ts_bound = _now - AUTH_TOKEN_DELETE_PERIOD

            # If we delete some old token, this will be the replacement.
            _token = datatypes.AuthToken.create()
            bsoned_token = models.AuthToken.from_common(_token).to_bson()

            # We attempt to delete a decayed token;
            # but, as a useful side effect, if we succeeded to delete
            # the token, we receive the tokens to output (so we don't need
            # to request them separately).
            result = fdbw.retried_sync(
                         fdbw.auth_tokens.find_and_modify,
                         query={'host_uuid': host_uuid,
                                'last_delete_ts': {'$lt': delete_ts_bound},
                                'tokens.ts': {'$lt': token_ts_bound}},
                         update={'$set': {'last_delete_ts': datetime.utcnow(),
                                          'tokens.$': bsoned_token}},
                         new=True,
                         safe=True)

            if result is None:
                # Need to do a separate query.
                return FDBQueries.AuthTokens._get_info_and_tokens(host_uuid,
                                                                  fdbw)
            else:
                # Can reuse the received data.
                return ([models.AuthToken.from_bson(t)()
                             for t in result['tokens']],
                        result['last_delete_ts'],
                        result['last_synced_token_ts'])


        @classmethod
        def validate_token(cls, host_uuid, token_uuid, fdbw):
            """
            Given a token UUID (as C{token_uuid}), validate whether
            it is acceptable for the host (as C{host_uuid}).
            If it is valid, make sure it won't be reused,
            and the amount of available auth tokens is still sufficient.

            @type host_uuid: PeerUUID
            @type token_uuid: datetime

            @param fdbw: FastDB wrapper.
            @type fdbw: abstract_docstorewrapper.AbstractDocStoreWrapper

            @returns: whether the token is valid.
            @rtype: bool
            """
            # This is the candidate for new token; though, it might be
            # not used
            _token = datatypes.AuthToken.create()
            bsoned_token = models.AuthToken.from_common(_token).to_bson()
            return fdbw.retried_sync(
                       fdbw.auth_tokens.find_and_modify,
                       query={'host_uuid': host_uuid,
                              'tokens.uuid': token_uuid},
                       update={'$set': {'tokens.$': bsoned_token,
                                        'last_delete_ts': datetime.utcnow()}},
                       fields=['_id'],
                       safe=True) is not None



    class HealChunkRequests(object):
        """
        The namespace for all the FastDB queries related to
        the "heal_chunk_requests" collection.
        """

        __slots__ = ()

        collection_name = 'heal_chunk_requests'

        indices = {
            ('chunk_uuid',): [IndexOptions.UNIQUE],
            ('state', 'last_update_ts'): [],
        }


        @classmethod
        @contract_epydoc
        def add_heal_chunk_request(cls, chunk_uuid, fdbw):
            """Add a request to heal some chunks.

            @param chunk_uuid: the UUID of the chunk to heal.
            @type chunk_uuid: ChunkUUID

            @param fdbw: FastDB wrapper.
            @type fdbw: abstract_docstorewrapper.AbstractDocStoreWrapper

            @raises Exception: if the chunk with such an UUID exists already.
            """
            req = models.HealChunkRequest(chunk_uuid=chunk_uuid,
                                          state='rth',
                                          last_update_ts=datetime.utcnow())
            fdbw.retried_sync(fdbw.heal_chunk_requests.insert,
                              doc_or_docs=req.to_bson(),
                              safe=True)


        @classmethod
        @contract_epydoc
        def atomic_start_oldest_restore_request(cls, fdbw):
            """
            Find the oldest chunk heal request not yet started,
            start it, and return the document (already marked as started).

            @param fdbw: FastDB wrapper.
            @type fdbw: abstract_docstorewrapper.AbstractDocStoreWrapper

            @rtype: models.HealChunkRequest, NoneType
            """
            doc = fdbw.retried_sync(
                      fdbw.heal_chunk_requests.find_and_modify,
                      query={'state': 'rth'},
                      sort=[('last_update_ts', pymongo.ASCENDING)],
                      update={'$set': {'state': 'hea',
                                       'last_update_ts': datetime.utcnow()}},
                      new=True)

            return models.HealChunkRequest.from_bson(doc)() if doc is not None\
                                                            else None


        @classmethod
        @contract_epydoc
        def purge_healing_requests(cls, age=HEAL_REQUEST_PURGE_AGE, fdbw=None):
            """Purge all the requests in C{'hea'} state older than C{age}.

            @param age: how old should be requests to purge.
            @type age: timedelta, NoneType

            @param fdbw: FastDB wrapper.
            @type fdbw: abstract_docstorewrapper.AbstractDocStoreWrapper

            @rtype: models.HealChunkRequest, NoneType
            """
            oldest_to_survive = datetime.utcnow() - age
            res = fdbw.retried_sync(fdbw.heal_chunk_requests.remove,
                                    spec_or_id={
                                        'state': 'hea',
                                        'last_update_ts':
                                            {'$lte': oldest_to_survive}
                                    },
                                    safe=True)



    class MagnetLinks(object):

        __slots__ = ()

        collection_name = 'magnet_links'

        indices = {
            ('code',): [IndexOptions.UNIQUE],
            # Note this index also covers the basic queries over
            # just the 'username_uc'
            ('username_uc', 'ds_uuid', 'basedirectory_path', 'rel_path'):
                [IndexOptions.UNIQUE],
        }


        @classmethod
        @contract_epydoc
        def make_magnet_link(cls, username, ds_uuid, basedirectory_path,
                             rel_path, fdbw):
            """
            Create magnetlink for certain file_local, dataset and user
            OR return an already created one.

            @param username: the name of the user.
            @type username: basestring

            @param ds_uuid: the UUID of the Host.
            @type ds_uuid: UUID

            @param basedirectory_path: the path of base_directory. Is used to
                find unique file.
            @type basedirectory_path: basestring

            @param rel_path: the rel_path of file_local.
            @type rel_path: basestring

            @param fdbw: FastDB wrapper.
            @type fdbw: abstract_docstorewrapper.AbstractDocStoreWrapper

            @return: magnet-code or None if magnetlink is blocked.
            """
            error = True
            code = None
            username = username.upper()
            index_error = 'username_uc_1_ds_uuid_1_' \
                          'basedirectory_path_1_rel_path_1'
            while error:
                code = gen_magnet_code()
                magnetlink = \
                    models.MagnetLink(code=code,
                                      username_uc=username,
                                      ds_uuid=ds_uuid,
                                      basedirectory_path=basedirectory_path,
                                      rel_path=rel_path)
                try:
                    fdbw.retried_sync(fdbw.magnet_links.insert,
                                      doc_or_docs=magnetlink.to_bson(),
                                      fields=['code'],
                                      safe=True)
                    error = False
                    logger.debug('Magnet link created: %s', magnetlink.code)
                except pymongo.errors.DuplicateKeyError as e:
                    logger.debug(u'Duplicate magnet code: %r for rel_path %r '
                                     u'and user %r. Trying once more.',
                                 magnetlink.code, rel_path, username)
                    if index_error in e.message:
                        ml = cls.get_magnet_link(
                                 username=username, ds_uuid=ds_uuid,
                                 basedirectory_path=basedirectory_path,
                                 rel_path=rel_path, fdbw=fdbw)
                        if ml.blocked:
                            return None
                        if not ml.active:
                            activated = cls.activate_magnetlink(ml.code, fdbw)
                            if activated is None:
                                return None
                        code = ml.code
                        error = False
                    else:
                        error = True
            return code


        @classmethod
        @contract_epydoc
        def get_all_magnet_links(cls, fdbw):
            """
            @param fdbw: FastDB wrapper.
            @type fdbw: abstract_docstorewrapper.AbstractDocStoreWrapper

            @return: a (possibly non-reiterable) Iterable of
                C{models.MagnetLink} objects.
            @rtype: col.Iterable
            """
            return (models.MagnetLink.from_bson(line)()
                        for line in fdbw.retried_sync(fdbw.magnet_links.find))


        @classmethod
        @contract_epydoc
        def get_magnet_link(cls, code=None, username=None, ds_uuid=None,
                            basedirectory_path=None, rel_path=None,
                            inc_views=False, fdbw=None):
            """Create new magnet link for a given file or get an existing one.

            @param username: the name of the user.
            @type username: basestring, NoneType

            @param ds_uuid: the UUID of the Host.
            @type ds_uuid: UUID, NoneType

            @param basedirectory_path: the path of base_directory. Is used to
                find unique file.
            @type basedirectory_path: basestring, NoneType

            @param rel_path: the rel_path of file_local.
            @type rel_path: basestring, NoneType

            @return: the magnet link.
            @rtype: MagnetLink, NoneType
            """
            query = {}
            if code is not None:
                query['code'] = code
            if username is not None:
                query['username_uc'] = username.upper()
            if ds_uuid is not None:
                query['ds_uuid'] = ds_uuid
            if basedirectory_path is not None:
                query['basedirectory_path'] = basedirectory_path
            if rel_path is not None:
                query['rel_path'] = rel_path

            if not inc_views:
                doc = fdbw.retried_sync(fdbw.magnet_links.find_one,
                                        spec_or_id=query, safe=True)
            else:
                doc = fdbw.retried_sync(
                          fdbw.magnet_links.find_and_modify,
                          query=query,
                          update={'$inc': {'viewsN': 1},
                                  '$set': {'last_view_ts': datetime.utcnow()}},
                          safe=True)
            return models.MagnetLink.from_bson(doc)() if doc is not None \
                                                      else None


        @classmethod
        @contract_epydoc
        def inc_downloads(cls, code, fdbw):
            """
            Given a magnet code, increment the number of downloads and mark it
            as last downloaded just now.
            """
            fdbw.retried_sync(
                fdbw.magnet_links.find_and_modify,
                query={'code': code},
                update={'$inc': {'downloadsN': 1},
                        '$set': {'last_download_ts': datetime.utcnow()}},
                safe=True)


        @classmethod
        @contract_epydoc
        def modify_magnetlink(cls, magnet_code, action, fdbw):
            r"""Alter a magnet link record somehow, depending on C{action}.

            @precondition: action in ('activate', 'deactivate', 'block',
                                      'unblock', 'remove')

            @raises ValueError: if C{action} is not supported.
            """
            modify = remove = False

            if action == 'activate':
                query = {'$set': {'active': True}}
                modify = True
            elif action == 'deactivate':
                query = {'$set': {'active': False}}
                modify = True
            elif action == 'block':
                query = {'$set': {'blocked': True, 'active': False}}
                modify = True
            elif action == 'unblock':
                query = {'$set': {'blocked': False, 'active': True}}
                modify = True
            elif action == 'remove':
                query = {'$set': {'blocked': True, 'active': False}}
                remove = True
            else:
                raise ValueError(u'{!r} action is not supported!'.format(
                                     action))

            if modify:
                state = fdbw.retried_sync(fdbw.magnet_links.find_and_modify,
                                          query={'code': magnet_code},
                                          update=query,
                                          safe=True)
            elif remove:
                state = fdbw.retried_sync(fdbw.magnet_links.remove,
                                          spec_or_id={'code': magnet_code},
                                          safe=True)
            else:
                # What?
                raise NotImplementedError('{!r}, {!r}'.format(
                                              magnet_code, action))

            return state


        @classmethod
        def activate_magnetlink(cls, code, fdbw):
            return cls.modify_magnetlink(code, 'activate', fdbw)



    class RestoreRequests(object):
        """
        The namespace for all the FastDB queries related to
        the "restore_requests" collection.
        """

        __slots__ = ()

        collection_name = 'restore_requests'

        indices = {
            # for find_by_executor()
            ('executor', 'ts_exec'): [],
            # for atomic_start_oldest_restore_request().
            # Note it is not UNIQUE and doesn't need to be SPARSE
            ('ts_exec', 'ts_start'): [],
        }


        @classmethod
        @contract_epydoc
        def add_restore_request(cls,
                                username, base_ds_uuid, paths_to_restore,
                                fdbw):
            """Add a request to restore some data.

            @param base_ds_uuid: the UUID of the dataset used as a baseline
                to find the paths.
            @type base_ds_uuid: DatasetUUID

            @param fdbw: FastDB wrapper.
            @type fdbw: abstract_docstorewrapper.AbstractDocStoreWrapper

            @param paths_to_restore: the paths which should be restored.
            @type paths_to_restore: col.Iterable
            """
            req = models.RestoreRequest(username,
                                        base_ds_uuid,
                                        list(paths_to_restore),
                                        datetime.utcnow())
            fdbw.retried_sync(fdbw.restore_requests.insert,
                              doc_or_docs=req.to_bson(),
                              safe=True)


        @classmethod
        @contract_epydoc
        def find_by_executor(cls, executor_username, fdbw):
            """Get the restore requests executed by some user.

            @param executor_username: the username of the user who executed
                the restore requests.
            @type executor_username: basestring

            @param fdbw: FastDB wrapper.
            @type fdbw: abstract_docstorewrapper.AbstractDocStoreWrapper

            @returns: the iterable (might be non-reiterable)
                over the restore requests (C{models.RestoreRequest} objects),
                ordered from the oldest to the youngest.
            @rtype: col.Iterable
            """
            docs = fdbw.retried_sync(fdbw.restore_requests.find,
                                     spec={'executor': executor_username},
                                     sort=[('ts_exec', pymongo.ASCENDING)])
            return (models.RestoreRequest.from_bson(doc)()
                        for doc in docs)


        @classmethod
        @contract_epydoc
        def atomic_start_oldest_restore_request(cls, fdbw):
            """
            Find the oldest restore request not yet started,
            start it, and return the document (already marked as started).

            @param fdbw: FastDB wrapper.
            @type fdbw: abstract_docstorewrapper.AbstractDocStoreWrapper

            @rtype: models.RestoreRequest, NoneType
            """
            start_time = datetime.utcnow()
            doc = fdbw.retried_sync(fdbw.restore_requests.find_and_modify,
                                    query={'ts_start': None},
                                    sort=[('ts_exec', pymongo.ASCENDING)],
                                    update={'$set': {'ts_start': start_time}},
                                    new=True)

            return models.RestoreRequest.from_bson(doc)() if doc is not None \
                                                          else None


        @classmethod
        @contract_epydoc
        def set_ds_uuid(cls, _id, new_ds_uuid, fdbw):
            """
            For a given restore request, set the field with the information
            about the dataset just created to fulfill this restore request.

            @param _id: the key of the document.
            @type _id: bson.objectid.ObjectId

            @param new_ds_uuid: the UUID of the newly created dataset.
            @type new_ds_uuid: DatasetUUID

            @param fdbw: FastDB wrapper.
            @type fdbw: abstract_docstorewrapper.AbstractDocStoreWrapper
            """
            fdbw.retried_sync(fdbw.restore_requests.update,
                              spec={'_id': _id},
                              document={'$set': {'ds_uuid': new_ds_uuid}},
                              safe=True)



    class Transactions(object):
        """
        The namespace for all the FastDB queries related to
        the "transactions" collection.
        """

        __slots__ = ()

        collection_name = 'transactions'

        indices = {
            ('uuid',): [IndexOptions.UNIQUE],
            ('type',): [],
            ('dst',):  [],
        }


        @classmethod
        @contract_epydoc
        def add_transaction(cls,
                            type_, uuid, src_uuid, dst_uuid, ts, state,
                            parent_uuid=None,
                            fdbw=None):
            """Add a transaction with its initial data.

            @param type_: the type of the transaction.
            @type type_: basestring

            @param uuid: the UUID of the transaction.
            @type uuid: TransactionUUID

            @param src_uuid: the UUID of the transaction originator.
            @type src_uuid: PeerUUID

            @param dst_uuid: the UUID of the transaction received.
            @type dst_uuid: PeerUUID

            @param ts: the time of the transaction start.
            @type ts: datetime

            @type state: NodeTransactionState

            @param parent_uuid: (optional) the UUID of the parent transaction.
            @type parent_uuid: TransactionUUID, NoneType

            @param fdbw: FastDB wrapper.
            @type fdbw: abstract_docstorewrapper.AbstractDocStoreWrapper
            """
            tr = models.Transaction(type_=type_,
                                    uuid=uuid,
                                    src=src_uuid,
                                    dst=dst_uuid,
                                    ts=ts,
                                    state=state,
                                    parent=parent_uuid)
            fdbw.retried_sync(fdbw.transactions.insert,
                              doc_or_docs=tr.to_bson(),
                              safe=True)


        @classmethod
        @contract_epydoc
        def del_transaction(cls, uuid, fdbw):
            """Delete a transaction by its UUID.

            @param uuid: the UUID of the transaction to delete.
            @type uuid: TransactionUUID

            @param fdbw: FastDB wrapper.
            @type fdbw: abstract_docstorewrapper.AbstractDocStoreWrapper
            """
            res = fdbw.retried_sync(fdbw.transactions.remove,
                                    spec_or_id={'uuid': uuid},
                                    safe=True)


        @classmethod
        def _uuid_or_uuids_spec(cls, uuid_or_uuids):
            r"""Get a spec to match a single or multiple UUIDs.

            >>> f = FDBQueries.Transactions._uuid_or_uuids_spec
            >>> f(PeerUUID('f51329e6-42f4-4e60-8cd4-b5894abdd76f'))
            PeerUUID('f51329e6-42f4-4e60-8cd4-b5894abdd76f')

            >>> f([PeerUUID('96c72127-c697-43b7-a357-8301ea09f130'),
            ...    PeerUUID('9332d159-77f3-4e85-a58d-d95b72e41647'),
            ...    PeerUUID('3435327a-f736-4d29-818f-a8f981401545')]) \
            ... # doctest:+NORMALIZE_WHITESPACE
            {'$in': [PeerUUID('96c72127-c697-43b7-a357-8301ea09f130'),
                     PeerUUID('9332d159-77f3-4e85-a58d-d95b72e41647'),
                     PeerUUID('3435327a-f736-4d29-818f-a8f981401545')]}

            @type uuid_or_uuids: PeerUUID, col.Iterable
            """
            if isinstance(uuid_or_uuids, col.Iterable):
                uuids = list(uuid_or_uuids)
                assert consists_of(uuid_or_uuids, PeerUUID), \
                       repr(uuid_or_uuids)
                return {'$in': uuids}
            elif isinstance(uuid_or_uuids, UUID):
                return uuid_or_uuids
            else:
                raise TypeError(u'Wrong argument {!r}'.format(uuid_or_uuids))


        @classmethod
        @contract_epydoc
        def del_all_transactions_from_somebody(cls, src_uuid_or_uuids, fdbw):
            """
            Delete all transactions with C{src} field matching a single or
            several given ones.

            @param src_uuid_or_uuids: the UUID (or iterable over them)
                of the sender for the transactions to delete.
            @type src_uuid_or_uuids: PeerUUID, col.Iterable

            @param fdbw: FastDB wrapper.
            @type fdbw: abstract_docstorewrapper.AbstractDocStoreWrapper

            @return: the number of deleted transactions.
            @rtype: numbers.Integral
            """
            res = fdbw.retried_sync(fdbw.transactions.remove,
                                    spec_or_id={'src': cls._uuid_or_uuids_spec(
                                                           src_uuid_or_uuids)},
                                    safe=True)
            return res['n']


        @classmethod
        @contract_epydoc
        def del_all_transactions_to_somebody(cls, dst_uuid_or_uuids, fdbw):
            """
            Delete all transactions with C{dst} field matching a single or
            several given ones.

            @param dst_uuid_or_uuids: the UUID (or iterable over them)
                of the receiver for the transactions to delete.
            @type dst_uuid_or_uuids: PeerUUID, col.Iterable

            @param fdbw: FastDB wrapper.
            @type fdbw: abstract_docstorewrapper.AbstractDocStoreWrapper

            @return: the number of deleted transactions.
            @rtype: numbers.Integral
            """
            res = fdbw.retried_sync(fdbw.transactions.remove,
                                    spec_or_id={'dst': cls._uuid_or_uuids_spec(
                                                           dst_uuid_or_uuids)},
                                    safe=True)
            return res['n']


        @classmethod
        @contract_epydoc
        def get_transaction_state(cls, uuid, fdbw):
            """Get a transaction by the transaction UUID.

            @param uuid: the UUID of the transaction.
            @type uuid: TransactionUUID

            @param fdbw: FastDB wrapper.
            @type fdbw: abstract_docstorewrapper.AbstractDocStoreWrapper

            @rtype: NodeTransactionState, NoneType
            """
            doc = fdbw.retried_sync(fdbw.transactions.find_one,
                                    spec_or_id={'uuid': uuid})

            return models.Transaction.state_from_tr_bson(doc) \
                       if doc is not None else None


        @classmethod
        @contract_epydoc
        def transaction_exists(cls, uuid, fdbw):
            """Find whether a transaction (with some particular UUID) exists.

            @param uuid: the UUID of the transaction.
            @type uuid: TransactionUUID

            @param fdbw: FastDB wrapper.
            @type fdbw: abstract_docstorewrapper.AbstractDocStoreWrapper

            @rtype: bool
            """
            doc = fdbw.retried_sync(fdbw.transactions.find_one,
                                    spec_or_id={'uuid': uuid},
                                    fields=['_id'])

            return (doc is not None)


        @classmethod
        def _spec_for_get_transaction_states(cls, tr_type=None, dst_uuid=None):
            r"""
            Generate a MongoDB spec for C{.get_transaction_states()} query.

            >>> FDBQueries.Transactions._spec_for_get_transaction_states()
            {}

            >>> FDBQueries.Transactions._spec_for_get_transaction_states(
            ...     tr_type='BACKUP'
            ... )
            {'type': 'BACKUP'}

            >>> FDBQueries.Transactions._spec_for_get_transaction_states(
            ...     dst_uuid=UUID(int=0x42)
            ... )
            {'dst': UUID('00000000-0000-0000-0000-000000000042')}

            >>> FDBQueries.Transactions._spec_for_get_transaction_states(
            ...     tr_type='BACKUP',
            ...     dst_uuid=UUID(int=0x42)
            ... )  # doctest:+NORMALIZE_WHITESPACE
            {'dst': UUID('00000000-0000-0000-0000-000000000042'),
             'type': 'BACKUP'}

            @type tr_type: basestring, NoneType
            @type dst_uuid: UUID, NoneType
            """
            # Whole-transaction conditions
            spec = {}
            if tr_type is not None:
                spec['type'] = tr_type
            if dst_uuid:
                spec['dst'] = dst_uuid
            return spec


        @classmethod
        @contract_epydoc
        def get_transaction_states(cls,
                                   tr_type=None, dst_uuid=None, fdbw=None):
            """Get a transaction by any of the provided arguments.

            @param tr_type: the name of the transaction,
                or C{None} if not filtering by it.
            @type tr_type: basestring, NoneType

            @param dst_uuid: the UUID of the transaction destination,
                or C{None} if not filtering by it.
            @type dst_uuid: PeerUUID, NoneType

            @param fdbw: FastDB wrapper.
            @type fdbw: abstract_docstorewrapper.AbstractDocStoreWrapper

            @rtype: col.Iterable
            @return: an iterable that consists of C{NodeTransactionState}
                objects.
            """
            spec = cls._spec_for_get_transaction_states(tr_type=tr_type,
                                                        dst_uuid=dst_uuid)
            logger.verbose('Getting transaction states with spec: %r', spec)

            docs = fdbw.retried_sync(fdbw.transactions.find,
                                     spec=spec)
            return imap(models.Transaction.state_from_tr_bson, docs)


        @classmethod
        @contract_epydoc
        def update_transaction_state(cls, uuid, state, fdbw):
            """Update a transaction state.

            @param uuid: the UUID of the transaction.
            @type uuid: TransactionUUID

            @type state: NodeTransactionState

            @param fdbw: FastDB wrapper.
            @type fdbw: abstract_docstorewrapper.AbstractDocStoreWrapper
            """
            fdbw.retried_sync(fdbw.transactions.update,
                              spec={'uuid': uuid},
                              document={'$set': {'state': state.to_bson()}},
                              safe=True)


        @classmethod
        @contract_epydoc
        def get_transactions_by_names(cls, transaction_types=None, fdbw=None):
            """Get all the transactions with the given transaction types.

            @param transaction_types: names of transactions that we
                need to get.
                Can be C{None} to get (yours C.O.) all transactions.
            @type transaction_types: col.Iterable, NoneType

            @param fdbw: FastDB wrapper.
            @type fdbw: abstract_docstorewrapper.AbstractDocStoreWrapper
            """
            spec = {'type': {'$in': list(transaction_types)}} \
                       if transaction_types is not None else None

            return (models.Transaction.from_bson(doc)()
                        for doc in fdbw.retried_sync(fdbw.transactions.find,
                                                     spec=spec))



    class Uploads(object):
        """Now occurring multiple files upload transactions."""

        __slots__ = ()

        collection_name = 'uploads'

        indices = {
            ('group_uuid', 'upload_uuid'): [IndexOptions.UNIQUE],
            ('state', 'last_update_ts'): [],
        }


        UploadOperation = col.namedtuple(
                              'UploadOperation',
                              ('group_uuid', 'upload_uuid', 'ds_uuid'))


        @classmethod
        @contract_epydoc
        def __upsert_upload_state(cls,
                                  group_uuid, upload_uuid, from_, to_, fdbw):
            """Update upload operation, or insert if missing.

            @note: it also "reheats" the upload internally, i.e. changes its
                'last_update_ts'.

            @param group_uuid: (optional) UUID of the user group,
                that initiated upload.
            @type group_uuid: UUID, NoneType

            @param upload_uuid: (optional) UUID of the upload operation.
            @type upload_uuid: UUID, NoneType

            @param from_: the state the operation should be in.
            @type from_: basestring

            @param to_: new state for upload operation.
            @type to_: basestring

            @param fdbw: FastDB wrapper.
            @type fdbw: abstract_docstorewrapper.AbstractDocStoreWrapper
            """
            spec = {'group_uuid': group_uuid,
                    'upload_uuid': upload_uuid,
                    'state': from_}

            to_set = {'group_uuid': group_uuid,
                      'upload_uuid': upload_uuid,
                      'state': to_,
                      'last_update_ts': datetime.utcnow(),
                      'ds_uuid': NULL_UUID}

            fdbw.retried_sync(fdbw.uploads.update,
                              spec=spec,
                              document={'$set': to_set},
                              safe=True,
                              upsert=True)


        @classmethod
        @contract_epydoc
        def __update_upload_state(cls,
                                  group_uuid, upload_uuid, from_, to_,
                                  ds_uuid=None, fdbw=None):
            """Update upload operation.

            @note: it also "reheats" the upload internally, i.e. changes its
                'last_update_ts'.

            @param group_uuid: (optional) UUID of the user group,
                that initiated upload.
            @type group_uuid: UUID, NoneType

            @param upload_uuid: (optional) UUID of the upload operation.
            @type upload_uuid: UUID, NoneType

            @param from_: the state the operation should be in.
            @type from_: basestring

            @param to_: new state for upload operation.
            @type to_: basestring

            @param ds_uuid: (optional) the UUID of newly created dataset
                to set in the document.
                If C{None}, nothing should be set.
                Otherwise the value is set to C{doc['ds_uuid']}.
            @type ds_uuid: UUID, NoneType

            @param fdbw: FastDB wrapper.
            @type fdbw: abstract_docstorewrapper.AbstractDocStoreWrapper
            """
            spec = {'group_uuid': group_uuid,
                    'upload_uuid': upload_uuid,
                    'state': from_}

            to_set = {'state': to_,
                      'last_update_ts': datetime.utcnow()}
            # Optional:
            if ds_uuid is not None:
                to_set['ds_uuid'] = ds_uuid

            fdbw.retried_sync(fdbw.uploads.update,
                              spec=spec,
                              document={'$set': to_set},
                              safe=True)


        @classmethod
        @contract_epydoc
        def reheat_upload(cls, group_uuid, upload_uuid, fdbw):
            """
            "Reheat" upload operation on every completed file. Transaction
            must have "in progress" state.

            @param group_uuid: UUID of the user group, which initiated
                upload.
            @type group_uuid: UUID

            @param upload_uuid: UUID of the upload operation.
            @type upload_uuid: UUID

            @param fdbw: FastDB wrapper.
            @type fdbw: abstract_docstorewrapper.AbstractDocStoreWrapper
            """
            cls.__upsert_upload_state(group_uuid, upload_uuid,
                                      'inp', 'inp', fdbw)


        @classmethod
        @contract_epydoc
        def ready_to_finalize_upload(cls, group_uuid, upload_uuid, fdbw):
            """
            Check upload transaction state (must be "in progress") and change
            it to "ready to finalize".

            This function likely should be called after all files are uploaded
            to FastDB (or after some cooldown in Trusted Worker).

            @note: it also "reheats" the upload internally.

            @param group_uuid: UUID of the user group, that initiated
                the upload.
            @type group_uuid: UUID

            @param upload_uuid: UUID of the upload transaction.
            @type upload_uuid: UUID

            @param fdbw: FastDB wrapper.
            @type fdbw: abstract_docstorewrapper.AbstractDocStoreWrapper
            """
            cls.__update_upload_state(group_uuid, upload_uuid,
                                      from_='inp', to_='rtf', fdbw=fdbw)


        @classmethod
        @contract_epydoc
        def dequeue_upload_to_finalize(cls, fdbw):
            """
            Find a "ready to finalize" upload operation,
            atomically change it to "finalizing" and return it.

            @note: it also "reheats" the upload internally.

            @param fdbw: FastDB wrapper.
            @type fdbw: abstract_docstorewrapper.AbstractDocStoreWrapper

            @return: FDBQueries.Uploads.UploadOperation
            """
            doc = fdbw.retried_sync(
                      fdbw.uploads.find_and_modify,
                      query={'state': 'rtf'},
                      update={'$set': {'state': 'fin',
                                       'last_update_ts': datetime.utcnow()}},
                      sort=[('last_update_ts', pymongo.ASCENDING)],
                      safe=True,
                      new=True)
            return None if doc is None \
                        else cls.UploadOperation(
                                 group_uuid=UserGroupUUID.safe_cast_uuid(
                                                doc['group_uuid']),
                                 upload_uuid=doc['upload_uuid'],
                                 ds_uuid=DatasetUUID.safe_cast_uuid(
                                             doc['ds_uuid']))


        @classmethod
        @contract_epydoc
        def ready_to_synchronize_upload(cls, group_uuid, upload_uuid, ds_uuid,
                                        fdbw):
            """
            Check upload transaction state (must be "finalizing") and change
            it to "ready to synchronize".

            This function likely should be called after the dataset has been
            created (or after some cooldown in Trusted Worker).

            @note: it also "reheats" the upload internally.

            @param group_uuid: UUID of the user group, that initiated
                upload.
            @type group_uuid: UUID

            @param upload_uuid: UUID of the upload transaction.
            @type upload_uuid: UUID

            @param ds_uuid: the UUID of newly created dataset.
            @type ds_uuid: DatasetUUID

            @param fdbw: FastDB wrapper.
            @type fdbw: abstract_docstorewrapper.AbstractDocStoreWrapper
            """
            cls.__update_upload_state(group_uuid, upload_uuid,
                                      from_='fin', to_='rts', ds_uuid=ds_uuid,
                                      fdbw=fdbw)


        @classmethod
        @contract_epydoc
        def add_rts_upload(cls, group_uuid, ds_uuid, fdbw):
            """
            Alternatively to the regular "upload" flow, one can create the
            upload record manually, already in "rts" state.

            @note: the C{upload_uuid} field is fake and useless.

            @param group_uuid: UUID of the user group, that initiated
                the upload.
            @type group_uuid: UserGroupUUID

            @param ds_uuid: the UUID of newly created dataset.
            @type ds_uuid: DatasetUUID

            @param fdbw: FastDB wrapper.
            @type fdbw: abstract_docstorewrapper.AbstractDocStoreWrapper
            """
            doc = {
                'group_uuid': group_uuid,
                'upload_uuid': gen_uuid(),
                'last_update_ts': datetime.utcnow(),
                'state': 'rts',
                'ds_uuid': ds_uuid
            }
            fdbw.retried_sync(fdbw.uploads.insert,
                              doc_or_docs=doc,
                              safe=True)


        @classmethod
        @contract_epydoc
        def dequeue_upload_to_synchronize(cls, fdbw):
            """
            Find a "ready to synchronize" upload operation,
            atomically change it to "synchronizing" and return it.

            @note: it also "reheats" the upload internally.

            @param fdbw: FastDB wrapper.
            @type fdbw: abstract_docstorewrapper.AbstractDocStoreWrapper

            @return: FDBQueries.Uploads.UploadOperation
            """
            doc = fdbw.retried_sync(
                      fdbw.uploads.find_and_modify,
                      query={'state': 'rts'},
                      update={'$set': {'state': 'syn',
                                       'last_update_ts': datetime.utcnow()}},
                      sort=[('last_update_ts', pymongo.ASCENDING)],
                      safe=True,
                      new=True)
            return None if doc is None \
                        else cls.UploadOperation(
                                 group_uuid=UserGroupUUID.safe_cast_uuid(
                                                doc['group_uuid']),
                                 upload_uuid=doc['upload_uuid'],
                                 ds_uuid=DatasetUUID.safe_cast_uuid(
                                             doc['ds_uuid']))


        @classmethod
        @contract_epydoc
        def remove_upload_after_sync(cls, group_uuid, upload_uuid, fdbw):
            """
            Remove upload operation as soon as its synchronizing phase
            was completed.

            @param group_uuid: UUID of the user group, that initiated
                upload.
            @type group_uuid: UUID

            @param upload_uuid: UUID of the upload transaction.
            @type upload_uuid: UUID

            @param fdbw: FastDB wrapper.
            @type fdbw: abstract_docstorewrapper.AbstractDocStoreWrapper
            """
            fdbw.retried_sync(fdbw.uploads.remove,
                              spec_or_id={'group_uuid': group_uuid,
                                          'upload_uuid': upload_uuid,
                                          'state': 'syn'},
                              safe=True)



    class Users(object):
        """
        The namespace for all the FastDB queries related to
        the "users" collection.
        """

        __slots__ = ()

        collection_name = 'users'

        indices = {
            ('name_uc',): [IndexOptions.UNIQUE],
            ('hosts.uuid',): [IndexOptions.SPARSE, IndexOptions.UNIQUE],
            ('hosts.last_seen',): [],
        }


        @classmethod
        @contract_epydoc
        def add_user(cls, username, digest, is_trusted, fdbw):
            """Add a user with its initial data.

            @type username: basestring
            @type digest: str
            @precondition: len(digest) == 40
            @type is_trusted: bool

            @param fdbw: FastDB wrapper.
            @type fdbw: abstract_docstorewrapper.AbstractDocStoreWrapper
            """
            user = models.User(name=username,
                               digest=digest,
                               trusted=is_trusted)
            fdbw.retried_sync(fdbw.users.insert,
                              doc_or_docs=user.to_bson(),
                              safe=True)


        @classmethod
        @contract_epydoc
        def find_by_name(cls, username, fdbw):
            """Find a user by their username.

            The search is case-insensitive!

            @param username: the username to search (case-insensitive).
            @type username: basestring

            @param fdbw: FastDB wrapper.
            @type fdbw: abstract_docstorewrapper.AbstractDocStoreWrapper

            @returns: a C{models.User} object, or C{None} if not found.
            @rtype: models.User, NoneType
            """
            doc = fdbw.retried_sync(fdbw.users.find_one,
                                    spec_or_id={'name_uc': username.upper()})

            return models.User.from_bson(doc)() if doc is not None else None


        @classmethod
        @contract_epydoc
        def update_user_digest(cls, username, digest, fdbw):
            """Update the digest of the user.

            The search is case-insensitive!

            @param username: the username to search (case-insensitive).
            @type username: basestring

            @param digest: the new digest.
            @type digest: str
            @precondition: len(digest) == 40

            @param fdbw: FastDB wrapper.
            @type fdbw: abstract_docstorewrapper.AbstractDocStoreWrapper
            """
            fdbw.retried_sync(fdbw.users.update,
                              spec={'name_uc': username.upper()},
                              document={'$set': {'digest': digest}},
                              safe=True)


        @classmethod
        @contract_epydoc
        def add_host(cls, username, host_uuid, fdbw):
            """Add a host to the user.

            @param username: the name of the user (case-insensitive).
            @type username: basestring

            @param host_uuid: the UUID of the host.
            @type host_uuid: HostUUID

            @param fdbw: FastDB wrapper.
            @type fdbw: abstract_docstorewrapper.AbstractDocStoreWrapper
            """
            host = models.Host(uuid=host_uuid,
                               urls=[])

            fdbw.retried_sync(fdbw.users.update,
                              spec={'name_uc': username.upper()},
                              document={'$push': {'hosts': host.to_bson()},
                                        '$inc': {'hostsN': 1}},
                              safe=True)


        @classmethod
        def _spec_for_get_hosts(cls,
                                username=None,
                                oldest_revive_ts=None, newest_revive_ts=None,
                                host_uuids=None):
            r"""Generate a MongoDB spec for C{.get_hosts()} query below.

            It must be used together with C{_host_post_filter_for_get_hosts()}.

            >>> FDBQueries.Users._spec_for_get_hosts()
            {}

            >>> FDBQueries.Users._spec_for_get_hosts(
            ...     username='JohnDoe'
            ... )  # doctest:+NORMALIZE_WHITESPACE
            {'name_uc': 'JOHNDOE'}

            >>> FDBQueries.Users._spec_for_get_hosts(
            ...     oldest_revive_ts=datetime(2012, 1, 3)
            ... )  # doctest:+NORMALIZE_WHITESPACE
            {'hosts':
                {'$elemMatch':
                    {'revive_ts':
                        {'$gte': datetime.datetime(2012, 1, 3, 0, 0)}}}}

            >>> FDBQueries.Users._spec_for_get_hosts(
            ...     newest_revive_ts=datetime(2012, 5, 7)
            ... )  # doctest:+NORMALIZE_WHITESPACE
            {'hosts':
                {'$elemMatch':
                    {'revive_ts':
                        {'$lte': datetime.datetime(2012, 5, 7, 0, 0)}}}}

            >>> FDBQueries.Users._spec_for_get_hosts(
            ...     host_uuids=[UUID(int=0x42), UUID(int=0x84)]
            ... )  # doctest:+NORMALIZE_WHITESPACE,+ELLIPSIS
            {'hosts':
                {'$elemMatch':
                    {'uuid':
                        {'$in': [UUID('0000...0042'), UUID('0000...0084')]}}}}

            >>> FDBQueries.Users._spec_for_get_hosts(
            ...     oldest_revive_ts=datetime(2012, 1, 3),
            ...     newest_revive_ts=datetime(2012, 5, 7),
            ...     host_uuids=[UUID(int=0x42), UUID(int=0x84)]
            ... )  # doctest:+NORMALIZE_WHITESPACE,+ELLIPSIS
            {'hosts':
                {'$elemMatch':
                    {'revive_ts':
                        {'$lte': datetime.datetime(2012, 5, 7, 0, 0),
                         '$gte': datetime.datetime(2012, 1, 3, 0, 0)},
                    'uuid':
                        {'$in': [UUID('0000...0042'), UUID('0000...0084')]}}}}

            >>> FDBQueries.Users._spec_for_get_hosts(
            ...     username='JohnDoe',
            ...     oldest_revive_ts=datetime(2012, 1, 3),
            ...     host_uuids=[UUID(int=0x42), UUID(int=0x84)]
            ... )  # doctest:+NORMALIZE_WHITESPACE,+ELLIPSIS
            {'hosts':
                {'$elemMatch':
                    {'revive_ts':
                        {'$gte': datetime.datetime(2012, 1, 3, 0, 0)},
                    'uuid':
                        {'$in': [UUID('0000...0042'), UUID('0000...0084')]}}},
            'name_uc': 'JOHNDOE'}

            @type username: basestring, NoneType
            @type oldest_revive_ts: datetime, NoneType
            @param host_uuids: either C{None}, or an iterable of UUIDs.
            @type host_uuids: col.Iterable, NoneType
            """
            # Host-specific conditions
            host_spec = {}

            if oldest_revive_ts is not None:
                host_spec['revive_ts'] = {'$gte': oldest_revive_ts}
            if newest_revive_ts is not None:
                host_spec.setdefault('revive_ts', {}) \
                         .update({'$lte': newest_revive_ts})

            if host_uuids is not None:
                host_spec['uuid'] = {'$in': list(host_uuids)}

            # Whole-user conditions
            spec = {}
            if username is not None:
                spec['name_uc'] = username.upper()
            if host_spec:
                spec['hosts'] = {'$elemMatch': host_spec}

            return spec


        @classmethod
        def _host_post_filter_for_get_hosts(
                cls, host_doc,
                oldest_revive_ts=None, newest_revive_ts=None, host_uuids=None):
            r"""Generate a Python post-filter for C{.get_hosts()} query below.

            It must be used together with C{_spec_for_get_hosts()}.

            @param host_doc: the BSON document of a host to be filtered.
            @type host_doc: dict

            @type host_uuids: col.Iterable, NoneType

            @todo: change the code to use the $-projections
                when fully supported.
            """
            if host_uuids is not None:
                host_uuids = frozenset(host_uuids)

            return ((oldest_revive_ts is None or
                     host_doc.get('revive_ts', datetime.min)
                         >= oldest_revive_ts) and
                    (newest_revive_ts is None or
                     host_doc.get('revive_ts', datetime.min)
                         <= newest_revive_ts) and
                    (host_uuids is None or
                     host_doc['uuid'] in host_uuids))


        @classmethod
        @contract_epydoc
        def get_hosts(cls,
                      username=None,
                      oldest_revive_ts=None, newest_revive_ts=None,
                      host_uuids=None, fdbw=None):
            """Get all the hosts, possibly filtered somehow.

            @param username: the name of the user whose hosts should be taken.
                If C{None}, no extra filtering on the C{username} field
                is performed.
                Note: filtering is case-insensitive!
            @type username: basestring, NoneType

            @param oldest_revive_ts: the oldest datetime to use.
                If C{None}, no extra filtering on the C{revive_ts} field
                is performed.
            @type oldest_revive_ts: datetime, NoneType

            @param newest_revive_ts: the newest datetime to use.
                If C{None}, no extra filtering on the C{revive_ts} field
                is performed.
            @type newest_revive_ts: datetime, NoneType

            @param host_uuids: the iterable over the host UUIDs to check.
                If C{None}, no extra filtering on the host UUIDs
                is performed.
            @type host_uuids: col.Iterable, NoneType

            @param fdbw: FastDB wrapper. Must NOT be None!
            @type fdbw: abstract_docstorewrapper.AbstractDocStoreWrapper

            @returns: the non-reiterable Iterable of C{models.Host} objects
                containing the per-host data.
            @rtype: col.Iterable
            """
            # Construct the filtering query
            spec = cls._spec_for_get_hosts(username=username,
                                           oldest_revive_ts=oldest_revive_ts,
                                           newest_revive_ts=newest_revive_ts,
                                           host_uuids=host_uuids)
            logger.verbose('Getting hosts with spec: %r', spec)

            partial_docs = fdbw.retried_sync(fdbw.users.find,
                                             spec=spec,
                                             fields=['hosts'])
            # We get ALL the hosts for the user; we need to post-filter them
            hosts_unfiltered = \
                chain.from_iterable(partial_doc['hosts']
                                        for partial_doc in partial_docs)

            hosts = (host for host in hosts_unfiltered
                          if cls._host_post_filter_for_get_hosts(
                                 host_doc=host,
                                 oldest_revive_ts=oldest_revive_ts,
                                 newest_revive_ts=newest_revive_ts,
                                 host_uuids=host_uuids))

            return (models.Host.from_bson(h)() for h in hosts)


        @classmethod
        @contract_epydoc
        def get_host_with_user(cls, host_uuid, fdbw):
            """
            Return the host by its UUID (or C{None}), together with its user.

            @param host_uuid: the UUID of the host.
            @type host_uuid: HostUUID

            @param fdbw: FastDB wrapper.
            @type fdbw: abstract_docstorewrapper.AbstractDocStoreWrapper

            @returns: either C{None}, or a tuple
                of C{(models.Host, models.User)}.
            @rtype: tuple, NoneType
            @postcondition: isinstance(result, NoneType) or
                            (isinstance(result[0], models.Host) and
                             isinstance(result[1], models.User))
            """
            doc = fdbw.retried_sync(fdbw.users.find_one,
                                    spec_or_id={'hosts.uuid': host_uuid})
            if doc is None:
                return None
            else:
                # We have to manually post-filter the particular host.
                # todo: change the code to use the $-projections
                # when fully supported.
                matching_hosts = filter(lambda h: h['uuid'] == host_uuid,
                                        doc['hosts'])
                assert len(matching_hosts) == 1, \
                       (host_uuid, matching_hosts)
                # Get both the host and the user.
                return (models.Host.from_bson(matching_hosts[0])(),
                        models.User.from_bson(doc)())


        @classmethod
        @contract_epydoc
        def get_hosts_count(cls, oldest_revive_ts=None, fdbw=None):
            """
            Get the total amount of hosts in the system,
            possibly filtered somehow.

            @param oldest_revive_ts: the oldest datetime to use.
                If C{None}, no extra filtering on the C{revive_ts} field
                is performed.
            @type oldest_revive_ts: datetime, NoneType

            @param fdbw: FastDB wrapper.
            @type fdbw: abstract_docstorewrapper.AbstractDocStoreWrapper

            @rtype: numbers.Integral

            @todo: for MongoDB 2.2, change to C{.aggregate()};
                or, at some point, remove at all.
            """
            spec = {}
            if oldest_revive_ts is not None:
                spec['hosts.revive_ts'] = {'$gt': oldest_revive_ts}

            # Besides a MongoDB-side filter for hosts for a query that STILL
            # returns the users (together with ALL their hosts),
            # we need a post-return filter to get only the appropriate hosts
            # (until we use MongoDB 2.2's .aggregate()).
            hosts_post_filter = \
                (lambda h: True) \
                    if oldest_revive_ts is None \
                    else (lambda h: h.get('revive_ts', datetime.min)
                                    > oldest_revive_ts)
            # todo: change the code to use the $-projections
            # when fully supported.

            # Doing the summing of the hosts counts on the Python side,
            # as the fastest implementation at the moment.
            hosts_counts = fdbw.retried_sync(fdbw.users.find,
                                             spec=spec,
                                             fields=['hostsN'])
            return sum(hc['hostsN']
                           for hc in hosts_counts
                           if hosts_post_filter(hc))
            # Another way:
            # db.users.group({
            #     key: '_id',
            #     initial: {count: 0},
            #     reduce: function(obj, prev) {
            #         prev.count += obj.hostsN
            #     }
            # })


        @classmethod
        def _spec_for_is_peer_alive(cls,
                                    host_uuid=None, oldest_revive_ts=None):
            r"""Generate a MongoDB spec for C{.is_peer_alive()} query below.

            >>> FDBQueries.Users._spec_for_is_peer_alive()
            {}

            >>> FDBQueries.Users._spec_for_is_peer_alive(
            ...     host_uuid=UUID(int=0x42)
            ... )  # doctest:+NORMALIZE_WHITESPACE
            {'hosts':
                {'$elemMatch':
                    {'uuid': UUID('00000000-0000-0000-0000-000000000042')}}}

            >>> FDBQueries.Users._spec_for_is_peer_alive(
            ...     oldest_revive_ts=datetime(2012, 1, 3, 0, 0)
            ... )  # doctest:+NORMALIZE_WHITESPACE
            {'hosts':
                {'$elemMatch':
                    {'revive_ts':
                        {'$gt': datetime.datetime(2012, 1, 3, 0, 0)}}}}

            >>> FDBQueries.Users._spec_for_is_peer_alive(
            ...     host_uuid=UUID(int=0x42),
            ...     oldest_revive_ts=datetime(2012, 1, 3, 0, 0)
            ... )  # doctest:+NORMALIZE_WHITESPACE
            {'hosts':
                {'$elemMatch':
                    {'revive_ts':
                        {'$gt': datetime.datetime(2012, 1, 3, 0, 0)},
                    'uuid':
                        UUID('00000000-0000-0000-0000-000000000042')}}}

            @type host_uuid: HostUUID, NoneType
            @type oldest_revive_ts: datetime, NoneType
            """
            # Host-specific conditions
            host_spec = {}
            if host_uuid is not None:
                host_spec['uuid'] = host_uuid
            if oldest_revive_ts is not None:
                host_spec['revive_ts'] = {'$gt': oldest_revive_ts}

            # Whole-user conditions
            spec = {}
            if host_spec:
                spec['hosts'] = {'$elemMatch': host_spec}

            return spec


        @classmethod
        @contract_epydoc
        def is_peer_alive(cls, host_uuid, oldest_revive_ts, fdbw):
            """Return whether the peer should be considered alive.

            @param host_uuid: the UUID of the host.
            @type host_uuid: HostUUID

            @param oldest_revive_ts: the oldest timestamp for a host
                to be considered alive if its C{revive_ts} time
                is newer than it.
            @type oldest_revive_ts: datetime

            @param fdbw: FastDB wrapper.
            @type fdbw: abstract_docstorewrapper.AbstractDocStoreWrapper

            @rtype: bool
            """
            spec = cls._spec_for_is_peer_alive(
                       host_uuid=host_uuid,
                       oldest_revive_ts=oldest_revive_ts)
            logger.verbose('Checking is_peer_alive: %r', spec)

            res = fdbw.retried_sync(fdbw.users.find_one,
                                    spec_or_id=spec,
                                    fields=['_id'])
            return res is not None


        @classmethod
        @contract_epydoc
        def update_host_info(cls, host_uuid, urls, timestamp, fdbw):
            """Update the information about the host presence.

            @param host_uuid: the UUID of the host.
            @type host_uuid: HostUUID

            @param urls: the list of the URLs on which the host claims
                to be reachable.
            @type urls: col.Iterable

            @param timestamp: the timestamp when the host was seen.
                Updates the fields C{last_seen} and C{revive_ts}.
                If C{None}, this means that requester wants to "unsee"
                the host. This will set its C{revive_ts} field
                to C{datetime.min} but will not reset the C{last_seen} field.
            @type timestamp: datetime, NoneType

            @return: the time of previous host revive,
                or C{None} if not found/too old (that is,
                contains the default value of C{datetime.min} in the DB).
            @rtype: datetime, NoneType

            @raise Exception: if the host was missing before
                (the function assumes it was already initialized).

            @param fdbw: FastDB wrapper.
            @type fdbw: abstract_docstorewrapper.AbstractDocStoreWrapper

            @todo: when possible, change the query so it does not ever decrease
                the C{last_seen} field; and so it doesn't decrease
                the C{revive_ts} field unless resetting it to C{datetime.min}.
            """
            # last_seen should never be reset.
            set_op = {'hosts.$.urls': urls,
                      # revive_ts should be reset to datetime.min
                      # if None is passed
                      'hosts.$.revive_ts': coalesce(timestamp, datetime.min)}
            if timestamp is not None and timestamp != datetime.min:
                # If timestamp is None, we'll just avoid modifying
                # the last_seen field.
                # The comparison with C{timestamp != datetime.min}
                # is unnecessary but let's do it for safety.
                set_op['hosts.$.last_seen'] = timestamp

            result = fdbw.retried_sync(fdbw.users.find_and_modify,
                                       query={'hosts.uuid': host_uuid},
                                       update={'$set': set_op},
                                       fields=['hosts'],
                                       safe=True)
            # We've updated some host. Now let's find it and return
            # its previous timestamp.

            # todo: change the code to use the $-projections
            # when fully supported.
            for host in result['hosts']:
                if host['uuid'] == host_uuid:
                    # TODO: change the following line to host['revive_ts']
                    # when all hosts contain this field.
                    revive_ts = host.get('revive_ts', datetime.min)
                    return revive_ts if revive_ts != datetime.min else None
            else:
                # But is it even possible? Though generate the exception
                # for safety anyway.
                raise Exception('Updating non-existing host {}'
                                    .format(host_uuid))


        @classmethod
        @contract_epydoc
        def get_last_msg_read_ts(cls, username, fdbw):
            """Get the timestamp of last message read by user.

            @param username: the name of the user.
            @type username: basestring

            @param fdbw: FastDB wrapper.
            @type fdbw: abstract_docstorewrapper.AbstractDocStoreWrapper

            @returns: the timestamp, or C{None} if the user cannot be found.
            @rtype: datetime, NoneType
            """
            user_data = fdbw.retried_sync(
                            fdbw.users.find_one,
                            spec_or_id={'name_uc': username.upper()},
                            fields=['last_msg_read_ts'])
            return user_data['last_msg_read_ts'] if user_data is not None \
                                                 else None


        @classmethod
        @contract_epydoc
        def get_last_msg_read_and_sync_ts(cls, username, host_uuid, fdbw):
            """
            Get the timestamps of last message read by user and
            of the last message synced to the host, by a single query.

            @param username: the name of the user.
            @type username: basestring

            @param host_uuid: the UUID of the host.
            @type host_uuid: PeerUUID

            @returns: a tuple of (last_msg_read_ts, last_msg_sync_ts),
                or C{(None, None)} if no such user exists.
            @rtype: tuple

            @param fdbw: FastDB wrapper.
            @type fdbw: abstract_docstorewrapper.AbstractDocStoreWrapper
            """
            user_data = fdbw.retried_sync(
                            fdbw.users.find_one,
                            spec_or_id={'name_uc': username.upper()},
                            fields=['last_msg_read_ts',
                                    'hosts.uuid',
                                    'hosts.last_msg_sync_ts'])

            if user_data is None:
                return None, None
            else:
                # todo: change the code to use the $-projections
                # when fully supported.
                _hosts = filter(lambda h: h['uuid'] == host_uuid,
                                user_data.get('hosts', []))
                assert len(_hosts) in (0, 1), repr(_hosts)
                last_msg_sync_ts = \
                    _hosts[0]['last_msg_sync_ts'] if _hosts else datetime.min

                last_msg_read_ts = user_data['last_msg_read_ts']

                return last_msg_read_ts, last_msg_sync_ts


        @classmethod
        def _spec_for_update_last_msg_read_ts(cls, username, ts):
            r"""Generate a MongoDB spec for C{.update_last_msg_read_ts()}
            query below.

            >>> FDBQueries.Users._spec_for_update_last_msg_read_ts(
            ...     username='JohnDoe',
            ...     ts=datetime(2012, 1, 3, 0, 0)
            ... )  # doctest:+NORMALIZE_WHITESPACE,+ELLIPSIS
            {'last_msg_read_ts': {'$lt': datetime.datetime(2012, 1, 3, 0, 0)},
            'name_uc': 'JOHNDOE'}

            @type username: basestring
            @type ts: datetime
            """
            return {
                'name_uc': username.upper(),
                'last_msg_read_ts': {'$lt': ts}
            }


        @classmethod
        @contract_epydoc
        def update_last_msg_read_ts(cls, username, ts, fdbw):
            """
            Update the "last message read by user" timestamp,
            but only if it is newer than the existing value.

            @param username: the name of the user
            @type username: basestring

            @type ts: datetime

            @param fdbw: FastDB wrapper.
            @type fdbw: abstract_docstorewrapper.AbstractDocStoreWrapper
            """
            spec = cls._spec_for_update_last_msg_read_ts(username=username,
                                                         ts=ts)
            logger.verbose('Doing update_last_msg_read_ts: %r', spec)

            fdbw.retried_sync(fdbw.users.update,
                              spec=spec,
                              document={'$set': {'last_msg_read_ts': ts}},
                              safe=True)


        @classmethod
        def _spec_for_update_last_msg_sync_ts(cls,
                                              username, host_uuid, ts):
            r"""Generate a MongoDB spec for C{.update_last_msg_sync_ts()}
            query below.

            >>> FDBQueries.Users._spec_for_update_last_msg_sync_ts(
            ...     username='johndoe',
            ...     host_uuid=UUID(int=0x42),
            ...     ts=datetime(2012, 1, 3, 0, 0)
            ... )  # doctest:+NORMALIZE_WHITESPACE,+ELLIPSIS
            {'hosts':
                {'$elemMatch':
                    {'last_msg_sync_ts':
                        {'$lt': datetime.datetime(2012, 1, 3, 0, 0)},
                    'uuid':
                        UUID('00000000-0000-0000-0000-000000000042')}},
            'name_uc': 'JOHNDOE'}

            @type username: basestring
            @type host_uuid: HostUUID
            @type ts: datetime
            """
            # Host-specific conditions
            host_spec = {
                'uuid': host_uuid,
                'last_msg_sync_ts': {'$lt': ts}
            }

            # Whole-user conditions
            return {
                'name_uc': username.upper(),
                'hosts': {'$elemMatch': host_spec}
            }


        @classmethod
        @contract_epydoc
        def update_last_msg_sync_ts(cls, username, host_uuid, ts, fdbw):
            """
            Update the "last message synced to the host" timestamp,
            but only if it is newer than the existing value.

            @param username: the name of the user
            @type username: basestring

            @param host_uuid: the UUID of the host.
            @type host_uuid: PeerUUID

            @type ts: datetime

            @param fdbw: FastDB wrapper.
            @type fdbw: abstract_docstorewrapper.AbstractDocStoreWrapper
            """
            spec = cls._spec_for_update_last_msg_sync_ts(
                       username=username,
                       host_uuid=host_uuid,
                       ts=ts)
            logger.verbose('Doing update_last_msg_sync_ts: %r', spec)

            fdbw.retried_sync(fdbw.users.update,
                              spec=spec,
                              document={
                                  '$set': {'hosts.$.last_msg_sync_ts': ts}
                              },
                              safe=True)


        @classmethod
        @contract_epydoc
        def get_users_with_last_seen_times(cls, fdbw):
            """Get all users (their names) together with the last seen times.

            @param fdbw: FastDB wrapper.
            @type fdbw: abstract_docstorewrapper.AbstractDocStoreWrapper

            @return: the iterable over tuples of (username, last-seen-time).
                The last-seen-time is the maximum of last-seen-times of
                all the user's hosts, or C{None} if it was not ever seen.
                with username-hosts[last_seen],
                or C{None} if the users cannot be found.
            @rtype: col.Iterable
            """
            docs = fdbw.retried_sync(fdbw.users.find,
                                     fields=['name', 'hosts.last_seen'])
            for doc in docs:
                max_last_seen = max(h['last_seen'] for h in doc['hosts']) \
                                    if doc['hosts'] \
                                    else None
                yield (doc['name'], max_last_seen)



    class UserMessages(object):
        """
        The namespace for all the FastDB queries related to
        the messages between the Host and the Node.
        """

        __slots__ = ()

        collection_name = 'user_messages'

        indices = {
            ('username_uc', 'key'): [IndexOptions.UNIQUE],
            ('username_uc', 'ts'): {}
        }


        @classmethod
        @contract_epydoc
        def add_message(cls, username, key, body, fdbw):
            """
            @param username: the name of the user.
            @type username: basestring

            @param fdbw: FastDB wrapper.
            @type fdbw: abstract_docstorewrapper.AbstractDocStoreWrapper
            """
            message = models.UserMessage(username=username,
                                         key=key,
                                         body=body,
                                         ts=datetime.utcnow())
            fdbw.retried_sync(fdbw.user_messages.insert,
                              doc_or_docs=message.to_bson(),
                              safe=True)


        @classmethod
        def __add_msg(cls, msg, fdbw):
            """Sample code, use as reference only!

            @todo: remove at some point, probably after September 2012.

            @type msg: datatypes.UserMessage
            """
            raise NotImplementedError()
            assert isinstance(msg, datatypes.UserMessage)

            # 1
            # not possible now
            bsoned = models.UserMessage.to_bson(msg)

            # 2
            bsoned = models.UserMessage.from_common(msg).to_bson()

            # 3
            bsoned = models.UserMessage.from_json(msg.to_json())().to_bson()
            bsoned


        @classmethod
        def _spec_for_get_new_messages(cls, username, ts):
            r"""Generate a MongoDB spec for C{.get_new_messages()}
            query below.

            >>> FDBQueries.UserMessages._spec_for_get_new_messages(
            ...     username='johndoe',
            ...     ts=datetime(2012, 1, 3, 0, 0)
            ... )  # doctest:+NORMALIZE_WHITESPACE,+ELLIPSIS
            {'username_uc': 'JOHNDOE',
             'ts': {'$gt': datetime.datetime(2012, 1, 3, 0, 0)}}

            @type username: basestring
            @type ts: datetime
            """
            return {
                'username_uc': username.upper(),
                'ts': {'$gt': ts}
            }


        @classmethod
        def _fields_for_get_new_messages(cls, locales=None):
            r"""Generate a MongoDB "fields" argument for C{.get_new_messages()}
            query below.

            >>> FDBQueries.UserMessages._fields_for_get_new_messages(
            ...     locales=None
            ... )  # doctest:+NORMALIZE_WHITESPACE,+ELLIPSIS
            ['username_uc', 'key', 'ts', 'body']

            >>> FDBQueries.UserMessages._fields_for_get_new_messages(
            ...     locales=[]
            ... )  # doctest:+NORMALIZE_WHITESPACE,+ELLIPSIS
            ['username_uc', 'key', 'ts']

            >>> FDBQueries.UserMessages._fields_for_get_new_messages(
            ...     locales=['c']
            ... )  # doctest:+NORMALIZE_WHITESPACE,+ELLIPSIS
            ['username_uc', 'key', 'ts', 'body.c']

            >>> # In fact, this is unlikely to happen, as all the locales IRL
            >>> # will be sorted from the narrowest to the widest,
            >>> # ending by 'c'.
            >>> FDBQueries.UserMessages._fields_for_get_new_messages(
            ...     locales=['en_us']
            ... )  # doctest:+NORMALIZE_WHITESPACE,+ELLIPSIS
            ['username_uc', 'key', 'ts', 'body.en_us']

            >>> FDBQueries.UserMessages._fields_for_get_new_messages(
            ...     locales=['ru_ru', 'ru', 'c']
            ... )  # doctest:+NORMALIZE_WHITESPACE,+ELLIPSIS
            ['username_uc', 'key', 'ts', 'body.ru_ru', 'body.ru', 'body.c']

            >>> # The BCP47 locales given up to the codeset are supported,
            >>> # though unlikely to happen, because they will be truncated
            >>> # to the C{language_territory} or the C{language}
            >>> # on earlier steps.
            >>> FDBQueries.UserMessages._fields_for_get_new_messages(
            ...     locales=['ru_ru.utf-8', 'ru_ru', 'ru', 'c']
            ... )  # doctest:+NORMALIZE_WHITESPACE,+ELLIPSIS
            ['username_uc', 'key', 'ts',
             'body.ru_ru.utf-8', 'body.ru_ru', 'body.ru', 'body.c']

            @type locales: NoneType, col.Iterable
            """
            return ['username_uc', 'key', 'ts'] \
                   + (['body.{}'.format(l) for l in locales]
                          if locales is not None
                          else ['body'])

        @classmethod
        def get_new_messages(cls, username, ts, locales=None, fdbw=None):
            """
            @param locales: the (possibly, non-reiterable) iterable
                to filter the locales which should be taken.
                If C{None}, no filtering is performed.
                The locales to filter (if present) must be in lowercase-only,
                because it is assumed that the locales in the FastDB
                are stored in lowercase-only.
            @type locales: NoneType, col.Iterable

            @returns: the (possibly, non-reiterable) iterable over
                C{datatypes.UserMessage} objects.
            """
            _locales = list(locales) if locales is not None else None
            assert _locales is None or all(l.lower() == l for l in _locales), \
                   repr(_locales)

            spec = cls._spec_for_get_new_messages(username=username, ts=ts)
            fields = cls._fields_for_get_new_messages(locales=_locales)
            logger.verbose('Doing get_new_messages: %r, with %r', spec, fields)

            messages = fdbw.retried_sync(fdbw.user_messages.find,
                                         spec=spec,
                                         fields=fields)
            return (models.UserMessage.from_bson(m)() for m in messages)



FDB_COLLECTIONS = [
    FDBQueries.AuthTokens,
    FDBQueries.HealChunkRequests,
    FDBQueries.MagnetLinks,
    FDBQueries.RestoreRequests,
    FDBQueries.Transactions,
    FDBQueries.Uploads,
    FDBQueries.Users,
    FDBQueries.UserMessages,
]

FDB_INDICES_PER_COLLECTION = \
    {c.collection_name: c.indices for c in FDB_COLLECTIONS}
