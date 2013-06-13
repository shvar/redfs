#!/usr/bin/python
"""
Various versions of classes containing the whole dataset.
"""

#
# Imports
#

from __future__ import absolute_import
import collections as col
import fnmatch
import logging
import numbers
import operator
import os
import re
import stat
import sys
from collections import namedtuple
from datetime import datetime
from functools import partial
from itertools import chain, imap, tee
from types import NoneType
from uuid import UUID

from contrib.dbc import contract_epydoc, consists_of

from . import limits, os_ex
from .abstract_dataset import AbstractDataset, AbstractDatasetWithChunks
from .chunks import (
    Chunk, ChunkFromFiles, LocalVirtualFileState, FILTERS_TO_WILDCARDS)
from .crypto import Cryptographer, co_crc32, co_fingerprint
from .itertools_ex import filterfalse, repr_long_sized_iterable
from .os_ex import fake_stat, stat_get_mtime, stat_isfile, stat_isdir
from .path_ex import decode_posix_path, encode_posix_path
from .typed_uuids import DatasetUUID, UserGroupUUID
from .utils import coalesce, on_exception
from .vfs import VirtualFile, RelVirtualFile


#
# Constants
#

logger = logging.getLogger(__name__)
logger_fp_file = logging.getLogger('status.fp_calc')



#
# Classes
#

class DatasetWithDirectories(AbstractDataset):
    """
    A dataset class which includes the directories which it is bound to.
    """

    __slots__ = ('directories',)


    @contract_epydoc
    def __init__(self, directories, *args, **kwargs):
        """Constructor.

        @param directories: the mapping of base backup directories
                            to the appropriate files.
        @type directories: dict
        """
        super(DatasetWithDirectories, self).__init__(*args, **kwargs)
        self.directories = directories


    def __str__(self):
        # '{self.directories!r}'.format() may be too long, don't use!
        return u'{super}, directories={dirs}'.format(
                   super=super(DatasetWithDirectories, self).__str__(),
                   self=self,
                   dirs=u'{{{}}}'.format(
                            ', '.join(u'{!r}: ({}, {})'
                                              .format(dir_name,
                                                      repr_long_sized_iterable(
                                                          small_files),
                                                      repr_long_sized_iterable(
                                                          large_files))
                                          for dir_name, (small_files,
                                                         large_files)
                                              in self.directories
                                                     .iteritems())))


    @classmethod
    def _directory_to_json(cls, dir_items):
        return {'files': [i.to_json() for i in dir_items]}


    @classmethod
    def _directory_from_json(cls, jsoned_dir):
        """
        @returns: The directory contents.
        @rtype: tuple
        """
        return ([LocalVirtualFileState.from_json(f)(file_getter=None)
                     for f in jsoned_dir['files']],
                [])


    def to_json(self):
        cls = self.__class__

        res = super(DatasetWithDirectories, self).to_json()
        res.update({
            'directories':
                {encode_posix_path(path):
                         cls._directory_to_json(chain.from_iterable(dir_items))
                     for path, dir_items in self.directories.iteritems()},
        })
        return res


    @classmethod
    def from_json(cls, data):
        return partial(super(DatasetWithDirectories, cls).from_json(data),
                       directories={decode_posix_path(path):
                                            cls._directory_from_json(data_dir)
                                        for path, data_dir
                                            in data['directories']
                                                   .iteritems()})



class AbstractDatasetFromDecryptedData(DatasetWithDirectories,
                                       AbstractDatasetWithChunks):
    """
    Any implementation of a dataset which is created from the orignal provided
    (decrypted) data, no matter if it is based on the files on the local
    filesystem, or on some virtual contents.
    """

    __slots__ = tuple()


    def chunks(self):
        """Implement C{AbstractDatasetWithChunks} interface.

        @return: the iterator over the chunks in this dataset.
        @rtype: col.Iterable
        """
        cls = self.__class__

        # Order the chunk sets by the top directory name.
        for k, (small_files, large_files) \
            in sorted(self.directories.iteritems(),
                      key=operator.itemgetter(0)):

            # Small files cause a single chunk substream
            for chunk in cls.__chunkstream_for_files(small_files):
                yield chunk

            # Large files cause a separate chunk substream for each file
            for f in large_files:
                for chunk in cls.__chunkstream_for_files([f]):
                    yield chunk


    def size(self):
        """The total size of all the chunks.

        @rtype: numbers.Integral
        """
        sum_over_chunks = super(AbstractDatasetFromDecryptedData, self).size()
        # The sum over chunks is the same as the sum over files
        assert sum_over_chunks == \
               sum(file.size
                       for directory in self.directories.itervalues()
                       for stream in directory
                       for file in stream), \
               repr(self.directories)

        return sum_over_chunks


    @classmethod
    def __chunkstream_for_files(cls, files):
        """Create a data stream from the sequence of files.

        @param files: the Iterable of file to generate a new chunk
            stream for.
        @type files: col.Iterable

        @return: iterator over the chunk stream, yielding its chunks
            (as C{ChunkFromFiles} objects).
        """
        file_list = list(files)

        total_size = sum(f.size for f in file_list if f.size is not None)
        chunk_size = Chunk.max_chunk_size_code_for_total_size(total_size)

        # Start chunk stream with a brand new chunk
        current_chunk = ChunkFromFiles(maxsize_code=chunk_size)

        for file in file_list:
            bytes_put_already = 0
            bytes_yet_to_put = file.size
            # Invariant
            assert (file.size is None or
                    bytes_yet_to_put + bytes_put_already == file.size), \
                   (file, file.size, bytes_yet_to_put, bytes_put_already)

            while bytes_yet_to_put > 0:
                # How much of this file should we put into the chunk?
                free = current_chunk.free_space
                bytes_putting = min(free, bytes_yet_to_put)

                # Append file to the chunk...
                current_chunk.append_file(offset_in_file=bytes_put_already,
                                          how_many=bytes_putting,
                                          file=file)

                bytes_yet_to_put -= bytes_putting
                bytes_put_already += bytes_putting
                # Invariant
                assert (file.size is None or
                        bytes_yet_to_put + bytes_put_already == file.size), \
                       (file, file.size, bytes_yet_to_put, bytes_put_already)

                # Current chunk is filled up; yield it and create a next chunk.
                assert current_chunk.free_space >= 0, \
                       u'The {} size fell below 0!'.format(current_chunk)
                if current_chunk.free_space == 0:
                    yield current_chunk
                    current_chunk = ChunkFromFiles(maxsize_code=chunk_size)

        # We've completed the loop over the chunks.
        # But do we need to yield the last chunk?
        # Was it just created on the last iteration, or was it
        if current_chunk.free_space != current_chunk.max_size:
            # It is not empty, so should be yielded too
            yield current_chunk


    @classmethod
    def __merge_filters_to_regexes(cls, filters):
        r"""
        Given a sequence of filters, map them to the wildcards
        and then generate a combined regex matching
        any of these wildcards.

        >>> clmethod = AbstractDatasetFromDecryptedData \
        ...        ._AbstractDatasetFromDecryptedData__merge_filters_to_regexes

        >>> # Smoke test
        >>> clmethod([]), clmethod([]).pattern  # doctest:+ELLIPSIS
        (<_sre.SRE_Pattern object at 0x...>, u'(?:)\\Z')

        >>> res = clmethod(['video'])
        >>> res, res.pattern  # doctest:+ELLIPSIS,+NORMALIZE_WHITESPACE
        (<_sre.SRE_Pattern object at 0x...>,
         u'(?:(?:.*\\.avi\\Z(?ms))|...|(?:.*\\.mov\\Z(?ms)))\\Z')

        >>> res = clmethod(['doc', 'audio'])
        >>> res, res.pattern  # doctest:+ELLIPSIS,+NORMALIZE_WHITESPACE
        (<_sre.SRE_Pattern object at 0x...>,
         u'(?:(?:.*\\.ppt\\Z(?ms))|...|(?:.*\\.aac\\Z(?ms)))\\Z')

        >>> res = clmethod(['all'])
        >>> res, res.pattern  # doctest:+ELLIPSIS,+NORMALIZE_WHITESPACE
        (<_sre.SRE_Pattern object at 0x...>,
         u'(?:(?:.*\\Z(?ms)))\\Z')

        @param filters: a sequence of filter names (see C{FILTERS_TO_WILDCARDS}
            to understand what filter names can be used).
        @type filters: col.Iterable

        @return: compiled RE.
        """
        re_inner = '|'.join(u'(?:{})'.format(fnmatch.translate(p))
                                for f in filters
                                for p in FILTERS_TO_WILDCARDS[f])
        re_str = u'(?:{})\\Z'.format(re_inner)
        return re.compile(re_str)


    @classmethod
    def _fix_settings_for_selected_path(cls, path_settings):
        r"""
        Given the path settings dictionary,
        convert its filter sets (inclusive/exclusive)
        to the appropriate regexes.

        >>> from common.utils import ts_to_dt
        >>> dt = ts_to_dt(1326821123)
        >>> cls = AbstractDatasetFromDecryptedData

        >>> dir_stat = os_ex.fake_stat(st_mode=16877, nlink=3, size=4096,
        ...                            atime=dt, mtime=dt, ctime=dt)
        >>> file_stat = os_ex.fake_stat(st_mode=33261, nlink=1, size=142,
        ...                             atime=dt, mtime=dt, ctime=dt)
        >>> no_stat = None
        >>> # Smoke test.
        >>> # Should fail, since 'stat' is mandatory
        >>> cls._fix_settings_for_selected_path({})
        Traceback (most recent call last):
          ...
        KeyError: 'stat'
        >>> # Minimal working configuration.
        >>> cls._fix_settings_for_selected_path({'stat': no_stat})
        {'stat': None}

        >>> # Another good configuration
        >>> cls._fix_settings_for_selected_path({
        ...     'stat': file_stat})  # doctest:+NORMALIZE_WHITESPACE
        {'stat': FakeStatResult(st_mode=33261, st_nlink=1, st_size=142,
                                st_atime=1326821123.0,
                                st_mtime=1326821123.0,
                                st_ctime=1326821123.0)}

        >>> # Huge configuration with ifiles

        >>> dt_f1 = datetime(2012, 11, 5, 12, 12, 41, 728425)
        >>> dt_f2 = datetime(2012, 11, 5, 12, 12, 41, 904430)
        >>> dt_f3 = datetime(2012, 11, 5, 12, 12, 41, 988433)
        >>> dt_bbb = datetime(2012, 10, 11, 15, 33, 42, 19808)
        >>> dt_ccc = datetime(2012, 10, 11, 15, 33, 41, 979807)
        >>> stat_f1 = os_ex.fake_stat(
        ...     st_mode=33261, size=12232704,
        ...     atime=dt_f1, mtime=dt_f1, ctime=dt_f1
        ... )
        >>> stat_f2 = os_ex.fake_stat(
        ...     st_mode=33261, size=13829879,
        ...     atime=dt_f2, mtime=dt_f2, ctime=dt_f2
        ... )
        >>> stat_f3 = os_ex.fake_stat(
        ...     st_mode=33261, size=3522710,
        ...     atime=dt_f3, mtime=dt_f3, ctime=dt_f3
        ... )
        >>> stat_bbb = os_ex.fake_stat(
        ...     st_mode=33261, size=4,
        ...     atime=dt_bbb, mtime=dt_bbb, ctime=dt_bbb
        ... )
        >>> stat_ccc = os_ex.fake_stat(
        ...     st_mode=33261, size=4,
        ...     atime=dt_ccc, mtime=dt_ccc, ctime=dt_ccc
        ... )
        >>> _dummy_stat_getter = lambda: None

        >>> ifiles = [
        ...     [
        ...         RelVirtualFile(rel_dir='',
        ...                        filename=u'f1.mp3',
        ...                        stat=stat_f1,
        ...                        stat_getter=_dummy_stat_getter),
        ...         RelVirtualFile(rel_dir='',
        ...                        filename=u'f2.mp3',
        ...                        stat=stat_f2,
        ...                        stat_getter=_dummy_stat_getter),
        ...         RelVirtualFile(rel_dir='',
        ...                        filename=u'f3.mp3',
        ...                        stat=stat_f3,
        ...                        stat_getter=_dummy_stat_getter)
        ...     ],
        ...     [
        ...         RelVirtualFile(rel_dir='a/b',
        ...                        filename=u'bbb',
        ...                        stat=stat_bbb,
        ...                        stat_getter=_dummy_stat_getter)],
        ...     [
        ...         RelVirtualFile(rel_dir='a/b/c',
        ...                        filename=u'ccc',
        ...                        stat=stat_ccc,
        ...                        stat_getter=_dummy_stat_getter)
        ...     ]
        ... ]

        >>> res = cls._fix_settings_for_selected_path({'f+': ['all'],
        ...                                            'stat': dir_stat})
        >>> res  # doctest:+ELLIPSIS,+NORMALIZE_WHITESPACE
        {'stat': FakeStatResult(st_mode=16877, st_nlink=3, st_size=4096,
                                st_atime=1326821123.0,
                                st_mtime=1326821123.0,
                                st_ctime=1326821123.0),
         'r+': <_sre.SRE_Pattern object at 0x...>}
        >>> res['r+'].pattern
        u'(?:(?:.*\\Z(?ms)))\\Z'

        >>> # Test 1 with iters
        >>> res1 = cls._fix_settings_for_selected_path({
        ...     'f-': ['video'],
        ...     'ifiles': imap(iter, ifiles),
        ...     'stat': dir_stat})
        >>> res1  # doctest:+ELLIPSIS,+NORMALIZE_WHITESPACE
        {'ifiles': <itertools.imap object at 0x...>,
         'r-': <_sre.SRE_Pattern object at 0x...>,
         'stat': FakeStatResult(st_mode=16877, st_nlink=3, st_size=4096,
                                st_atime=1326821123.0,
                                st_mtime=1326821123.0,
                                st_ctime=1326821123.0)}
        >>> map(list, res1['ifiles']) # doctest:+ELLIPSIS,+NORMALIZE_WHITESPACE
        [[RelVirtualFile(rel_dir='',
                         filename=u'f1.mp3',
                         stat=FakeStatResult(st_mode=33261, st_nlink=1,
                                             st_size=12232704,
                                             st_atime=1352117561.728425,
                                             st_mtime=1352117561.728425,
                                             st_ctime=1352117561.728425)),
          RelVirtualFile(rel_dir='',
                         filename=u'f2.mp3',
                         stat=FakeStatResult(st_mode=33261, st_nlink=1,
                                             st_size=13829879,
                                             st_atime=1352117561.90443,
                                             st_mtime=1352117561.90443,
                                             st_ctime=1352117561.90443)),
          RelVirtualFile(rel_dir='',
                         filename=u'f3.mp3',
                         stat=FakeStatResult(st_mode=33261, st_nlink=1,
                                             st_size=3522710,
                                             st_atime=1352117561.988433,
                                             st_mtime=1352117561.988433,
                                             st_ctime=1352117561.988433))],
         [RelVirtualFile(rel_dir='a/b',
                         filename=u'bbb',
                         stat=FakeStatResult(st_mode=33261, st_nlink=1,
                                             st_size=4,
                                             st_atime=1349969622.019808,
                                             st_mtime=1349969622.019808,
                                             st_ctime=1349969622.019808))],
         [RelVirtualFile(rel_dir='a/b/c',
                         filename=u'ccc',
                         stat=FakeStatResult(st_mode=33261, st_nlink=1,
                                             st_size=4,
                                             st_atime=1349969621.979807,
                                             st_mtime=1349969621.979807,
                                             st_ctime=1349969621.979807))]]
        >>> res1['r-'].pattern  # doctest:+ELLIPSIS,+NORMALIZE_WHITESPACE
        u'(?:(?:.*\\.avi\\Z(?ms))|...|(?:.*\\.mov\\Z(?ms)))\\Z'

        >>> # Test 2 with iters
        >>> res2 = cls._fix_settings_for_selected_path({
        ...     'f+': ['doc', 'audio'],
        ...     'f-': ['video'],
        ...     'ifiles': imap(iter, ifiles),
        ...     'stat': dir_stat})
        >>> res2  # doctest:+ELLIPSIS,+NORMALIZE_WHITESPACE
        {'ifiles': <itertools.imap object at 0x...>,
         'r-': <_sre.SRE_Pattern object at 0x...>,
         'stat': FakeStatResult(st_mode=16877, st_nlink=3, st_size=4096,
                                st_atime=1326821123.0,
                                st_mtime=1326821123.0,
                                st_ctime=1326821123.0),
         'r+': <_sre.SRE_Pattern object at 0x...>}
        >>> map(list, res2['ifiles']) # doctest:+ELLIPSIS,+NORMALIZE_WHITESPACE
        [[RelVirtualFile(rel_dir='',
                         filename=u'f1.mp3',
                         stat=FakeStatResult(st_mode=33261, st_nlink=1,
                                             st_size=12232704,
                                             st_atime=1352117561.728425,
                                             st_mtime=1352117561.728425,
                                             st_ctime=1352117561.728425)),
          RelVirtualFile(rel_dir='',
                         filename=u'f2.mp3',
                         stat=FakeStatResult(st_mode=33261, st_nlink=1,
                                             st_size=13829879,
                                             st_atime=1352117561.90443,
                                             st_mtime=1352117561.90443,
                                             st_ctime=1352117561.90443)),
          RelVirtualFile(rel_dir='',
                         filename=u'f3.mp3',
                         stat=FakeStatResult(st_mode=33261, st_nlink=1,
                                             st_size=3522710,
                                             st_atime=1352117561.988433,
                                             st_mtime=1352117561.988433,
                                             st_ctime=1352117561.988433))],
         [RelVirtualFile(rel_dir='a/b',
                         filename=u'bbb',
                         stat=FakeStatResult(st_mode=33261, st_nlink=1,
                                             st_size=4,
                                             st_atime=1349969622.019808,
                                             st_mtime=1349969622.019808,
                                             st_ctime=1349969622.019808))],
         [RelVirtualFile(rel_dir='a/b/c',
                         filename=u'ccc',
                         stat=FakeStatResult(st_mode=33261, st_nlink=1,
                                             st_size=4,
                                             st_atime=1349969621.979807,
                                             st_mtime=1349969621.979807,
                                             st_ctime=1349969621.979807))]]
        >>> res2['r+'].pattern  # doctest:+ELLIPSIS
        u'(?:(?:.*\\.ppt\\Z(?ms))|...|(?:.*\\.aac\\Z(?ms)))\\Z'
        >>> res2['r-'].pattern  # doctest:+ELLIPSIS
        u'(?:(?:.*\\.avi\\Z(?ms))|...|(?:.*\\.mov\\Z(?ms)))\\Z'

        @type path_settings: dict
        @rtype: dict
        """
        # Mandatory fields are filled immediately
        res = {'stat': path_settings['stat']}

        if 'f+' in path_settings:
            res['r+'] = cls.__merge_filters_to_regexes(path_settings['f+'])
        if 'f-' in path_settings:
            res['r-'] = cls.__merge_filters_to_regexes(path_settings['f-'])
        if 'ifiles' in path_settings:
            res['ifiles'] = path_settings['ifiles']
        return res



class DatasetOnVirtualFiles(AbstractDatasetFromDecryptedData):
    """
    The dataset structure over the real file in local directories.
    """

    __slots__ = tuple()


    ArrangeFilesResult = \
        namedtuple('ArrangeFilesResult',
                   ('small_files', 'large_files', 'current_files_count'))

    _ToAppend = \
        namedtuple('_ToAppend', ('vfile', 'sort_key', 'fp', 'crc32'))

    @classmethod
    @contract_epydoc
    def _arrange_directory_files_for_backup(
            cls,
            base_dir, vfiles, current_files_count, total_files_count,
            ds_uuid, cryptographer, ospath):
        r"""
        Arrange an iterable of files (from the same directory) into small/large
        file streams, according to their fingerprints.

        >>> cls = DatasetOnVirtualFiles
        >>> f = cls._arrange_directory_files_for_backup

        >>> from common.logger import add_verbose_level; add_verbose_level()

        >>> import posixpath
        >>> from contextlib import closing
        >>> from cStringIO import StringIO
        >>> from common.crypto import AES_KEY_SIZE

        >>> dt_f1 = datetime(2012, 11, 5, 12, 12, 41, 728425)
        >>> dt_f2 = datetime(2012, 11, 5, 12, 12, 41, 904430)
        >>> dt_f3 = datetime(2012, 11, 5, 12, 12, 41, 988433)
        >>> dt_f4 = datetime(2012, 11, 6, 12, 12, 41, 728425)
        >>> dt_f5 = datetime(2012, 11, 6, 12, 12, 41, 904430)
        >>> dt_f6 = datetime(2012, 11, 6, 12, 12, 41, 988433)
        >>> dt_f7 = datetime(2012, 11, 7, 12, 12, 41, 728425)
        >>> dt_f8 = datetime(2012, 11, 7, 12, 12, 41, 904430)
        >>> dt_f9 = datetime(2012, 11, 7, 12, 12, 41, 988433)

        >>> c1 = '1' * 10
        >>> c2 = '2' * 100
        >>> c3 = '3' * 1000000
        >>> c4 = '4' * 0xfffff  # almost large enough to be a "large file"
        >>> c5 = '5' * 0x100000  # the first "large file"
        >>> c6 = '6' * 0x300000
        >>> c7 = '7' * 0x300000
        >>> c8 = '8' * 0x300000
        >>> c9 = '9' * 0x400000

        >>> st_f1 = os_ex.fake_stat(st_mode=33261, size=10,
        ...                         atime=dt_f1, mtime=dt_f1, ctime=dt_f1)
        >>> st_f2 = os_ex.fake_stat(st_mode=33261, size=100,
        ...                         atime=dt_f2, mtime=dt_f2, ctime=dt_f2)
        >>> st_f3 = os_ex.fake_stat(st_mode=33261, size=1000000,
        ...                         atime=dt_f3, mtime=dt_f3, ctime=dt_f3)
        >>> st_f4 = os_ex.fake_stat(st_mode=33261, size=0xfffff,
        ...                         atime=dt_f4, mtime=dt_f4, ctime=dt_f4)
        >>> st_f5 = os_ex.fake_stat(st_mode=33261, size=0x100000,
        ...                         atime=dt_f5, mtime=dt_f5, ctime=dt_f5)
        >>> st_f6 = os_ex.fake_stat(st_mode=33261, size=0x300000,
        ...                         atime=dt_f6, mtime=dt_f6, ctime=dt_f6)
        >>> st_f7 = os_ex.fake_stat(st_mode=33261, size=0x300000,
        ...                         atime=dt_f7, mtime=dt_f7, ctime=dt_f7)
        >>> st_f8 = os_ex.fake_stat(st_mode=33261, size=0x300000,
        ...                         atime=dt_f8, mtime=dt_f8, ctime=dt_f8)
        >>> st_f9 = os_ex.fake_stat(st_mode=33261, size=0x400000,
        ...                         atime=dt_f9, mtime=dt_f9, ctime=dt_f9)
        >>> # Also stat for some, kind of, "deleted" files
        >>> st_f1d = os_ex.fake_stat(st_mode=None, size=None,
        ...                          atime=dt_f1, mtime=dt_f1, ctime=dt_f1)
        >>> st_f2d = os_ex.fake_stat(st_mode=None, size=None,
        ...                          atime=dt_f2, mtime=dt_f2, ctime=dt_f2)
        >>> st_f3d = os_ex.fake_stat(st_mode=None, size=None,
        ...                          atime=dt_f3, mtime=dt_f3, ctime=dt_f3)

        >>> f1 = RelVirtualFile(rel_dir='', filename=u'f1.mp3', stat=st_f1,
        ...                     stat_getter=lambda: st_f1,
        ...                     file_getter=lambda: closing(StringIO(c1)))
        >>> f2 = RelVirtualFile(rel_dir='', filename=u'f2.mp3', stat=st_f2,
        ...                     stat_getter=lambda: st_f2,
        ...                     file_getter=lambda: closing(StringIO(c2)))
        >>> f3 = RelVirtualFile(rel_dir='', filename=u'f3.mp3', stat=st_f3,
        ...                     stat_getter=lambda: st_f3,
        ...                     file_getter=lambda: closing(StringIO(c3)))
        >>> f4 = RelVirtualFile(rel_dir='', filename=u'f4.mp3', stat=st_f4,
        ...                     stat_getter=lambda: st_f4,
        ...                     file_getter=lambda: closing(StringIO(c4)))
        >>> f5 = RelVirtualFile(rel_dir='', filename=u'f5.mp3', stat=st_f5,
        ...                     stat_getter=lambda: st_f5,
        ...                     file_getter=lambda: closing(StringIO(c5)))
        >>> f6 = RelVirtualFile(rel_dir='', filename=u'f6.mp3', stat=st_f6,
        ...                     stat_getter=lambda: st_f6,
        ...                     file_getter=lambda: closing(StringIO(c6)))
        >>> f7 = RelVirtualFile(rel_dir='', filename=u'f7.mp3', stat=st_f7,
        ...                     stat_getter=lambda: st_f7,
        ...                     file_getter=lambda: closing(StringIO(c7)))
        >>> f8 = RelVirtualFile(rel_dir='', filename=u'f8.mp3', stat=st_f8,
        ...                     stat_getter=lambda: st_f8,
        ...                     file_getter=lambda: closing(StringIO(c8)))
        >>> f9 = RelVirtualFile(rel_dir='', filename=u'f9.mp3', stat=st_f9,
        ...                     stat_getter=lambda: st_f9,
        ...                     file_getter=lambda: closing(StringIO(c9)))
        >>> # Also several, kind of, "deleted" ones
        >>> f1d = RelVirtualFile(rel_dir='', filename=u'f1.mp3', stat=st_f1d,
        ...                      stat_getter=lambda: None,
        ...                      file_getter=None)
        >>> f2d = RelVirtualFile(rel_dir='', filename=u'f2.mp3', stat=st_f2d,
        ...                      stat_getter=lambda: None,
        ...                      file_getter=None)
        >>> f3d = RelVirtualFile(rel_dir='', filename=u'f3.mp3', stat=st_f3d,
        ...                      stat_getter=lambda: None,
        ...                      file_getter=None)

        >>> cr = Cryptographer(''.join(chr(i) for i in xrange(AES_KEY_SIZE)))

        >>> # Now verify the small/large arrangement.

        >>> res = f(base_dir='/home/john/freebrie',
        ...         vfiles=[f1, f2, f3, f4, f5, f6, f7, f8, f9],
        ...         current_files_count=17, total_files_count=45,
        ...         ds_uuid=DatasetUUID('766796e96f08456d806ba24bc01f180c'),
        ...         cryptographer=cr, ospath=posixpath)

        >>> # Basic structure
        >>> res  # doctest:+ELLIPSIS,+NORMALIZE_WHITESPACE
        ArrangeFilesResult(small_files=<generator object <genexpr> at 0x...>,
                           large_files=<generator object <genexpr> at 0x...>,
                           current_files_count=26)

        >>> # Make it ready for reiteration
        >>> res1 = cls.ArrangeFilesResult(small_files=list(res.small_files),
        ...                               large_files=list(res.large_files),
        ...                               current_files_count=26)

        >>> # Small files?
        >>> list(res1.small_files)  # doctest:+ELLIPSIS,+NORMALIZE_WHITESPACE
        [LocalVirtualFileState(00000000000fffff12f9a...4cfa0f: 1048575 bytes,
             rel_path=u'f4.mp3', base_dir='/home/john/freebrie',
             time_changed=datetime.datetime(2012, 11, 6, 12, 12, 41, 728425)),
         LocalVirtualFileState(00000000000f4240640b8f...38f2f6: 1000000 bytes,
             rel_path=u'f3.mp3', base_dir='/home/john/freebrie',
             time_changed=datetime.datetime(2012, 11, 5, 12, 12, 41, 988433)),
         LocalVirtualFileState(0000000000000064ca2a25...e2cb4721c2: 100 bytes,
             rel_path=u'f2.mp3', base_dir='/home/john/freebrie',
             time_changed=datetime.datetime(2012, 11, 5, 12, 12, 41, 904430)),
         LocalVirtualFileState(000000000000000ae79a0e...7f47fc51c45: 10 bytes,
             rel_path=u'f1.mp3', base_dir='/home/john/freebrie',
             time_changed=datetime.datetime(2012, 11, 5, 12, 12, 41, 728425))]
        >>> # Large files?
        >>> list(res1.large_files)  # doctest:+ELLIPSIS,+NORMALIZE_WHITESPACE
        [LocalVirtualFileState(0000000000400000fc9f7...a6d49a: 4194304 bytes,
             rel_path=u'f9.mp3', base_dir='/home/john/freebrie',
             time_changed=datetime.datetime(2012, 11, 7, 12, 12, 41, 988433)),
         LocalVirtualFileState(00000000003000000f3899...e48b08: 3145728 bytes,
             rel_path=u'f6.mp3', base_dir='/home/john/freebrie',
             time_changed=datetime.datetime(2012, 11, 6, 12, 12, 41, 988433)),
         LocalVirtualFileState(0000000000300000235879...39e25c: 3145728 bytes,
             rel_path=u'f7.mp3', base_dir='/home/john/freebrie',
             time_changed=datetime.datetime(2012, 11, 7, 12, 12, 41, 728425)),
         LocalVirtualFileState(00000000003000003d46cd...773463: 3145728 bytes,
             rel_path=u'f8.mp3', base_dir='/home/john/freebrie',
             time_changed=datetime.datetime(2012, 11, 7, 12, 12, 41, 904430)),
         LocalVirtualFileState(0000000000100000e40aa4...b513ca: 1048576 bytes,
             rel_path=u'f5.mp3', base_dir='/home/john/freebrie',
             time_changed=datetime.datetime(2012, 11, 6, 12, 12, 41, 904430))]

        >>> # No matter of the order of original files,
        >>> # the small/large files arrangement should be the same.
        >>> res2 = f(base_dir='/home/john/freebrie',
        ...          vfiles=[f9, f7, f6, f1, f3, f2, f5, f4, f8],
        ...          current_files_count=17, total_files_count=45,
        ...          ds_uuid=DatasetUUID('766796e96f08456d806ba24bc01f180c'),
        ...          cryptographer=cr, ospath=posixpath)

        >>> res3 = f(base_dir='/home/john/freebrie',
        ...          vfiles=[f5, f2, f1, f8, f6, f9, f7, f4, f3],
        ...          current_files_count=17, total_files_count=45,
        ...          ds_uuid=DatasetUUID('766796e96f08456d806ba24bc01f180c'),
        ...          cryptographer=cr, ospath=posixpath)

        >>> res4 = f(base_dir='/home/john/freebrie',
        ...          vfiles=[f4, f6, f2, f5, f1, f8, f3, f7, f9],
        ...          current_files_count=17, total_files_count=45,
        ...          ds_uuid=DatasetUUID('766796e96f08456d806ba24bc01f180c'),
        ...          cryptographer=cr, ospath=posixpath)

        >>> # Actual verification of the file order invariance.
        >>> (list(res1.small_files) == list(res2.small_files)
        ...                         == list(res3.small_files)
        ...                         == list(res4.small_files))
        True
        >>> (list(res1.large_files) == list(res2.large_files)
        ...                         == list(res3.large_files)
        ...                         == list(res4.large_files))
        True

        >>> # And the jewel of the crown, the directory
        >>> # with several files deleted.
        >>> res1d = f(base_dir='/home/john/freebrie',
        ...           vfiles=[f1d, f2d, f3d, f4, f5, f6, f7, f8, f9],
        ...           current_files_count=17, total_files_count=45,
        ...           ds_uuid=DatasetUUID('766796e96f08456d806ba24bc01f180c'),
        ...           cryptographer=cr, ospath=posixpath)
        >>> # Make it ready for reiteration
        >>> res1d = cls.ArrangeFilesResult(
        ...             small_files=list(res1d.small_files),
        ...             large_files=list(res1d.large_files),
        ...             current_files_count=26)

        >>> res2d = f(base_dir='/home/john/freebrie',
        ...           vfiles=[f9, f7, f6, f1d, f3d, f2d, f5, f4, f8],
        ...           current_files_count=17, total_files_count=45,
        ...           ds_uuid=DatasetUUID('766796e96f08456d806ba24bc01f180c'),
        ...           cryptographer=cr, ospath=posixpath)

        >>> res3d = f(base_dir='/home/john/freebrie',
        ...           vfiles=[f5, f2d, f1d, f8, f6, f9, f7, f4, f3d],
        ...           current_files_count=17, total_files_count=45,
        ...           ds_uuid=DatasetUUID('766796e96f08456d806ba24bc01f180c'),
        ...           cryptographer=cr, ospath=posixpath)

        >>> res4d = f(base_dir='/home/john/freebrie',
        ...           vfiles=[f4, f6, f2d, f5, f1d, f8, f3d, f7, f9],
        ...           current_files_count=17, total_files_count=45,
        ...           ds_uuid=DatasetUUID('766796e96f08456d806ba24bc01f180c'),
        ...           cryptographer=cr, ospath=posixpath)

        >>> # [1/2] The order?...
        >>> # Interesting fact: the order of deleted files is guaranteed
        >>> # not upon FP, but upon the time of the file.
        >>> list(res1d.small_files)  # doctest:+ELLIPSIS,+NORMALIZE_WHITESPACE
        [LocalVirtualFileState(00000000000fffff12f...fd94cfa0f: 1048575 bytes,
             rel_path=u'f4.mp3', base_dir='/home/john/freebrie',
             time_changed=datetime.datetime(2012, 11, 6, 12, 12, 41, 728425)),
         LocalVirtualFileState(None bytes,
             rel_path=u'f3.mp3', base_dir='/home/john/freebrie',
             time_changed=datetime.datetime(2012, 11, 5, 12, 12, 41, 988433)),
         LocalVirtualFileState(None bytes,
             rel_path=u'f2.mp3', base_dir='/home/john/freebrie',
             time_changed=datetime.datetime(2012, 11, 5, 12, 12, 41, 904430)),
         LocalVirtualFileState(None bytes,
             rel_path=u'f1.mp3', base_dir='/home/john/freebrie',
             time_changed=datetime.datetime(2012, 11, 5, 12, 12, 41, 728425))]
        >>> list(res1d.large_files)  # doctest:+ELLIPSIS,+NORMALIZE_WHITESPACE
        [LocalVirtualFileState(0000000000400000fc9...aaca6d49a: 4194304 bytes,
             rel_path=u'f9.mp3', base_dir='/home/john/freebrie',
             time_changed=datetime.datetime(2012, 11, 7, 12, 12, 41, 988433)),
         LocalVirtualFileState(00000000003000000f3...4a5e48b08: 3145728 bytes,
             rel_path=u'f6.mp3', base_dir='/home/john/freebrie',
             time_changed=datetime.datetime(2012, 11, 6, 12, 12, 41, 988433)),
         LocalVirtualFileState(0000000000300000235...59b39e25c: 3145728 bytes,
             rel_path=u'f7.mp3', base_dir='/home/john/freebrie',
             time_changed=datetime.datetime(2012, 11, 7, 12, 12, 41, 728425)),
         LocalVirtualFileState(00000000003000003d4...982773463: 3145728 bytes,
             rel_path=u'f8.mp3', base_dir='/home/john/freebrie',
             time_changed=datetime.datetime(2012, 11, 7, 12, 12, 41, 904430)),
         LocalVirtualFileState(0000000000100000e40...edab513ca: 1048576 bytes,
             rel_path=u'f5.mp3', base_dir='/home/john/freebrie',
             time_changed=datetime.datetime(2012, 11, 6, 12, 12, 41, 904430))]

        >>> # [2/2] The invariance?...
        >>> # For deleted files, this works only due to they are sorted
        >>> # upon the .time_changed as well
        >>> (list(res1d.small_files) == list(res2d.small_files)
        ...                          == list(res3d.small_files)
        ...                          == list(res4d.small_files))
        True
        >>> (list(res1d.large_files) == list(res2d.large_files)
        ...                          == list(res3d.large_files)
        ...                          == list(res4d.large_files))
        True

        @param base_dir: the assumed base directory. May be empty.
        @type base_dir: basestring

        @param vfiles: the iterable of C{RelVirtualFile} objects.
        @type vfiles: col.Iterable

        @param current_files_count: the current number of files
            already arranged.
        @type current_files_count: numbers.Integral

        @param total_files_count: the current number of files to be arranged.
        @type total_files_count: numbers.Integral

        @param ds_uuid: dataset UUID; used for logging/progress measurement.
        @type ds_uuid: DatasetUUID

        @type cryptographer: Cryptographer

        @param ospath: the particular module implementing
            the C{os.path} interface.

        @return: a named tuple that contains:
            - "small files" and "large files" iterables; both iterables contain
            instances of C{LocalVirtualFileState};
            - the new value of total C{current_files_count} progress counter.
        @rtype: DatasetOnVirtualFiles.ArrangeFilesResult
        """
        # The files in the directory should be sorted
        # in reverse Fingerprint-comparison order.
        files_with_extra_data = []
        # Its items will be tuples of:
        # 1. vfile
        # 2. sort key
        # 3. fp
        # 4. crc32

        for vfile in vfiles:
            assert isinstance(vfile, RelVirtualFile), repr(vfile)

            current_files_count += 1

            full_path = ospath.join(base_dir, vfile.rel_path) \
                            if base_dir \
                            else vfile.rel_path

            to_append = None

            if vfile.is_deleted:
                logger.verbose('File %r was deleted', vfile)
                to_append = cls._ToAppend(vfile=vfile,
                                          sort_key=(None,
                                                    None,
                                                    full_path),
                                          fp=None, crc32=None)
            elif vfile.isdir:  # must be AFTER "if vfile.is_deleted".
                logger.verbose('%r is a directory', vfile)
                to_append = cls._ToAppend(vfile=vfile,
                                          sort_key=(None,
                                                    vfile.time_changed,
                                                    full_path),
                                          fp=None, crc32=None)
            else:
                logger.verbose('Calculating CRC32/FP for %r', vfile)

                crc32_gen = co_crc32()
                crc32 = crc32_gen.next()
                fp_gen = co_fingerprint(cryptographer)
                fp_func = fp_gen.next()

                dt_begin = datetime.utcnow()

                try:
                    for bl in vfile.contents():
                        # Actually, crc32_func and fp_func
                        # are still the same,
                        # but assignments are left for clarity
                        crc32 = crc32_gen.send(bl)
                        fp_func = fp_gen.send(bl)
                        eof = not(len(bl))
                except IOError as e:
                    # We won't append this file
                    logger.warning(
                        'During open(%r), the problem occured: %s',
                        full_path, e)
                else:
                    fp = fp_func()

                    dt_end = datetime.utcnow()

                    logger.verbose(
                        'CRC32/FP for %r: (%s, %08x), took %s',
                        full_path, fp, crc32, dt_end - dt_begin)

                    # Ok, we've finally calculated the fingerprint
                    # for the file which was passed here;
                    # but does the file state still match
                    # the initial one?
                    # Should the file be added?

                    st = vfile.restat()

                    if st is None:
                        logger.warning('%r suddenly disappeared',
                                       full_path)
                    else:
                        # There are two options...
                        if ((
                             #... 1: time_changed is not None - we care
                             #       we care for the particular state
                             #       of the file.
                             vfile.time_changed is not None and
                             vfile.time_changed == stat_get_mtime(st) and
                             vfile.size == st.st_size) or
                            (
                             #... 2: time_changed is None - we need
                             #       the latest state of the file.
                             #       No other checks for now.
                             vfile.time_changed is None)):
                            # File is ok for backup.
                            to_append = cls._ToAppend(
                                            vfile=vfile,
                                            sort_key=(fp,
                                                      vfile.time_changed,
                                                      full_path),
                                            fp=fp, crc32=crc32)

            assert isinstance(to_append, (cls._ToAppend, NoneType)), \
                   repr(to_append)

            # Did we read the file, factually?
            if to_append is None:
                logger.debug("We didn't add %r", full_path)
            else:
                files_with_extra_data.append(to_append)
                logger_fp_file.info(
                    'Fingerprints calculated for %i file(s) of %i.',
                    current_files_count, total_files_count,
                    extra={'num': current_files_count,
                           'of': total_files_count,
                           'ds_uuid': ds_uuid})

        # end for vfile in files

        # As promised, sort them in reverse-Fingerprint order.
        # Note it does NOT ensure the order of deleted files.
        files_with_extra_data.sort(key=operator.attrgetter('sort_key'),
                                   reverse=True)

        small_files_to_append = \
            (LocalVirtualFileState(
                     file_getter=vfile.file_getter,
                     # LocalFileState-specific
                     time_changed=vfile.time_changed,
                     isdir=False if vfile.is_deleted else vfile.isdir,
                     # LocalFile-specific
                     base_dir=base_dir,
                     rel_path=vfile.rel_path,
                     # File-specific
                     crc32=crc32,
                     fingerprint=fp)
                 for vfile, _ignore, fp, crc32 in files_with_extra_data
                 if fp is None or fp.size < limits.SMALL_FILE_WATERMARK)
        # small_files_to_append will also contain deleted files
        # and directories.
        large_files_to_append = \
            (LocalVirtualFileState(
                     file_getter=vfile.file_getter,
                     # LocalFileState-specific
                     time_changed=vfile.time_changed,
                     isdir=False if vfile.is_deleted else vfile.isdir,
                     # LocalFile-specific
                     base_dir=base_dir,
                     rel_path=vfile.rel_path,
                     # File-specific
                     crc32=crc32,
                     fingerprint=fp)
                 for vfile, _ignore, fp, crc32 in files_with_extra_data
                 if fp is not None and fp.size >= limits.SMALL_FILE_WATERMARK)

        return cls.ArrangeFilesResult(
                   small_files=small_files_to_append,
                   large_files=large_files_to_append,
                   current_files_count=current_files_count)


    @classmethod
    def _ifiles_local_file_is_good(cls, base_dir, rel_vfile):
        """Predicate to check if a file matches the specification.

        @type rel_vfile: RelVirtualFile

        @returns: whether the file passed in the file iterator for backup
            creation matches its specification.
        @rtype: bool
        @todo: properly handle directories, misc non-standard files and the
            situation when the file turns into a directory, and vice versa.
        """
        return True


    @classmethod
    def _get_filesets_per_dirs(cls, dir_map):
        """Get the filesets for the given directories.

        @rtype: col.Mapping
        @return: a mapping from the base directory path to the iter-of-iters
            of C{RelVirtualFile}.
        """
        per_dir_filesets = {}

        # Scan the selected directories
        for base_dir, dir_settings in dir_map.iteritems():
            directory = os.path.abspath(base_dir)

            # There are two ways of scanning.
            # If there is "ifiles" present, we already have the files to be
            # used in the backup (let's just check they indeed exist and match
            # their size/last-change-time). Otherwise, we scan the directories
            # optionally using the filters.
            if 'ifiles' in dir_settings:
                # We have the iterator over the exact files to be backed up.
                # Now let's do the last-minute filtering to be sure that the
                # files being backed up match what we expect of them.
                _ifiles = dir_settings['ifiles']

                if 'r+' in dir_settings or 'r-' in dir_settings:
                    logger.warning('r+ and r- are redundant if ifiles '
                                       'present in dir_settings: %r',
                                   dir_settings)

                per_base_dir = ((rel_vfile
                                     for rel_vfile in gr
                                         if cls._ifiles_local_file_is_good(
                                                base_dir, rel_vfile))
                                    for gr in _ifiles)

            else:
                # Not supported for now.
                # Must be implemented in subclasses, if needed.
                per_base_dir = []

            # Unfortunately, the results are iterated multiple times;
            # we have to store the explicit list here for now.
            # Also, see file_list analysis below, it has the same issue.
            per_base_dir = map(list, per_base_dir)
            assert all(consists_of(l, RelVirtualFile)
                           for l in per_base_dir), \
                   repr(per_base_dir)

            per_dir_filesets[directory] = per_base_dir

        return per_dir_filesets


    @classmethod
    def _get_filesets_per_files(cls, files):
        """Get the filesets for the given independent files.

        @type files: col.Iterable

        @rtype: col.Mapping
        """
        # For now, should NOT be called on Virtual Files.
        # When needed, refactor proper parts of the implementation
        # from the C{DatasetOnPhysicalFiles}.
        return {}


    @classmethod
    @contract_epydoc
    def _arrange_files_for_backup(cls,
                                  dir_map, file_list, ds_uuid, cryptographer,
                                  ospath):
        r"""
        Arrange the files from multiple directories into
        the small/large streams.

        The order of files is defined in chunk generation specification.

        @param dir_map: the mapping of directories to use for chunk generation
                        to the directory-specific settings.
        @type dir_map: dict
        @precondition: consists_of(dir_map.iterkeys(), basestring)
        @precondition: all(os_ex.stat_isdir(v['stat'])
                               for v in dir_map.itervalues())

        @param file_list: the list of files to use for chunk generation.
        @type file_list: list
        @precondition: consists_of(file_list, basestring)
        @precondition: all(os_ex.stat_isfile(v['stat'])
                               for v in file_list)

        @precondition: dir_map or file_list  # "Dataset should be non-empty!"

        @param ds_uuid: dataset UUID; used for logging/progress measurement.
        @type ds_uuid: DatasetUUID

        @type cryptographer: Cryptographer

        @returns: the tuple of two chunk streams with the chunks
                  containing the files from the directory list,
                  in the order defined by the chunk generation specification;
                  first the "small file" stream, then the "large file" stream.
        @rtype: col.Mapping
        """
        result = {}

        # First, we find all the files to use for the chunk stream
        per_dir_filesets = cls._get_filesets_per_dirs(dir_map)

        per_dir_filesets.update(cls._get_filesets_per_files(file_list))

        # Total counts over all directories
        total_files_count = \
            sum(len(file_set)
                    for file_sets in per_dir_filesets.itervalues()
                    for file_set in file_sets)
        current_files_count = 0

        for base_dir, filesets in per_dir_filesets.iteritems():
            small_files, large_files = [], []
            # Each "filesets" is a list of lists of files;
            # the outer list groups the files by subdirectories.
            for vfiles in filesets:
                arrange_result = \
                    cls._arrange_directory_files_for_backup(
                        base_dir=base_dir,
                        vfiles=vfiles,
                        current_files_count=current_files_count,
                        total_files_count=total_files_count,
                        ds_uuid=ds_uuid,
                        cryptographer=cryptographer,
                        ospath=ospath)

                current_files_count = arrange_result.current_files_count
                small_files.extend(arrange_result.small_files)
                large_files.extend(arrange_result.large_files)

            result[base_dir] = (small_files, large_files)

        return result


    @classmethod
    @contract_epydoc
    def from_paths(cls,
                   ds_name, ds_uuid, ugroup_uuid, sync, paths_map,
                   time_started, cryptographer):
        r"""Given a list of paths (to either directories or files),
        and a dataset name, create a dataset for them.

        >>> from cStringIO import StringIO
        >>> from contextlib import closing
        >>> from common.crypto import AES_KEY_SIZE
        >>> from common.logger import add_verbose_level; add_verbose_level()

        >>> cr = Cryptographer(''.join(chr(i) for i in xrange(AES_KEY_SIZE)))

        >>> dt1 = datetime(2012, 11, 5, 12, 12, 41, 395211)

        >>> dir_stat1 = os_ex.fake_stat(
        ...     isdir=True,
        ...     nlink=3, size=4096, atime=dt1, mtime=dt1, ctime=dt1)
        >>> file_stat1 = os_ex.fake_stat(
        ...     isdir=False,
        ...     nlink=1, size=142, atime=dt1, mtime=dt1, ctime=dt1)
        >>> file_getter1 = lambda: closing(StringIO('X'*50))

        >>> files1 = [
        ...     [
        ...          RelVirtualFile(rel_dir='1/2',
        ...                         filename='music.mp3',
        ...                         stat=file_stat1,
        ...                         stat_getter=lambda: None,
        ...                         file_getter=file_getter1)
        ...     ]
        ... ]

        >>> pmap1 = {
        ...     u'/home/john/t1':
        ...         {'ifiles': imap(iter, files1),
        ...          'stat': dir_stat1}
        ... }

        >>> DatasetOnVirtualFiles.from_paths(
        ...     ds_name='ABC',
        ...     ds_uuid=UUID('766796e9-6f08-456d-806b-a24bc01f180c'),
        ...     ugroup_uuid=UUID('19ab45a1-ff4b-4e8a-a38e-c3771b7c61db'),
        ...     sync=True,
        ...     paths_map=pmap1,
        ...     time_started=datetime(2012, 12, 4, 1, 1, 32, 858221),
        ...     cryptographer=cr
        ... )  # doctest:+NORMALIZE_WHITESPACE
        DatasetOnVirtualFiles(name='ABC',
            sync=True,
            uuid=UUID('766796e9-6f08-456d-806b-a24bc01f180c'),
            ugroup_uuid=UUID('19ab45a1-ff4b-4e8a-a38e-c3771b7c61db'),
            time_started=datetime.datetime(2012, 12, 4, 1, 1, 32, 858221),
            directories={u'/home/john/t1': ([], [])})

        @todo: add unit tests that verify the datasets with much more files,
            with physical files,
            with the directories without "ifiles", with deleted files, etc etc.

        @param ds_name: the dataset name.
        @type ds_name: basestring

        @name ds_uuid: the UUID of the dataset.
        @type ds_uuid: DatasetUUID

        @param ugroup_uuid: the UUID of the User Group to which the dataset
                            should be bound.
        @type ugroup_uuid: UserGroupUUID

        @param sync: whether a dataset should be considered a "sync" one.
        @type sync: bool

        @param paths_map: the mapping of paths to either files or directories
                          into the per-path settings.
        @type paths_map: dict

        @precondition: consists_of(paths_map.iterkeys(), basestring)

        @precondition: ensure::
            all(os_ex.stat_isdir(i['stat']) or
                os_ex.stat_isfile(i['stat'])
                    for i in paths_map.itervalues())

        @param time_started: startup time of the Dataset generation.
        @type time_started: datetime

        @param cryptographer: the C{Cryptographer} object to use for file
            fingerprint calculation.
        @type cryptographer: Cryptographer

        @return: the C{AbstractDatasetFromDecryptedData} object
            with the chunks containing the files from the directory/files list.
        @rtype: DatasetOnVirtualFiles
        """
        logger.verbose('Creating %r from_paths: %r, %r, %r, %r, %r, %r, %r',
                       cls, ds_name, ds_uuid, ugroup_uuid, sync,
                       paths_map, time_started, cryptographer)

        paths_map_fixed = {k: cls._fix_settings_for_selected_path(v)
                               for k, v in paths_map.iteritems()}

        dir_map = {path: path_settings
                       for path, path_settings in paths_map_fixed.iteritems()
                       if os_ex.stat_isdir(path_settings['stat'])}

        file_map = {path: path_settings
                        for path, path_settings in paths_map_fixed.iteritems()
                        if os_ex.stat_isfile(path_settings['stat'])}

        return cls(name=ds_name,
                   sync=sync,
                   directories=cls._arrange_files_for_backup(dir_map,
                                                             file_map.keys(),
                                                             ds_uuid,
                                                             cryptographer,
                                                             ospath=os.path),
                   uuid=ds_uuid,
                   ugroup_uuid=ugroup_uuid,
                   time_started=time_started)



class DatasetOnPhysicalFiles(DatasetOnVirtualFiles):
    """
    The dataset structure over the real locally available files
    in local directories.

    @todo: ideally, it may be a subclass of C{DatasetOnVirtualFiles}.
    """

    __slots__ = tuple()


    @classmethod
    @contract_epydoc
    def walk_directories(cls, base_dir, regex_incl, regex_excl, dir=''):
        """
        Given a base directory, scan it recursively and return the iterable
        of lists which contain files, grouped by directories.

        @type base_dir: basestring
        @param regex_incl: compiled RE pattern
        @param regex_excl: compiled RE pattern
        @type dir: basestring

        @rtype: col.Iterable
        @returns: a (possibly non-reiterable) Iterable of lists

        @todo: can it be changed to os.walk?
        """
        _full_dir = os.path.join(base_dir, dir)

        # Contains tuples of a short (rel) and a long (full) path.
        _all_entries = ((os.path.join(dir, e), os.path.join(_full_dir, e))
                            for e in os_ex.safe_listdir(_full_dir))

        # Sort it by name, just for convenience and stability.
        _all_entries_sorted = sorted(_all_entries,
                                     key=operator.itemgetter(0))

        # Besides rel and full path, we need the "stat" information
        # for each file
        all_entries = ((rel,
                        full,
                        os_ex.safe_stat(full),
                        partial(os_ex.safe_stat, full))
                           for rel, full in _all_entries_sorted)

        # Loop over the short and the long entries simultaneously
        # and filter out files and directories.

        _all_entries_for_files, _all_entries_for_dirs = tee(all_entries, 2)

        # time_changed field is set to None, so that it is not important to
        # backup the particular state of the file, but rather the latest one.
        all_files = (RelVirtualFile(rel_dir=os.path.dirname(es),
                                    name=os.path.basename(es),
                                    stat=st,
                                    stat_getter=st_getter)
                         for es, el, st, st_getter in _all_entries_for_files
                             if st is not None and
                                stat_isfile(st) and
                                regex_incl.match(es) and
                                not regex_excl.match(es))
        all_dirs = (es
                        for es, el, st, st_getter in _all_entries_for_dirs
                            if st is not None and stat_isdir(st))

        # Recurse into every subdirectory
        per_subdirectory = \
            (cls.walk_directories(base_dir, regex_incl, regex_excl, d)
                 for d in all_dirs)
        return chain([all_files], chain.from_iterable(per_subdirectory))


    @classmethod
    def _ifiles_local_file_is_good(cls, base_dir, rel_vfile):
        """Overrides the implementation from C{DatasetOnVirtualFiles}.

        @type rel_vfile: RelVirtualFile

        @returns: whether the file passed in the file iterator for backup
                  creation matches its specification.
        @rtype: bool
        @todo: properly handle directories, misc non-standard files and the
               situation when the file turns into a directory, and vice versa.
        """
        # Test the virtual parameters.
        pre_check = super(DatasetOnPhysicalFiles, cls) \
                        ._ifiles_local_file_is_good(base_dir, rel_vfile)

        if not pre_check:
            result = False
        else:
            # Test the real physical presence.
            full_path = os.path.join(base_dir, rel_vfile.rel_path)
            if rel_vfile.is_deleted:
                # Test that the file is missing.
                result = os.path.exists(full_path) == False
            else:
                # Check that the file exists and matches its specs.
                if os.path.exists(full_path):
                    st = rel_vfile.restat()
                    result = (st is not None and
                              rel_vfile.size == st.st_size and
                              rel_vfile.time_changed == stat_get_mtime(st))
                else:
                    # The file should be present
                    result = False

        return result


    @classmethod
    def _get_filesets_per_dirs(cls, dir_map):
        """Overrides the implementation from C{DatasetOnVirtualFiles}.

        @rtype: col.Mapping
        """
        # The filesets are keyed by base directory
        # (or the root dir symbol ("/") on Unix,
        #  or the drive name for independent files on Windows).
        per_dir_filesets = super(DatasetOnPhysicalFiles, cls) \
                               ._get_filesets_per_dirs(dir_map)

        # Scan the selected directories
        for base_dir, dir_settings in dir_map.iteritems():
            directory = os.path.abspath(base_dir)

            # There are two ways of scanning.
            # If there is "ifiles" present, we already have the files to be
            # used in the backup (let's just check they indeed exist and match
            # their size/last-change-time). Otherwise, we scan the directories
            # optionally using the filters.

            # But ifiles were already scanned in the superclass,
            # so we just recheck that.
            if 'ifiles' in dir_settings and directory in per_dir_filesets:
                # Scanned already, do nothing
                pass
            elif 'ifiles' in dir_settings or directory in per_dir_filesets:
                # some problems with scanning in the super class?
                logger.error('Problem with ifiles-scanning %r: %r in %r',
                             dir_settings, directory, per_dir_filesets)
            else:
                # Scan the directory normally. Store the list.
                per_base_dir = (cls.walk_directories(directory,
                                                     dir_settings['r+'],
                                                     dir_settings['r-']))

                # Unfortunately, the results are iterated multiple times;
                # we have to store the explicit list here for now.
                # Also, see file_list analysis below, it has the same issue.
                per_base_dir = map(list, per_base_dir)
                assert all(consists_of(l, RelVirtualFile)
                               for l in per_base_dir), \
                       repr(per_base_dir)
                per_dir_filesets[directory] = per_base_dir

        return per_dir_filesets


    @classmethod
    def _get_filesets_per_files(cls, files):
        """Overrides the implementation from C{DatasetOnVirtualFiles}.

        @type files: col.Iterable

        @rtype: col.Mapping
        """
        per_dir_filesets = super(DatasetOnPhysicalFiles, cls) \
                               ._get_filesets_per_files(files)

        # Scan the selected separate files, if there are any.
        # Under Windows and Linux, they are grouped differently:
        # under the each root disk of the file (Windows) or under the root dir
        # (Linux).
        if files:
            if sys.platform == 'win32':
                # Each separate file goes under the directory of the root disk.
                # Let's generate the tuples containing:
                # 1. disk (C:/)
                # 2. path on disk, relative to the disk root directory
                #    (/ntldr.com)
                # 3. original full path.
                __path_splits = ((os.path.splitdrive(f), f)
                                     for f in files)
                _path_splits = \
                    ((os.path.join(drive, os.path.sep),
                      rel_disk_path,
                      full_path)
                         for (drive, rel_disk_path), full_path
                             in __path_splits)

                _split_pairs = ((os.path.join(_drive, os.path.sep), _path)
                                    for _drive, _path
                                        in imap(os.path.splitdrive, files))

                # Group per_dir_filesets by the disk
                for disk, rel_disk_path, full_path in _path_splits:
                    # The value is a list of lists (actually, of a single one)
                    # of paths!
                    st = os_ex.safe_stat(full_path)
                    if st is None:
                        logger.warning('Unreadable attributes for %r', st)
                    else:
                        paths_for_disk_list = \
                            per_dir_filesets.setdefault(disk, [])
                        paths_for_disk_l0 = \
                            paths_for_disk_list.setdefault(0, [])

                        paths_for_disk_l0.append(
                            RelVirtualFile(
                                rel_dir=os.path.dirname(rel_disk_path),
                                name=os.path.basename(rel_disk_path),
                                stat=st,
                                stat_getter=partial(os_ex.safe_path,
                                                    full_path)))

            elif sys.platform.startswith('linux') or sys.platform == 'darwin':
                # Separate files go under the root directory
                assert os.path.sep not in per_dir_filesets, \
                       (sys.platform, per_dir_filesets)

                _abs_file_paths = imap(os.path.abspath, files)
                # 1. rel. path (relative to root dir)
                # 2. os.stat result
                _file_paths = ((os.path.relpath(abspath, os.sep),
                                os_ex.safe_stat(abspath),
                                partial(os_ex.safe_stat, abspath))
                                   for abspath in _abs_file_paths)
                _file_datas = (RelVirtualFile(rel_dir=os.path.dirname(relpath),
                                              name=os.path.basename(relpath),
                                              stat=st,
                                              stat_getter=st_getter)
                                   for relpath, st, st_getter in _file_paths
                                       if st is not None)

                per_base_dir = [_file_datas]

                per_base_dir = map(list, per_base_dir)
                assert all(consists_of(l, RelVirtualFile)
                               for l in per_base_dir), \
                       repr(per_base_dir)
                per_dir_filesets[os.path.sep] = per_base_dir

            else:
                raise NotImplementedError(sys.platform)

        return per_dir_filesets



class DatasetOnChunks(DatasetWithDirectories):
    """
    The dataset object over the particular chunk objects.
    """

    __slots__ = ('__chunks',)


    @contract_epydoc
    def __init__(self, chunks=None, *args, **kwargs):
        """
        @type chunks: list, NoneType
        """
        chunks = coalesce(chunks, [])

        super(DatasetOnChunks, self).__init__(*args, **kwargs)
        self.__chunks = chunks


    def __str__(self):
        return u'{super}, {ch:d} chunks' \
                   .format(super=super(DatasetOnChunks, self).__str__(),
                           ch=len(self.__chunks))


    def chunks(self):
        """Overrides the .chunks() method in the AbstractDataset.

        @returns: The iterator over the chunks in this dataset.
        @rtype: iterator
        """
        return iter(self.__chunks)



class DatasetWithAggrInfo(AbstractDataset):
    """
    The dataset object with aggregated info over its components.
    """

    __slots__ = ('__chunks_count', '__files_count', '__size')


    def __init__(self, chunks_count, files_count, size, *args, **kwargs):
        r"""Constructor.

        >>> DatasetWithAggrInfo(
        ...     # DatasetWithAggrInfo-specific
        ...     chunks_count=15, files_count=42, size=16385,
        ...     # AbstractDataset-specific
        ...     name='ABC', sync=True,
        ...     uuid=UUID('6aa80e50-c460-4b02-9272-f9132ba56fd5'),
        ...     ugroup_uuid= UUID('e5e74503-dd07-4f30-b848-7630d33487d4'),
        ...     time_started=datetime(2012, 12, 17, 19, 5, 2, 171558)
        ... )  # doctest:+NORMALIZE_WHITESPACE
        DatasetWithAggrInfo(name='ABC', sync=True,
            uuid=UUID('6aa80e50-c460-4b02-9272-f9132ba56fd5'),
            ugroup_uuid=UUID('e5e74503-dd07-4f30-b848-7630d33487d4'),
            time_started=datetime.datetime(2012, 12, 17, 19, 5, 2, 171558),
            chunks_count=15, files_count=42, size=16385)
        """
        super(DatasetWithAggrInfo, self).__init__(*args, **kwargs)
        self.__chunks_count = chunks_count
        self.__files_count = files_count
        self.__size = size


    def __str__(self):
        return u'{super}, '\
               u'chunks_count={self._DatasetWithAggrInfo__chunks_count:d}, '\
               u'files_count={self._DatasetWithAggrInfo__files_count:d}, '\
               u'size={self._DatasetWithAggrInfo__size:d}'.format(
                   super=super(DatasetWithAggrInfo, self).__str__(),
                   self=self)


    def chunks_count(self):
        r"""Implements the C{AbstractDatasetWithChunks} interface.

        >>> DatasetWithAggrInfo(
        ...     # DatasetWithAggrInfo-specific
        ...     chunks_count=15, files_count=42, size=16385,
        ...     # AbstractDataset-specific
        ...     name='ABC', sync=True,
        ...     uuid=UUID('6aa80e50-c460-4b02-9272-f9132ba56fd5'),
        ...     ugroup_uuid= UUID('e5e74503-dd07-4f30-b848-7630d33487d4')
        ... ).chunks_count()
        15
        """
        return self.__chunks_count


    def files_count(self):
        r"""Implements the C{AbstractDataset} interface.

        >>> DatasetWithAggrInfo(
        ...     # DatasetWithAggrInfo-specific
        ...     chunks_count=15, files_count=42, size=16385,
        ...     # AbstractDataset-specific
        ...     name='ABC', sync=True,
        ...     uuid=UUID('6aa80e50-c460-4b02-9272-f9132ba56fd5'),
        ...     ugroup_uuid= UUID('e5e74503-dd07-4f30-b848-7630d33487d4')
        ... ).files_count()
        42
        """
        return self.__files_count


    def size(self):
        r"""Implements the C{AbstractDataset} interface.

        >>> DatasetWithAggrInfo(
        ...     # DatasetWithAggrInfo-specific
        ...     chunks_count=15, files_count=42, size=16385,
        ...     # AbstractDataset-specific
        ...     name='ABC', sync=True,
        ...     uuid=UUID('6aa80e50-c460-4b02-9272-f9132ba56fd5'),
        ...     ugroup_uuid= UUID('e5e74503-dd07-4f30-b848-7630d33487d4')
        ... ).size()
        16385
        """
        return self.__size


    def to_json(self):
        r"""Implement C{IJSONable} interface.

        @precondition: isinstance(self.uuid, UUID)
        @rtype: dict

        >>> DatasetWithAggrInfo(
        ...     # DatasetWithAggrInfo-specific
        ...     chunks_count=15, files_count=42, size=16385,
        ...     # AbstractDataset-specific
        ...     name='ABC', sync=True,
        ...     uuid=UUID('6aa80e50-c460-4b02-9272-f9132ba56fd5'),
        ...     ugroup_uuid= UUID('e5e74503-dd07-4f30-b848-7630d33487d4'),
        ...     time_started=datetime(2012, 11, 5, 12, 12, 42, 74823),
        ... ).to_json()  # doctest:+NORMALIZE_WHITESPACE
        {'uuid': '6aa80e50c4604b029272f9132ba56fd5', 'sync': 1,
         'chunks_count': 15, 'ugroup_uuid': 'e5e74503dd074f30b8487630d33487d4',
         'time_started': '2012-11-05 12:12:42.074823', 'size': 16385,
         'files_count': 42, 'name': 'ABC'}
        """
        res = super(DatasetWithAggrInfo, self).to_json()
        res.update({'chunks_count': self.__chunks_count,
                    'files_count': self.__files_count,
                    'size': self.__size})
        return res


    @classmethod
    def from_json(cls, data):
        r"""Implement C{IJSONable} interface.

        For directories, the dataset puts all the files to the list
        of small files only.

        >>> DatasetWithAggrInfo.from_json({
        ...     'uuid': '6aa80e50c4604b029272f9132ba56fd5', 'sync': 1,
        ...     'chunks_count': 15,
        ...     'ugroup_uuid': 'e5e74503dd074f30b8487630d33487d4',
        ...     'time_started': '2012-12-17 15:23:30.654187', 'size': 16385,
        ...     'files_count': 42, 'name': 'ABC'
        ... })()  # doctest:+NORMALIZE_WHITESPACE
        DatasetWithAggrInfo(name='ABC', sync=True,
            uuid=DatasetUUID('6aa80e50-c460-4b02-9272-f9132ba56fd5'),
            ugroup_uuid=UserGroupUUID('e5e74503-dd07-4f30-b848-7630d33487d4'),
            time_started=datetime.datetime(2012, 12, 17, 15, 23, 30, 654187),
            chunks_count=15, files_count=42, size=16385)

        @rtype: DatasetWithAggrInfo
        """
        return partial(super(DatasetWithAggrInfo, cls).from_json(data),
                       chunks_count=int(data['chunks_count']),
                       files_count=int(data['files_count']),
                       size=int(data['size']))
