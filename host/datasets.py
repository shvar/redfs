#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
Host-specific structures and classes related to Datasets.
"""

#
# Imports
#

from __future__ import absolute_import

from contrib.dbc import contract_epydoc

from common.datasets import DatasetOnChunks



#
# Classes
#

class MyDatasetOnChunks(DatasetOnChunks):
    """The dataset object on the chunks local to the host.

    @invariant: consists_of(self.chunks, ChunkFromFiles)
    """

    __slots__ = ('paused',)


    @contract_epydoc
    def __init__(self, paused=False, *args, **kwargs):
        """
        @type paused: bool
        """
        super(MyDatasetOnChunks, self).__init__(*args, **kwargs)
        self.paused = paused


    def __str__(self):
        return u'{super}{paused}'.format(
                   super=super(MyDatasetOnChunks, self).__str__(),
                   paused=' ▮▮' if self.paused else '')
