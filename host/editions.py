#!/usr/bin/python
"""
The host-specific edition-specific features of the system.
"""

#
# Imports
#

from __future__ import absolute_import
from common import editions as common_editions


class HostFeatureSet(common_editions.FeatureSet):

    __slots__ = ('blinking_icon',
                 'dflt_user_conf_dir_tmpl', 'dflt_user_sync_dir_tmpl',
                 'dflt_group_sync_dir_tmpl', 'dflt_restore_dir_tmpl',
                 'first_launch_wizard')


    def __init__(self,
                 blinking_icon,
                 dflt_user_conf_dir_tmpl, dflt_user_sync_dir_tmpl,
                 dflt_group_sync_dir_tmpl, dflt_restore_dir_tmpl,
                 first_launch_wizard,
                 *args, **kwargs):

        super(HostFeatureSet, self).__init__(*args, **kwargs)
        self.blinking_icon = blinking_icon
        self.dflt_user_conf_dir_tmpl = dflt_user_conf_dir_tmpl
        self.dflt_user_sync_dir_tmpl = dflt_user_sync_dir_tmpl
        self.dflt_group_sync_dir_tmpl = dflt_group_sync_dir_tmpl
        self.dflt_restore_dir_tmpl = dflt_restore_dir_tmpl
        self.first_launch_wizard = first_launch_wizard


    def __str__(self):
        return (u'{super}, '
                u'blinking_icon={self.blinking_icon!r}, '
                u'dflt_user_conf_dir_tmpl={self.dflt_user_conf_dir_tmpl!r}, '
                u'dflt_user_sync_dir_tmpl={self.dflt_user_sync_dir_tmpl!r}, '
                u'dflt_group_sync_dir_tmpl={self.dflt_group_sync_dir_tmpl!r}, '
                u'dflt_restore_dir_tmpl={self.dflt_restore_dir_tmpl!r}, '
                u'first_launch_wizard={self.first_launch_wizard!r}'
                     .format(super=super(HostFeatureSet, self).__str__(),
                             self=self))


    @property
    def _init_kwargs(self):
        base_kwars = dict(super(HostFeatureSet, self)._init_kwargs)
        base_kwars.update({
            'blinking_icon': self.blinking_icon,
            'dflt_user_conf_dir_tmpl': self.dflt_user_conf_dir_tmpl,
            'dflt_user_sync_dir_tmpl': self.dflt_user_sync_dir_tmpl,
            'dflt_group_sync_dir_tmpl': self.dflt_group_sync_dir_tmpl,
            'dflt_restore_dir_tmpl': self.dflt_restore_dir_tmpl,
            'first_launch_wizard': self.first_launch_wizard,
        })
        return base_kwars



EDITIONS = {
    'community':
        HostFeatureSet(
            blinking_icon=False,
            dflt_user_conf_dir_tmpl=u'~/.{short_name_lc}',
            dflt_user_sync_dir_tmpl=u'~/{short_name}/My files',
            dflt_group_sync_dir_tmpl=u'~/{short_name}/Shared/{group_name}',
            dflt_restore_dir_tmpl=u'~/{short_name}/Restored files',
            first_launch_wizard=True,
            **common_editions.EDITIONS['community']._init_kwargs),
    'personal':
        HostFeatureSet(
            blinking_icon=False,
            dflt_user_conf_dir_tmpl=u'~/.{short_name_lc}',
            dflt_user_sync_dir_tmpl=u'~/{short_name}',
            dflt_group_sync_dir_tmpl=u'~/{short_name}/{group_name}',
            dflt_restore_dir_tmpl=u'~/{short_name}/Restored files',
            first_launch_wizard=False,
            **common_editions.EDITIONS['personal']._init_kwargs),
    'superfolder':
        HostFeatureSet(
            blinking_icon=True,
            dflt_user_conf_dir_tmpl=u'~/.{short_name_lc}',
            dflt_user_sync_dir_tmpl=u'~/{short_name}',
            dflt_group_sync_dir_tmpl=u'~/{short_name}/{group_name}',
            dflt_restore_dir_tmpl=u'~/{short_name}/Restored files',
            first_launch_wizard=False,
            **common_editions.EDITIONS['superfolder']._init_kwargs),
}
"""
@note: all paths and path templates are forced-Unicode for more uniform calling
    of OS-specific interfaces!

@seealso: C{common.editions}.
"""
