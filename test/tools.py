#!/usr/bin/env python
#
# Copyright 2012-2015 Ghent University
#
# This file is part of vsc-filesystems-quota,
# originally created by the HPC team of Ghent University (http://ugent.be/hpc/en),
# with support of Ghent University (http://ugent.be/hpc),
# the Flemish Supercomputer Centre (VSC) (https://www.vscentrum.be),
# the Hercules foundation (http://www.herculesstichting.be/in_English)
# and the Department of Economy, Science and Innovation (EWI) (http://www.ewi-vlaanderen.be/en).
#
# https://github.ugent.be/hpcugent/vsc-filesystems-quota
#
# vsc-filesystems-quota is free software: you can redistribute it and/or modify
# it under the terms of the GNU Library General Public License as
# published by the Free Software Foundation, either version 2 of
# the License, or (at your option) any later version.
#
# vsc-filesystems-quota is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU Library General Public License for more details.
#
# You should have received a copy of the GNU Library General Public License
# along with vsc-filesystems-quota. If not, see <http://www.gnu.org/licenses/>.
#
"""
Tests for all helper functions in vsc.filesystems.quota.tools.

@author: Andy Georges (Ghent University)
"""
import mock

import vsc.filesystem.quota.tools as tools

from vsc.config.base import VSC_DATA
from vsc.filesystem.quota.entities import QuotaUser, QuotaFileset
from vsc.filesystem.quota.tools import push_vo_quota_to_django
from vsc.install.testing import TestCase


class TestAuxiliary(TestCase):
    """
    Stuff that does not belong anywhere else :)
    """

    @mock.patch('vsc.filesystem.quota.tools.pwd.getpwall')
    def test_map_uids_to_names(self, mock_getpwall):
        """
        Check that the remapping functions properly
        """
        uids = [(1, 2, 3), (4, 5, 6), (7, 8, 9)]

        mock_getpwall.return_value = uids
        res = tools.map_uids_to_names()

        self.assertEqual(res, {3: 1, 6: 4, 9: 7})


class TestProcessing(TestCase):

    @mock.patch('vsc.filesystem.quota.tools.push_user_quota_to_django')
    def test_process_user_quota_no_store(self, mock_push_quota):

        storage_name = VSC_DATA
        item = 'vsc40075'
        filesystem = 'vulpixdata'
        quota = QuotaUser(storage_name, filesystem, item)
        quota.update('vsc400', used=1230, soft=456, hard=789, doubt=0, expired=(False, None), timestamp=None)
        quota.update('gvo00002', used=1230, soft=456, hard=789, doubt=0, expired=(False, None), timestamp=None)

        storage = mock.MagicMock()
        storage[storage_name] = mock.MagicMock()
        storage[storage_name].login_mount_point = "/my_login_mount_point"
        storage[storage_name].gpfs_mount_point = "/my_gpfs_mount_point"
        storage.path_templates = mock.MagicMock()

        gpfs = mock.MagicMock()
        gpfs.is_symlink.return_value = False

        client = mock.MagicMock()

        quota_map = {'2540075': quota}
        user_map = {2540075: 'vsc40075'}

        tools.process_user_quota(
            storage, gpfs, storage_name, None, quota_map, user_map, client, dry_run=False
        )

        mock_push_quota.assert_called_with(
            user_map, storage_name, storage.path_templates[storage_name], quota_map, client, False
        )

    @mock.patch('vsc.filesystem.quota.tools.push_vo_quota_to_django')
    def test_process_fileset_quota_no_store(self, mock_push_quota):

        storage_name = VSC_DATA
        filesystem = 'vulpixdata'
        fileset = 'gvo00002'
        quota = QuotaFileset(storage_name, filesystem, fileset)
        quota.update('gvo00002', used=1230, soft=456, hard=789, doubt=0, expired=(False, None), timestamp=None)

        storage = mock.MagicMock()

        filesets = {
            filesystem: {
                fileset: {
                    'path': '/my_path',
                    'filesetName': fileset,
                }
            }
        }
        gpfs = mock.MagicMock()
        gpfs.list_filesets.return_value = filesets

        client = mock.MagicMock()

        quota_map = {fileset: quota}

        tools.process_fileset_quota(
            storage, gpfs, storage_name, filesystem, quota_map, client, dry_run=False
        )

        mock_push_quota.assert_called_with(storage_name, quota_map, client, False, filesets, filesystem)

    @mock.patch('vsc.config.base.VscStorage', autospec=True)
    def test_push_vo_quota_to_django(self, mock_storage):

        fileset_name = 'gvo00002'
        fileset_id = '1'
        filesystem = 'scratchdelcatty'
        filesets = {filesystem: {fileset_id: {'filesetName': fileset_name}}}
        storage_name = 'VSC_SCRATCH_DELCATTY'
        quota = QuotaFileset(mock_storage, filesystem, fileset_name)
        quota.update(fileset_name, used=1230, soft=456, hard=789, doubt=0, expired=(False, None), timestamp=None)

        quota_map = {
            '1': quota,
        }

        client = mock.MagicMock()
        push_vo_quota_to_django(storage_name, quota_map, client, False, filesets, filesystem)
