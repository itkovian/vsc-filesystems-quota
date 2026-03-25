#
# Copyright 2023-2026 Ghent University
#
# This file is part of vsc-filesystems-quota,
# originally created by the HPC team of Ghent University (http://ugent.be/hpc/en),
# with support of Ghent University (http://ugent.be/hpc),
# the Flemish Supercomputer Centre (VSC) (https://www.vscentrum.be),
# the Flemish Research Foundation (FWO) (http://www.fwo.be/en)
# and the Department of Economy, Science and Innovation (EWI) (http://www.ewi-vlaanderen.be/en).
#
# https://github.com/hpcugent/vsc-filesystems-quota
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
@author: Ward Poelmans (Vrije Universiteit Brussel)
"""

import json
import os
import pytest
import vsc.config.base as config
from vsc.install.testing import TestCase


from collections import namedtuple
from unittest.mock import MagicMock, patch, PropertyMock

from vsc.filesystem.quota.ptools import (
    parse_metric_name,
    KIND_MAP,
    SUFFIX_MAP,
    UsageReporter,
)
from vsc.filesystem.quota.utils import UsageInformation

config.STORAGE_CONFIGURATION_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'filesystem_info.conf')


class TestParseMetricName(TestCase):
    def test_user_used_files(self):
        kind, field = parse_metric_name("gpfs_user_used_files")
        assert kind == "USR"
        assert field == "files_usage"

    def test_fileset_used_blocks(self):
        kind, field = parse_metric_name("gpfs_fileset_used_bytes")
        assert kind == "FILESET"
        assert field == "block_usage"

    def test_group_kind(self):
        kind, field = parse_metric_name("gpfs_group_used_files")
        assert kind == "GROUP"
        assert field == "files_usage"

    def test_not_gpfs_prefix(self):
        kind, field = parse_metric_name("process_cpu_seconds_total")
        assert kind is None
        assert field is None

    def test_unknown_kind(self):
        kind, field = parse_metric_name("gpfs_banana_used_files")
        assert kind is None
        assert field is None

    def test_unknown_suffix(self):
        kind, field = parse_metric_name("gpfs_user_something_weird")
        kind_result, field_result = parse_metric_name("gpfs_user_something_weird")
        assert kind_result == "USR"
        assert field_result is None

    def test_too_short(self):
        kind, field = parse_metric_name("gpfs_user")
        assert kind is None
        assert field is None

    def test_empty_string(self):
        kind, field = parse_metric_name("")
        assert kind is None
        assert field is None


def make_reporter(filesystems=None, replication_factors=None):
    """Build a minimal UsageReporter without going through CLI.__init__."""
    reporter = UsageReporter.__new__(UsageReporter)
    reporter.usage_list = []
    reporter.cache = {}
    reporter.system_storage_map = filesystems or {"home": "kyukonhome"}
    reporter.replication_factors = replication_factors or {"kyukonhome": 1}
    reporter.options = MagicMock()
    reporter.options.metrics_url = "https://ces06.gastly.os:9100/metrics"
    reporter.options.metrics_user = "prometheus"
    reporter.options.metrics_passwd = "secret"
    reporter.options.cert_file = "/etc/ipa/quattor/certs/host.pem"
    reporter.options.key_file = "/etc/ipa/quattor/keys/host.key"
    reporter.options.ca_file = "/etc/ipa/ca.crt"
    return reporter

def make_entry(name, fs, fileset, user=None, value=0.0):
    tags = {"fs": fs, "fileset": fileset}
    if user:
        tags["user"] = user
    return {
        "name": name,
        "tags": tags,
        "timestamp": "2026-03-25T08:52:36.790027879Z",
        "kind": "absolute",
        "gauge": {"value": value},
    }


class TestConsolidate(TestCase):

    def test_single_user_metric(self):
        reporter = make_reporter()
        entries = [make_entry("gpfs_user_used_files", "kyukonhome", "99", "vsc40001", 100.0)]
        results = reporter.consolidate(entries)
        assert len(results) == 1
        assert results[0].files_usage == 100
        assert results[0].kind == "USR"
        assert results[0].entity == "vsc40001"

    def test_multiple_metrics_same_user_consolidated(self):
        reporter = make_reporter()
        entries = [
            make_entry("gpfs_user_used_files", "kyukonhome", "99", "vsc40001", 100.0),
            make_entry("gpfs_user_used_bytes", "kyukonhome", "99", "vsc40001", 2048.0),
        ]
        results = reporter.consolidate(entries)
        assert len(results) == 1
        assert results[0].files_usage == 100
        assert results[0].block_usage == 2048
