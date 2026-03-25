#
# Copyright 2023-2025 Ghent University
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
Helper class for pushing data to django webapp

@author: Andy Georges (Ghent University)
"""

import logging
import re
from collections import namedtuple

from vsc.config.base import STORAGE_SHARED_SUFFIX

DISK_CACHE_LOCATION = "/var/cache/kusage.cache"

UsageInformation = namedtuple(
    "UsageInformation",
    [
        "filesystem",  # filesystem name
        "fileset",  # fileset (gpfs)
        "entity",  # the user or VO owning the usage
        "kind",  # the kind of usage info (USR, FILESET, GROUP)
        "block_usage",  # used quota in KiB
        "block_soft",  # soft quota limit in KiB
        "block_hard",  # hard quota limit in KiB
        "block_doubt",  # the KiB GPFS is not sure about
        "block_expired",  # tuple (boolean, grace period expressed in seconds)
        "files_usage",  # used number of inodes
        "files_soft",  # soft limit for inodes
        "files_hard",  # hard limit for inodes
        "files_doubt",  # the inodes GPFS is not sure about
        "files_expired",  # tuple (boolean, grace period expressed in seconds)
    ],
)

GPFS_GRACE_REGEX = re.compile(
    r"(?P<days>\d+)\s*days?|(?P<hours>\d+)\s*hours?|(?P<minutes>\d+)\s*minutes?|(?P<expired>expired)"
)

GPFS_NOGRACE_REGEX = re.compile(r"none", re.I)

QUOTA_USER_KIND = "USR"
QUOTA_VO_KIND = "FILESET"


class QuotaException(Exception):
    pass


class DjangoPusher:
    """Context manager for pushing stuff to django"""

    def __init__(self, storage_name, client, kind, dry_run):
        self.storage_name = storage_name
        self.storage_name_shared = storage_name + STORAGE_SHARED_SUFFIX
        self.client = client
        self.kind = kind
        self.dry_run = dry_run

        self.count = {self.storage_name: 0, self.storage_name_shared: 0}

        self.payload = {self.storage_name: [], self.storage_name_shared: []}

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        if self.payload[self.storage_name]:
            self._push(self.storage_name, self.payload[self.storage_name])
        if self.payload[self.storage_name_shared]:
            self._push(self.storage_name_shared, self.payload[self.storage_name_shared])

        if exc_type is not None:
            logging.error("Received exception %s in DjangoPusher: %s", exc_type, exc_value)
            return False

        return True

    def push(self, storage_name, payload):
        if storage_name not in self.payload:
            logging.error("Can not add payload for unknown storage: %s vs %s", storage_name, self.storage_name)
            return

        self.payload[storage_name].append(payload)
        self.count[storage_name] += 1

        if self.count[storage_name] > 100:
            self._push(storage_name, self.payload[storage_name])
            self.count[storage_name] = 0
            self.payload[storage_name] = []

    def push_quota(self, owner, quota, shared=False):
        """
        Push quota to accountpage: it belongs to owner (can either be user_id or vo_id),
        in the given fileset and quota.
        :param owner: the name of the user or VO to which the quota belongs
        :param fileset: fileset name
        :param quota: actual quota data
        :param shared: is this a shared user/VO quota or not?
        """
        params = {
            "fileset": quota.fileset,
            "used": quota.block_usage,
            "soft": quota.block_soft,
            "hard": quota.block_hard,
            "doubt": quota.block_doubt,
            "expired": quota.block_expired[0],
            "remaining": quota.block_expired[1] or 0,  # seconds
            "files_used": quota.files_usage,
            "files_soft": quota.files_soft,
            "files_hard": quota.files_hard,
            "files_doubt": quota.files_doubt,
            "files_expired": quota.files_expired[0],
            "files_remaining": quota.files_expired[1] or 0,  # seconds
        }
        logging.debug("Pushing quota %s with params %s", quota, params)

        if self.kind == QUOTA_USER_KIND:
            params["user"] = owner
        elif self.kind == QUOTA_VO_KIND:
            params["vo"] = owner

        if shared:
            self.push(self.storage_name_shared, params)
        else:
            self.push(self.storage_name, params)

    def _push(self, storage_name, payload):
        """Does the actual pushing to the REST API"""

        if self.dry_run:
            logging.info("Would push payload to account web app: %s", payload)
        else:
            try:
                cl = self.client.usage.storage[storage_name]
                if self.kind == QUOTA_USER_KIND:
                    logging.debug("Pushing user payload to account web app: %s", payload)
                    cl = cl.user
                elif self.kind == QUOTA_VO_KIND:
                    logging.debug("Pushing vo payload to account web app: %s", payload)
                    cl = cl.vo
                else:
                    logging.error("Unknown quota kind, not pushing any quota to the account page")
                    return
                cl.size.put(body=payload)  # if all is well, there's nothing returned except (200, empty string)
            except Exception:
                logging.error("Could not store quota info in account web app")
                raise
