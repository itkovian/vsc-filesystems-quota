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
Helper functions for all things quota related.

@author: Andy Georges (Ghent University)
@author: Ward Poelmans (Vrije Universiteit Brussel)
"""

import json
import logging
import re
import diskcache as dc

from vsc.kafka.cli import ConsumerCLI

from vsc.accountpage.client import AccountpageClient
from vsc.config.base import GENT, VO_PREFIX_BY_SITE, VO_SHARED_PREFIX_BY_SITE, VscStorage
from vsc.filesystem.quota.utils import (
    UsageInformation,
    DjangoPusher,
    QuotaException,
    QUOTA_USER_KIND,
    QUOTA_VO_KIND,
    determine_grace_period,
)

DISK_CACHE_LOCATION = "/var/cache/kusage.cache"

GPFS_GRACE_REGEX = re.compile(
    r"(?P<days>\d+)\s*days?|(?P<hours>\d+)\s*hours?|(?P<minutes>\d+)\s*minutes?|(?P<expired>expired)"
)

GPFS_NOGRACE_REGEX = re.compile(r"none", re.I)


class UsageReporter(ConsumerCLI):
    CLI_OPTIONS = {
        "storage": ("the VSC filesystems that are checked by this script", None, "extend", []),
        "account_page_url": ("Base URL of the account page", None, "store", "https://account.vscentrum.be/django"),
        "access_token": ("OAuth2 token to access the account page REST API", None, "store", None),
        "host_institute": ("Name of the institute where this script is being run", str, "store", GENT),
        "group": ("Kafka consumer group", None, "store", "ap-quota"),
    }

    def convert_msg(self, msg):
        """
        Process msg as JSON.
        Return None on failure or if the message holds no usage information.

        full message looks like:
        {
              "@timestamp": "2023-01-09T19:19:19.518Z",
              "@metadata": {
                "beat": "gpfsbeat",
                "type": "_doc",
                "version": "7.10.0"
              },
              "quota": {
                "files_soft": 0,
                "kind": "USR",
                "files_usage": 2,
                "block_usage": 0,
                "filesystem": "arcaninescratch",
                "entity": "vsc40075",
                "block_hard": 1048576,
                "files_expired": "none",
                "fileset": "gvo00002",
                "block_soft": 995328,
                "files_hard": 0,
                "block_expired": "none",
                "block_doubt": 0,
                "files_doubt": 0
              },
              "type": "gpfsbeat",
              "counter": 657,
              "ecs": {
                "version": "1.6.0"
              },
              "host": {
                "name": "gpfsbeat"
              },
              "agent": {
                "ephemeral_id": "snip",
                "id": "snip",
                "name": "gpfsbeat",
                "type": "gpfsbeat",
                "version": "7.10.0",
                "hostname": "myhost.mydomain"
              }
        }
        """
        value = msg.value
        if value:
            try:
                event = json.loads(value)
            except ValueError:
                logging.error("Failed to load as JSON: %s", value)
                return None

            if "quota" in event:
                kwargs = {field: event["quota"][field] for field in UsageInformation._fields}
                return self._update_usage(UsageInformation(**kwargs))
            else:
                return None
        else:
            logging.error("msg has no value %s (%s)", msg, type(msg))
            return None

    def process_event(self, event, dry_run):
        if event and event.filesystem in self.system_storage_map.values():
            cache_key = (event.filesystem, event.fileset, event.entity, event.kind)
            cached_usage = self.cache.get(cache_key, default=None)
            if cached_usage == event:
                logging.debug("Event %s equals cached version", event)
            else:
                if not dry_run:
                    self.cache.set(cache_key, event, expire=864000)
                logging.debug("Event %s differs from %s, adding to usage list", event, cached_usage)
                self.usage_list.append(event)

    def do(self, dry_run):
        # pylint: disable=unused-argument

        ap_client = AccountpageClient(token=self.options.access_token, url=self.options.account_page_url + "/api/")

        self.storage = VscStorage()
        self.system_storage_map = {k: self.storage[GENT][k].filesystem for k in self.storage if k != GENT}
        self.replication_factors = {
            self.storage[GENT][k].filesystem: self.storage[GENT][k].data_replication_factor
            for k in self.storage
            if k != GENT
        }

        logging.info("storage map: %s", self.system_storage_map)

        self.usage_list = []
        with dc.Cache(DISK_CACHE_LOCATION) as cache:
            self.cache = cache
            super().do(dry_run)

        for storage_name in self.options.storage:
            logging.info("Processing quota for storage_name %s", storage_name)
            fileset_quota_data = [
                q
                for q in self.usage_list
                if self.system_storage_map[storage_name] == q.filesystem and q.kind == "FILESET"
            ]
            logging.debug("Fileset quota for storage %s: %s", storage_name, fileset_quota_data)
            self.process_fileset_quota(storage_name, fileset_quota_data, ap_client)

            usr_quota_data = [
                q for q in self.usage_list if self.system_storage_map[storage_name] == q.filesystem and q.kind == "USR"
            ]
            logging.debug("Usr quota for storage %s: %s", storage_name, usr_quota_data)
            self.process_user_quota(storage_name, usr_quota_data, ap_client)

    def process_user_quota(self, storage_name, quota_list, client):
        institute = self.options.host_institute
        path_template = self.storage.path_templates[institute][storage_name]

        logging.info("Logging user quota to account page")
        logging.debug("Considering the following quota items for pushing: %s", quota_list)

        with DjangoPusher(storage_name, client, QUOTA_USER_KIND, self.options.dry_run) as pusher:
            for quota in quota_list:
                if not quota.entity.startswith("vsc"):
                    # no longer a known user, we got the numerical UID, so no need to push info
                    continue

                user_name = quota.entity
                fileset_name = path_template["user"](user_name)[1]
                fileset_re = (
                    rf"^(vsc[1-4]|{VO_PREFIX_BY_SITE[institute]}|"
                    rf"{VO_SHARED_PREFIX_BY_SITE[institute]}|{fileset_name})"
                )

                if re.search(fileset_re, quota.fileset):
                    pusher.push_quota(user_name, quota)

    def process_fileset_quota(self, storage_name, quota_list, client):
        logging.info("Logging VO quota to account page")
        logging.debug("Considering the following quota items for pushing: %s", quota_list)

        institute = self.options.host_institute

        with DjangoPusher(storage_name, client, QUOTA_VO_KIND, self.options.dry_run) as pusher:
            for quota in quota_list:
                fileset_name = quota.fileset
                logging.debug("Fileset %s quota: %s", fileset_name, quota)

                if not fileset_name.startswith(VO_PREFIX_BY_SITE[institute]):
                    continue
                elif fileset_name.startswith(VO_SHARED_PREFIX_BY_SITE[institute]):
                    vo_name = fileset_name.replace(VO_SHARED_PREFIX_BY_SITE[institute], VO_PREFIX_BY_SITE[institute])
                    shared = True
                else:
                    vo_name = fileset_name
                    shared = False

                pusher.push_quota(vo_name, quota, shared=shared)

    def _update_usage(self, usage):
        """
        Update the quota information for an entity (user or fileset).
        """

        block_expired = determine_grace_period(usage.block_expired)
        files_expired = determine_grace_period(usage.files_expired)

        # when the filesystem gpfsbeat looked at is not actually something that is in the
        # config file, we shoud ignore it
        try:
            replication_factor = self.replication_factors[usage.filesystem]
        except KeyError:
            logging.warning(f"Skipping usage for filesystem {usage.filesystem}")
            return None

        # TODO: check if we should address the inode usage in relation to the replication factor (ideally: no)
        usage = usage._replace(
            block_usage=int(usage.block_usage) // replication_factor,
            block_soft=int(usage.block_soft) // replication_factor,
            block_hard=int(usage.block_hard) // replication_factor,
            block_doubt=int(usage.block_doubt) // replication_factor,
            block_expired=block_expired,
            files_usage=int(usage.files_usage),
            files_soft=int(usage.files_soft),
            files_hard=int(usage.files_hard),
            files_doubt=int(usage.files_doubt),
            files_expired=files_expired,
        )

        logging.debug("Usage after replace: %s", usage)
        return usage
