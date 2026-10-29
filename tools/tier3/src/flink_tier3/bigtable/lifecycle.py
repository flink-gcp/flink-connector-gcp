#
# Copyright 2026 The flink-gcp authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Persist the run's instance intent, its readback and its cleanup."""

from __future__ import annotations

from ..common import ApiError, Failure, transient
from ..resource import InstanceLifecycle
from .instance import ROUTING, TABLES
from .trial import APP_PROFILE, EXACTLY_ONCE_TABLE


class BigtableLifecycle(InstanceLifecycle):
    """The run's instance, from intent to deletion, in the run's control record.

    The runner creates the instance, waits for it to be ready, creates its
    tables and app profile and reads all three back before the application
    exists. Deleting the instance deletes what it contains, so the instance is
    the one resource the intent names; the supervisor first disables `eo`'s
    change stream, without which the service refuses the deletion.
    """

    scenario = "bigtable-recovery"

    def _populate(self, api, deadline):
        """Wait for the instance, create its tables and profile, and read them back."""
        self._ready(api.get, deadline)
        for table in TABLES:
            self._created(
                lambda table=table: api.create_table(table),
                lambda table=table: api.table(table),
                deadline,
            )
        self._created(api.create_app_profile, api.app_profile, deadline)
        return self._readback(api, deadline)

    def _readback(self, api, deadline):
        """The tables' families and change stream and the app profile's routing.

        A family's type is the service's to report in its own shape, so only
        the family names are compared; the change stream and the routing are
        what the application cannot run without.
        """
        tables = {}
        for table, expected in TABLES.items():
            readback = self._settled(lambda table=table: api.table(table), deadline)
            families = sorted((readback or {}).get("columnFamilies", {}))
            stream = (readback or {}).get("changeStreamConfig")
            if families != sorted(expected["columnFamilies"]) or (
                table == EXACTLY_ONCE_TABLE
            ) != (stream is not None):
                self.env.emit("bigtable-table-deviation", {table: readback})
                raise Failure(f"Bigtable table {table} differs from its configuration")
            tables[table] = {"families": families, "changeStreamConfig": stream}
        profile = self._settled(api.app_profile, deadline)
        routing = (profile or {}).get("singleClusterRouting")
        if routing != ROUTING:
            self.env.emit("bigtable-app-profile-deviation", {"readback": profile})
            raise Failure("Bigtable app profile differs from its configuration")
        return {"tables": tables, "app_profile": {APP_PROFILE: routing}}

    def delete(self, deadline):
        """Disable the change stream, then delete the instance.

        The service refuses to delete an instance while a change stream is
        enabled in it, and disabling one is itself an operation, which another
        disable may meet still running. A deletion or disable refused with 400
        or 409, and a call that said nothing, are left to the caller's next
        pass, which reads the change stream again before disabling it.
        """
        api = self.api.with_deadline(deadline)
        try:
            if api.get() is None:
                return True
            table = api.table(EXACTLY_ONCE_TABLE)
            if table is not None and table.get("changeStreamConfig"):
                api.disable_change_stream()
            api.delete()
            return api.get() is None
        except Failure as error:
            refused = isinstance(error, ApiError) and error.status in (400, 409)
            if not (refused or transient(error)):
                raise
            self.env.emit("bigtable-delete-pending", {"cause": str(error)})
            return False
