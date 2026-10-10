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
"""Meter every request a Pub/Sub actor process sends against one ceiling."""

import http.client
import re
import threading
from contextlib import contextmanager

import urllib3.connectionpool

from ..common import Failure

# Requests one reservation grants, and the headroom below which a grant
# stops admission: a reservation costs three requests and at worst 55, plus
# a credential refresh.
BLOCK = 512
LOW_WATER = 80
ACTORS = 32
# Requests an actor may send before it can reserve: the runner's dispatch
# checks before the lock, or the supervisor's before its environment exists.
PREBIND = 1024
# Requests reserved before finalization: the lock and record reads, the idle
# inventory, the receipt and the record's deletion.
TAIL = 256
CATEGORIES = ("pubsub", "storage", "credential", "kubernetes", "flink-rest", "registry")
# Each destination host, with the path prefix its requests must carry;
# anything else is refused unsent.
HOSTS = {
    "pubsub.googleapis.com": ("pubsub", "/v1/projects/flink-gcp/"),
    "storage.googleapis.com": ("storage", "/"),
    "oauth2.googleapis.com": ("credential", "/"),
    "sts.googleapis.com": ("credential", "/"),
    "iamcredentials.googleapis.com": ("credential", "/"),
    "www.googleapis.com": ("credential", "/oauth2/"),
    "metadata.google.internal": ("credential", "/"),
    "169.254.169.254": ("credential", "/"),
    # External-account credentials look up their project number here.
    "cloudresourcemanager.googleapis.com": ("credential", "/v1/projects/"),
    "artifactregistry.googleapis.com": (
        "registry",
        "/v1/projects/flink-gcp/locations/us-central1/repositories/flink-tier3/",
    ),
    "kubernetes.default.svc": ("kubernetes", "/"),
}
SUFFIXES = {
    # The GitHub Actions OIDC token the runner exchanges for Google access.
    ".actions.githubusercontent.com": "credential",
    ".us-central1.gke.goog": "kubernetes",
}
PROXY = re.compile(r"/api/v1/namespaces/[^/]+/services/[^/]+/proxy/")
# Pub/Sub and Kubernetes requests that admit new work, refused once the
# ceiling is reached; stop, cleanup and settlement requests still go out.
ADMITTING = re.compile(r".*:(publish|pull|acknowledge|setIamPolicy)$")


class RequestRefused(Failure):
    """A request the meter refused before it was sent."""


def classify(host, path):
    """The category of a request, or refusal before anything is sent."""
    host = (host or "").lower()
    path = path or ""
    category, prefix = HOSTS.get(host, (None, None))
    if category is None:
        category = next(
            (c for suffix, c in SUFFIXES.items() if host.endswith(suffix)), None
        )
        prefix = "/"
    if category is None or not path.startswith(prefix):
        raise RequestRefused(f"Unmetered request destination: {host}")
    if category == "kubernetes" and PROXY.match(path):
        return "flink-rest"
    return category


def admits(category, method, path):
    path = path.split("?", 1)[0]
    if category == "pubsub":
        return method == "PUT" or bool(ADMITTING.match(path))
    if category == "kubernetes":
        return method == "POST" and "/namespaces/" in path
    return category == "flink-rest" and method != "GET"


def malformed(state, limit):
    """Whether a durable meter state is not one this meter could have written.

    It is JSON that nothing types on the way in: a count that is not a whole
    number in range would grant past the ceiling or fail later; every grant
    adds to its actor's share exactly what it adds to the aggregate and
    covers what the actor had sent, and nothing goes over the ceiling without
    exhausting it.
    """
    if (
        not isinstance(state, dict)
        or state.get("version") != 1
        or state.get("limit") != limit
        or state.get("block") != BLOCK
        or type(state.get("reserved")) is not int
        or not 0 <= state["reserved"] <= limit
        or type(state.get("over")) is not int
        or state["over"] < 0
        or type(state.get("exhausted")) is not bool
        or type(state.get("incomplete")) is not bool
        or (state["over"] and not state["exhausted"])
        or not isinstance(state.get("actors"), dict)
    ):
        return True
    actors = state["actors"].values()
    return not all(
        isinstance(actor, dict)
        and type(actor.get("reserved")) is int
        and actor["reserved"] >= 0
        and isinstance(actor.get("used"), dict)
        and all(type(n) is int and n >= 0 for n in actor["used"].values())
        and sum(actor["used"].values()) <= actor["reserved"]
        for actor in actors
    ) or sum(actor["reserved"] for actor in actors) != (
        state["reserved"] + state["over"]
    )


class Meter:
    """One process's share of the run's durable request aggregate.

    Every request the process sends, from any of its threads, is counted at
    the transport, after the client libraries' own retries, and charged
    against blocks reserved from the run's control record by conditional
    update. A grant is never refunded. A grant that would leave less than
    the low-water headroom stops the run; past it, and past any grant the
    process could not renew, only requests that admit new work are refused,
    and stop, cleanup and settlement are recorded as over the ceiling.
    """

    def __init__(self, actor):
        self.actor = actor
        self.counts = dict.fromkeys(CATEGORIES, 0)
        self.sent = self.granted = self.retry_at = 0
        self.records = self.limit = None
        self.first = False
        self.exhausted = self.closed = self.reserving = self.frozen = False
        # A credential library may send from its own thread while the
        # process's request is in flight.
        self.lock = threading.RLock()

    @property
    def headroom(self):
        return self.granted - self.sent

    @property
    def stopped(self):
        """Whether the meter refuses new work: past the ceiling or its grant."""
        return self.exhausted or self.closed

    def attach(self, env, *, first=False):
        """Charge this process's requests to `env`'s run and stop it on them.

        The first attachment binds the meter to the run and charges
        everything sent so far; `first` says the caller is the runner that
        dispatched the run, so the aggregate begins with its requests.
        """
        limit = env.approval.pubsub_trial["total_request_limit"]
        with self.lock:
            if self.records is None:
                if type(limit) is not int or limit <= 0:
                    raise Failure("Request meter needs the approved ceiling")
                self.records, self.limit, self.first = env.records, limit, first
                self.reserve()
            elif not self.bound_to(env):
                raise Failure("Request meter is bound to another run")
        env.records.meter = self

    def bound_to(self, env):
        return (
            self.records is not None
            and self.records.store is env.records.store
            and self.records.path == env.records.path
            and self.limit == env.approval.pubsub_trial["total_request_limit"]
        )

    def _due(self):
        if (
            self.records is None
            or self.reserving
            or self.frozen
            or self.sent < self.retry_at
        ):
            return False
        # Past the ceiling, only flush what was sent, a low-water at a time.
        if self.exhausted:
            return -self.headroom >= LOW_WATER
        return self.headroom <= 0

    def charge(self, host, method, path):
        category = classify(host, path)
        with self.lock:
            if self.records is None and self.sent >= PREBIND:
                raise RequestRefused("Requests exceeded the unbound allowance")
            if self._due():
                self.reserve()
            if (
                self.records is not None
                and not self.reserving
                and not self.exhausted
                and self.headroom <= 0
            ):
                # Spent, and not renewed: a reservation failed or waits.
                self.closed = True
            if self.stopped and admits(category, method, path):
                cause = (
                    "has reached the run's ceiling"
                    if self.exhausted
                    else "could not renew its grant"
                )
                raise RequestRefused(
                    f"The request meter {cause}; refusing {method} {category}"
                )
            self.sent += 1
            self.counts[category] += 1

    def top_up(self, need=2 * LOW_WATER):
        """Reserve at a safe point, before control I/O, while headroom is low."""
        with self.lock:
            if (
                self.records is not None
                and not self.reserving
                and not self.frozen
                and not self.exhausted
                and self.sent >= self.retry_at
                and self.headroom < need
            ):
                self.reserve()

    def flush(self):
        """Record what was sent since the last grant, as a process ends."""
        with self.lock:
            if self.records is not None and not self.frozen and self.headroom < 0:
                self.reserve()

    @contextmanager
    def freeze(self):
        """Reserve nothing while the caller reads the record it will delete.

        Finalization reserves its tail first; what it sends past that is not
        recorded, because the record it would be recorded in is deleted.
        """
        self.top_up(TAIL)
        self.frozen = True
        try:
            yield
        finally:
            self.frozen = False

    def _state(self, record):
        state = record.requests
        if state is None:
            state = record.requests = {
                "version": 1,
                "limit": self.limit,
                "block": BLOCK,
                "reserved": 0,
                "over": 0,
                "exhausted": False,
                "incomplete": not self.first,
                "actors": {},
            }
        if malformed(state, self.limit):
            raise Failure("Missing or replaced request meter binding")
        if self.actor not in state["actors"] and len(state["actors"]) >= ACTORS:
            raise Failure("Request meter has too many actors")
        return state

    def reserve(self):
        """Grant a block by conditional update; a failure only spends headroom."""
        granted = None

        def edit(record):
            nonlocal granted
            state = self._state(record)
            actor = state["actors"].setdefault(self.actor, {"reserved": 0, "used": {}})
            # Everything sent since the last grant, this update's reads
            # included, and one more for its write.
            owed = self.sent - self.granted + 1
            room = state["limit"] - state["reserved"]
            take = 0 if state["exhausted"] else max(0, min(room, owed + BLOCK))
            over = max(0, owed - take)
            state["reserved"] += take
            state["over"] += over
            actor["reserved"] += take + over
            actor["used"] = {k: v for k, v in self.counts.items() if v}
            if over or take - owed < LOW_WATER:
                state["exhausted"] = True
                record.stop_requested = True
            granted = (take + over, state["exhausted"])

        with self.lock:
            self.reserving = True
            try:
                self.records.unfenced(edit)
            except (Failure, ValueError, OSError):
                # Retried after half a low-water more; until then the headroom
                # left is spent, and nothing past it admits work.
                self.retry_at = self.sent + LOW_WATER // 2
                if self.headroom <= 0:
                    self.closed = True
                return False
            finally:
                self.reserving = False
            self.granted += granted[0]
            self.exhausted = granted[1]
            self.closed = False
            return True

    def charge_external(self, name, requests):
        """Reserve a bounded actor's whole allowance before it starts."""

        def edit(record):
            state = self._state(record)
            if name in state["actors"]:
                raise Failure("Request allowance already reserved for " + name)
            if state["exhausted"] or state["reserved"] + requests > state["limit"]:
                raise Failure("The run's request ceiling cannot cover " + name)
            state["reserved"] += requests
            state["actors"][name] = {"reserved": requests, "used": {}}

        self.records.unfenced(edit)

    def report_external(self, name, used):
        """Record what a bounded actor reported it sent."""

        def edit(record):
            actor = self._state(record)["actors"].get(name)
            if actor is None:
                raise Failure("No request allowance was reserved for " + name)
            actor["used"] = dict(used)

        self.records.unfenced(edit)


ACTIVE = None
# Whether this thread is inside the metered pool, where http.client may send.
_ARMED = threading.local()
_urlopen = urllib3.connectionpool.HTTPConnectionPool.urlopen
_putrequest = http.client.HTTPConnection.putrequest


def _metered(pool, method, url, *args, **kwargs):
    if ACTIVE is not None:
        ACTIVE.charge(pool.host, method, url)
    armed = getattr(_ARMED, "value", False)
    _ARMED.value = True
    try:
        return _urlopen(pool, method, url, *args, **kwargs)
    finally:
        _ARMED.value = armed


def _tripwire(connection, method, url, *args, **kwargs):
    if ACTIVE is not None and not getattr(_ARMED, "value", False):
        raise RequestRefused("Request outside the metered transport")
    return _putrequest(connection, method, url, *args, **kwargs)


@contextmanager
def install(meter):
    """Route every request of this process through `meter` until exit."""
    global ACTIVE
    if ACTIVE is not None:
        raise Failure("A request meter is already installed")
    ACTIVE = meter
    urllib3.connectionpool.HTTPConnectionPool.urlopen = _metered
    http.client.HTTPConnection.putrequest = _tripwire
    try:
        yield meter
    finally:
        urllib3.connectionpool.HTTPConnectionPool.urlopen = _urlopen
        http.client.HTTPConnection.putrequest = _putrequest
        ACTIVE = None


def require_metered(env):
    """Refuse a Pub/Sub actor whose process is not metered for this run."""
    meter = ACTIVE
    if meter is None or env.records.meter is not meter or not meter.bound_to(env):
        raise Failure("Pub/Sub actor requires the run's request meter")
    return meter
