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
"""Count every request a Pub/Sub actor sends and charge it to one ceiling."""

import http.client
import http.server
import threading
from types import SimpleNamespace

import pytest
import requests
import urllib3
from flink_tier3.common import Failure
from flink_tier3.pubsub import meter as request_meter
from flink_tier3.pubsub.meter import (
    BLOCK,
    LOW_WATER,
    PREBIND,
    TAIL,
    Meter,
    RequestRefused,
    admits,
    classify,
    install,
    require_metered,
)
from flink_tier3.records import Records

from ..test_lifecycle import Store

PUBSUB = "pubsub.googleapis.com"
TOPIC = "/v1/projects/flink-gcp/topics/t3-run-in-0"
STORAGE = "storage.googleapis.com"
GKE = "abc.us-central1.gke.goog"


@pytest.mark.parametrize(
    "host, path, category",
    [
        (PUBSUB, TOPIC + ":publish", "pubsub"),
        (STORAGE, "/storage/v1/b/bucket/o/x", "storage"),
        (STORAGE, "/upload/storage/v1/b/bucket/o", "storage"),
        ("oauth2.googleapis.com", "/token", "credential"),
        ("sts.googleapis.com", "/v1/token", "credential"),
        ("iamcredentials.googleapis.com", "/v1/projects/-/x", "credential"),
        ("www.googleapis.com", "/oauth2/v2/userinfo", "credential"),
        ("metadata.google.internal", "/computeMetadata/v1/x", "credential"),
        ("169.254.169.254", "/", "credential"),
        ("cloudresourcemanager.googleapis.com", "/v1/projects/1", "credential"),
        ("pipelines.actions.githubusercontent.com", "/token", "credential"),
        (GKE, "/api/v1/namespaces/tier3-pubsub/pods", "kubernetes"),
        (GKE, "/api/v1/namespaces/tier3-pubsub/pods/p/log", "kubernetes"),
        (
            "kubernetes.default.svc",
            "/api/v1/namespaces/tier3-pubsub/services/run-rest:8081/proxy/jobs",
            "flink-rest",
        ),
        (
            "artifactregistry.googleapis.com",
            (
                "/v1/projects/flink-gcp/locations/us-central1/repositories/"
                "flink-tier3/packages/p/versions/v"
            ),
            "registry",
        ),
    ],
)
def test_each_known_destination_has_its_category(host, path, category):
    assert classify(host, path) == category


@pytest.mark.parametrize(
    "host, path",
    [
        (PUBSUB, "/v1/projects/other/topics/t"),
        ("www.googleapis.com", "/storage/v1/b"),
        ("artifactregistry.googleapis.com", "/v1/projects/flink-gcp/locations/eu/x"),
        ("bigquery.googleapis.com", "/bigquery/v2/projects/flink-gcp"),
        ("evil.us-central1.gke.goog.example", "/api"),
        ("evil-us-central1.gke.goog", "/api"),
        ("xkubernetes.default.svc", "/api"),
        ("example.com", "/"),
        (None, "/"),
    ],
)
def test_any_other_destination_is_refused(host, path):
    with pytest.raises(RequestRefused, match="Unmetered request destination"):
        classify(host, path)


@pytest.mark.parametrize(
    "category, method, path, admitting",
    [
        ("pubsub", "PUT", TOPIC, True),
        ("pubsub", "POST", TOPIC + ":publish", True),
        ("pubsub", "POST", TOPIC + ":pull", True),
        ("pubsub", "POST", TOPIC + ":acknowledge", True),
        ("pubsub", "POST", TOPIC + ":setIamPolicy", True),
        ("pubsub", "POST", TOPIC + ":testIamPermissions", False),
        ("pubsub", "GET", TOPIC, False),
        ("pubsub", "DELETE", TOPIC, False),
        ("kubernetes", "POST", "/api/v1/namespaces/tier3-pubsub/pods", True),
        ("kubernetes", "POST", "/apis/authorization.k8s.io/v1/x", False),
        ("kubernetes", "DELETE", "/api/v1/namespaces/tier3-pubsub/pods/p", False),
        ("kubernetes", "PATCH", "/api/v1/namespaces/tier3-pubsub/x", False),
        ("flink-rest", "POST", "/jobs/x/savepoints", True),
        ("flink-rest", "GET", "/jobs?x=1", False),
        ("storage", "POST", "/upload", False),
        ("credential", "POST", "/token", False),
    ],
)
def test_only_requests_that_admit_work_stop_at_the_ceiling(
    category, method, path, admitting
):
    assert admits(category, method, path) is admitting


class Control:
    """A run control record that sends the requests a conditional update does."""

    def __init__(self, store=None):
        self.store = store or object()
        self.path = "_control/runs/run.json"
        self.record = SimpleNamespace(requests=None, stop_requested=False)
        self.updates = 0
        self.fail = None
        self.lose_ack = False
        self.meter = None

    def unfenced(self, edit):
        self.updates += 1
        # The metadata and data reads before the edit, the upload after it.
        request_meter.ACTIVE.charge(STORAGE, "GET", "/storage/v1/b/e/o/run")
        request_meter.ACTIVE.charge(STORAGE, "GET", "/storage/v1/b/e/o/run")
        if self.fail:
            raise self.fail
        edit(self.record)
        request_meter.ACTIVE.charge(STORAGE, "POST", "/upload/storage/v1/b/e/o")
        if self.lose_ack:
            raise Failure("response lost")
        return self.record

    @property
    def state(self):
        return self.record.requests


def bind(meter, control, limit, *, first=False):
    meter.attach(run(control, limit), first=first)


def run(control, limit=100000):
    """The environment of the run `control` holds, as an actor sees it."""
    return SimpleNamespace(
        records=control,
        approval=SimpleNamespace(pubsub_trial={"total_request_limit": limit}),
    )


@pytest.fixture
def metered():
    meter = Meter("runner:1-1:start")
    with install(meter):
        yield meter


def send(meter, count, method="GET", path=TOPIC):
    for _ in range(count):
        meter.charge(PUBSUB, method, path)


def test_requests_before_binding_are_charged_to_the_first_block(metered):
    send(metered, 10)
    control = Control()
    bind(metered, control, 100000, first=True)
    # Ten before, the update's two reads, and its write.
    assert control.state == {
        "version": 1,
        "limit": 100000,
        "block": BLOCK,
        "reserved": 13 + BLOCK,
        "over": 0,
        "exhausted": False,
        "incomplete": False,
        "actors": {
            "runner:1-1:start": {
                "reserved": 13 + BLOCK,
                "used": {"pubsub": 10, "storage": 2},
            }
        },
    }
    assert metered.headroom == BLOCK
    assert not control.record.stop_requested


def test_an_unbound_meter_refuses_past_its_allowance(metered):
    send(metered, PREBIND)
    with pytest.raises(RequestRefused, match="unbound"):
        send(metered, 1)


def test_an_actor_that_did_not_dispatch_marks_the_meter_incomplete(metered):
    control = Control()
    bind(metered, control, 100000)
    assert control.state["incomplete"] is True


def test_a_spent_block_reserves_the_next_before_the_request(metered):
    control = Control()
    bind(metered, control, 100000, first=True)
    send(metered, BLOCK)
    assert control.updates == 1 and metered.headroom == 0
    send(metered, 1)
    assert control.updates == 2
    assert control.state["reserved"] == 2 * (3 + BLOCK)
    assert metered.headroom == BLOCK - 1


def test_a_grant_leaving_less_than_the_low_water_stops_the_run(metered):
    control = Control()
    bind(metered, control, 3 + BLOCK + 3 + LOW_WATER - 1, first=True)
    assert not metered.exhausted
    send(metered, BLOCK)
    send(metered, 1)
    assert metered.exhausted and control.state["exhausted"]
    assert control.record.stop_requested
    # What admits new work is refused; stop, cleanup and reads continue.
    with pytest.raises(
        RequestRefused, match="reached the run's ceiling; refusing POST"
    ):
        send(metered, 1, "POST", TOPIC + ":publish")
    send(metered, 1, "DELETE")
    metered.charge(GKE, "DELETE", "/api/v1/namespaces/tier3-pubsub/pods/p")


def test_requests_past_an_exhausted_ceiling_are_recorded_as_over(metered):
    control = Control()
    bind(metered, control, 3 + LOW_WATER - 1, first=True)
    assert metered.exhausted and control.state["reserved"] == 3 + LOW_WATER - 1
    # Spent headroom alone records nothing; a low-water's worth is flushed.
    send(metered, LOW_WATER - 1 + LOW_WATER, "DELETE")
    assert control.updates == 1
    send(metered, 1, "DELETE")
    assert control.updates == 2
    assert control.state["over"] == LOW_WATER + 2 + 1
    assert control.state["reserved"] == 3 + LOW_WATER - 1


def test_a_failed_reservation_closes_admission_once_headroom_is_spent(metered):
    control = Control()
    bind(metered, control, 100000, first=True)
    control.fail = Failure("storage unavailable")
    send(metered, BLOCK)
    assert not metered.closed
    send(metered, 1)
    assert metered.closed and not control.record.stop_requested
    with pytest.raises(RequestRefused):
        metered.charge(GKE, "POST", "/api/v1/namespaces/tier3-pubsub/pods")
    send(metered, 1, "DELETE")
    # Retried half a low-water later; its grant records what was sent
    # meanwhile and reopens admission.
    control.fail = None
    send(metered, LOW_WATER // 2 - 2, "DELETE")
    assert control.updates == 2
    send(metered, 1, "DELETE")
    assert control.updates == 3 and not metered.closed
    # The failed update's reads, the two requests after it, those until the
    # retry, and the retry's reads and write.
    owed = 2 + 2 + (LOW_WATER // 2 - 2) + 3
    assert control.state["reserved"] == (3 + BLOCK) + (owed + BLOCK)


def test_a_lost_acknowledgement_is_charged_again(metered):
    control = Control()
    control.lose_ack = True
    bind(metered, control, 100000, first=True)
    assert metered.granted == 0 and control.state["reserved"] == 3 + BLOCK
    control.lose_ack = False
    send(metered, LOW_WATER // 2 + 1)
    assert control.state["reserved"] == (3 + BLOCK) + (3 + LOW_WATER // 2 + 3) + BLOCK


@pytest.mark.parametrize(
    "state",
    [
        {"version": 1, "limit": 99999, "block": BLOCK, "actors": {}},
        {"version": 1, "limit": 100000, "block": 256, "actors": {}},
        {"version": 2, "limit": 100000, "block": BLOCK, "actors": {}},
        {"version": 1, "limit": 100000, "block": BLOCK, "actors": []},
        [],
    ],
)
def test_a_replaced_meter_binding_is_not_reserved_from(metered, state):
    _refused(metered, state)


VALID = {
    "version": 1,
    "limit": 100000,
    "block": BLOCK,
    "reserved": 0,
    "over": 0,
    "exhausted": False,
    "incomplete": False,
    "actors": {},
}


@pytest.mark.parametrize(
    "change",
    [
        {"reserved": "0"},
        {"reserved": -1},
        {"reserved": 100001},
        {"reserved": True},
        {"over": -1},
        {"exhausted": 0},
        {"incomplete": None},
        {"actors": {"runner:x": []}},
        {"actors": {"runner:x": {"reserved": "1"}}},
        # Every grant adds to its actor exactly what it adds to the total.
        {"actors": {"runner:x": {"reserved": 100000, "used": {}}}},
        {"reserved": 5, "over": 1, "actors": {"runner:x": {"reserved": 5, "used": {}}}},
        # Nothing goes over the ceiling without exhausting it.
        {"reserved": 4, "over": 1, "actors": {"runner:x": {"reserved": 5, "used": {}}}},
        # A grant covers what its actor had sent.
        {
            "reserved": 5,
            "actors": {"runner:x": {"reserved": 5, "used": {"pubsub": 6}}},
        },
        {"reserved": 5, "actors": {"runner:x": {"reserved": 5, "used": []}}},
        {
            "reserved": 5,
            "actors": {"runner:x": {"reserved": 5, "used": {"pubsub": -1}}},
        },
        # Shares are grants: none is negative, whatever the sum.
        {
            "reserved": 5,
            "actors": {
                "runner:x": {"reserved": 100005, "used": {}},
                "y": {"reserved": -100000, "used": {}},
            },
        },
    ],
)
def test_a_forged_meter_state_is_refused_not_trusted(metered, change):
    _refused(metered, {**VALID, **change})


def _refused(metered, state):
    control = Control()
    control.record.requests = state
    bind(metered, control, 100000, first=True)
    assert metered.granted == 0 and metered.closed
    assert control.record.requests == state


def test_the_actor_table_is_bounded(metered):
    control = Control()
    control.record.requests = {
        "version": 1,
        "limit": 100000,
        "block": BLOCK,
        "reserved": 0,
        "over": 0,
        "exhausted": False,
        "incomplete": False,
        "actors": {f"supervisor:{n}": {} for n in range(32)},
    }
    bind(metered, control, 100000, first=True)
    assert metered.closed and metered.granted == 0


def test_a_bounded_actor_reserves_its_whole_allowance_first(metered):
    control = Control()
    bind(metered, control, 3 + BLOCK + 528, first=True)
    metered.charge_external("probe", 528)
    assert control.state["actors"]["probe"] == {"reserved": 528, "used": {}}
    with pytest.raises(Failure, match="already reserved"):
        metered.charge_external("probe", 1)
    metered.report_external("probe", {"pubsub": 8, "credential": 4})
    assert control.state["actors"]["probe"]["used"] == {"pubsub": 8, "credential": 4}
    with pytest.raises(Failure, match="cannot cover other"):
        metered.charge_external("other", 1)


def test_a_pubsub_actor_needs_the_meter_of_its_own_run(metered):
    control = Control()
    env = run(control)
    with pytest.raises(Failure, match="request meter"):
        require_metered(env)
    metered.attach(env, first=True)
    assert require_metered(env) is metered and control.meter is metered
    # Another environment of the same run shares the process's meter.
    other = Control(control.store)
    metered.attach(run(other))
    assert require_metered(run(other)) is metered and other.meter is metered
    # Bound, but not attached to this environment's records.
    detached = Control(control.store)
    with pytest.raises(Failure, match="request meter"):
        require_metered(run(detached))
    for change in ({"path": "_control/runs/other.json"}, {"store": object()}):
        foreign = Control()
        vars(foreign).update(change)
        if "store" not in change:
            foreign.store = control.store
        with pytest.raises(Failure, match="bound to another run"):
            metered.attach(run(foreign))
        foreign.meter = metered
        with pytest.raises(Failure, match="request meter"):
            require_metered(run(foreign))
    with pytest.raises(Failure, match="bound to another run"):
        metered.attach(run(Control(control.store), 99999))


def test_an_attachment_needs_the_approved_ceiling(metered):
    for limit in (0, True, "100000", None):
        with pytest.raises(Failure, match="approved ceiling"):
            metered.attach(run(Control(), limit))
    assert metered.records is None


class Sent(Exception):
    """What the fake transport raises once a request would leave."""


@pytest.fixture
def transport(monkeypatch):
    sent = []

    def urlopen(pool, method, url, *args, **kwargs):
        sent.append((pool.host, method, url))
        if len(sent) == 1 and kwargs.get("retry_once"):
            # urllib3 retries by calling the pool's urlopen again.
            return pool.urlopen(method, url)
        raise Sent

    monkeypatch.setattr(request_meter, "_urlopen", urlopen)
    return sent


def test_library_built_sessions_are_metered_at_the_transport(metered, transport):
    with pytest.raises(Sent):
        requests.Session().get("https://" + PUBSUB + TOPIC)
    assert transport == [(PUBSUB, "GET", TOPIC)]
    assert metered.counts["pubsub"] == 1
    with pytest.raises(RequestRefused):
        requests.Session().get("https://example.com/")
    assert len(transport) == 1 and metered.sent == 1


def test_a_transport_retry_is_counted_again(metered, transport):
    pool = urllib3.HTTPSConnectionPool(STORAGE)
    with pytest.raises(Sent):
        pool.urlopen("GET", "/storage/v1/b", retry_once=True)
    assert metered.counts["storage"] == 2


def test_installing_restores_the_transport_on_exit():
    original = urllib3.connectionpool.HTTPConnectionPool.urlopen
    putrequest = http.client.HTTPConnection.putrequest
    with install(Meter("supervisor:x")) as meter:
        assert urllib3.connectionpool.HTTPConnectionPool.urlopen is not original
        with pytest.raises(Failure, match="already installed"), install(meter):
            pass
        # A request around the pool never reaches the socket.
        with pytest.raises(RequestRefused, match="outside the metered"):
            http.client.HTTPConnection("localhost").putrequest("GET", "/")
    assert urllib3.connectionpool.HTTPConnectionPool.urlopen is original
    assert http.client.HTTPConnection.putrequest is putrequest
    assert request_meter.ACTIVE is None


def test_a_spent_grant_waiting_for_its_retry_admits_nothing(metered):
    control = Control()
    bind(metered, control, 100000, first=True)
    send(metered, BLOCK - 20)
    control.fail = Failure("storage unavailable")
    metered.top_up()
    # The failed top-up spent two reads; the retry is half a low-water away.
    assert not metered.closed and metered.headroom == 18
    send(metered, metered.headroom)
    with pytest.raises(
        RequestRefused, match="could not renew its grant; refusing POST"
    ):
        send(metered, 1, "POST", TOPIC + ":pull")
    assert metered.closed and control.updates == 2
    send(metered, 1, "DELETE")


def test_finalization_reserves_its_tail_then_reserves_nothing(metered):
    control = Control()
    bind(metered, control, 100000, first=True)
    send(metered, BLOCK - 100)
    with metered.freeze():
        assert control.updates == 2 and metered.headroom >= TAIL
        send(metered, metered.headroom + 5, "DELETE")
        assert control.updates == 2
        with pytest.raises(RequestRefused):
            send(metered, 1, "POST", TOPIC + ":publish")
    send(metered, 1)
    assert control.updates == 3 and not metered.closed


def test_an_ending_process_records_what_it_sent_past_its_grant(metered):
    control = Control()
    bind(metered, control, 3 + LOW_WATER - 1, first=True)
    send(metered, LOW_WATER - 1 + 10, "DELETE")
    metered.flush()
    assert control.state["over"] == 10 + 2 + 1
    # The flush's own write is in what it recorded.
    updates = control.updates
    metered.flush()
    assert control.updates == updates
    # Nothing owed, nothing written.
    healthy = Meter("supervisor:y")
    request_meter.ACTIVE = healthy
    other = Control()
    bind(healthy, other, 100000, first=True)
    healthy.flush()
    assert other.updates == 1
    request_meter.ACTIVE = metered


def test_the_transport_guard_belongs_to_each_thread(metered, monkeypatch):
    entered, released = threading.Event(), threading.Event()
    failures = []

    def urlopen(pool, method, url, *args, **kwargs):
        if pool.host == STORAGE:
            # The library's own thread: in flight while the caller enters,
            # done before the caller sends.
            entered.set()
            released.wait(5)
            return "background"
        thread.start()
        entered.wait(5)
        released.set()
        thread.join(5)
        try:
            request_meter._tripwire(http.client.HTTPConnection("localhost"), "GET", "/")
        except RequestRefused as error:
            failures.append(error)
        except OSError:
            pass
        return "caller"

    monkeypatch.setattr(request_meter, "_urlopen", urlopen)
    monkeypatch.setattr(request_meter, "_putrequest", lambda *a, **k: None)
    background = urllib3.HTTPSConnectionPool(STORAGE)
    thread = threading.Thread(target=lambda: background.urlopen("GET", "/storage/v1/b"))
    assert urllib3.HTTPSConnectionPool(PUBSUB).urlopen("GET", TOPIC) == "caller"
    assert failures == []
    assert metered.counts["storage"] == metered.counts["pubsub"] == 1
    with pytest.raises(RequestRefused, match="outside the metered"):
        request_meter._tripwire(http.client.HTTPConnection("localhost"), "GET", "/")


def test_a_real_request_passes_the_pool_and_its_guard(metered, monkeypatch):
    class Handler(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(204)
            self.end_headers()

        def log_message(self, *args):
            pass

    server = http.server.HTTPServer(("127.0.0.1", 0), Handler)
    serving = threading.Thread(target=server.serve_forever, daemon=True)
    serving.start()
    try:
        monkeypatch.setitem(request_meter.HOSTS, "127.0.0.1", ("storage", "/"))
        pool = urllib3.HTTPConnectionPool("127.0.0.1", server.server_port)
        assert pool.urlopen("GET", "/x", retries=False).status == 204
        assert metered.counts["storage"] == 1
    finally:
        server.shutdown()


@pytest.fixture
def records():
    approval = SimpleNamespace(run_id="run", nonce="n" * 32)
    store = Store()
    store.write(
        "_control/runs/run.json",
        {"nonce": approval.nonce, "phase": "approved", "roots": {}, "observed": {}},
    )
    return Records(store, approval)


def reserved(records):
    return records.store.read(records.path)[0]["requests"]["reserved"]


@pytest.mark.parametrize("io", ["read", "update"])
def test_control_io_tops_up_a_low_grant_first(metered, records, io):
    metered.attach(run(records), first=True)
    first = reserved(records)

    def control():
        if io == "read":
            records.read()
        else:
            records._update(lambda record: None)

    send(metered, metered.headroom - 2 * LOW_WATER)
    control()
    assert reserved(records) == first
    send(metered, 1)
    control()
    assert reserved(records) > first
    # Past the ceiling, control I/O no longer reserves.
    metered.exhausted = True
    after = reserved(records)
    send(metered, metered.headroom)
    control()
    assert reserved(records) == after


def test_a_well_formed_meter_state_is_reserved_from(metered):
    control = Control()
    control.record.requests = {
        **VALID,
        "reserved": 5,
        "actors": {"runner:x": {"reserved": 5, "used": {"pubsub": 5}}},
    }
    bind(metered, control, 100000, first=True)
    assert metered.granted == 3 + BLOCK and not metered.closed
    # Over an exhausted ceiling: recorded as over, not refused.
    request_meter.ACTIVE = other = Meter("supervisor:y")
    control = Control()
    control.record.requests = {
        **VALID,
        "reserved": 4,
        "over": 1,
        "exhausted": True,
        "actors": {"runner:x": {"reserved": 5, "used": {"pubsub": 5}}},
    }
    bind(other, control, 100000)
    assert other.exhausted and control.state["over"] == 1 + 3
    request_meter.ACTIVE = metered
