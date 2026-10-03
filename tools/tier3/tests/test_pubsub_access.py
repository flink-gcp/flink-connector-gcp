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
"""Each identity's expected access, and the probe Pod's program and its reading."""

import ast
import json
from types import SimpleNamespace

import pytest
from flink_tier3 import pubsub_probe as probe
from flink_tier3.bundle import delivered_sources, package_sources
from flink_tier3.common import Failure
from flink_tier3.pubsub import ResourcePlan
from flink_tier3.pubsub_access import (
    CONSUME,
    MEMBERS,
    PUBLISH,
    evaluate_workload_log,
    expectations,
    probe_spec,
    pulls,
)

PLAN = ResourcePlan("access-1581", "a" * 32)
NAMES = {
    "in-0": "projects/flink-gcp/topics/t3-access-1581-in-0",
    "in-1": "projects/flink-gcp/topics/t3-access-1581-in-1",
    "out": "projects/flink-gcp/topics/t3-access-1581-out",
    "sub-in-0": "projects/flink-gcp/subscriptions/t3-access-1581-in-0",
    "sub-in-1": "projects/flink-gcp/subscriptions/t3-access-1581-in-1",
    "sub-out": "projects/flink-gcp/subscriptions/t3-access-1581-out",
}


@pytest.mark.parametrize(
    "role, held",
    [
        ("runner", {"in-0", "in-1"}),
        ("workload", {"out", "sub-in-0", "sub-in-1"}),
        ("supervisor", {"sub-out"}),
    ],
)
def test_each_identity_must_hold_exactly_its_bindings(role, held):
    """Every other resource is a negative control for the same permission."""
    expected = expectations(PLAN, role)
    assert set(expected) == set(NAMES.values())
    for key, name in NAMES.items():
        want = expected[name]
        assert want["permission"] == (CONSUME if key.startswith("sub-") else PUBLISH)
        assert want["held"] is (key in held), key


def test_only_the_subscriptions_an_identity_reads_are_pulled():
    assert pulls(PLAN, "runner") == []
    assert pulls(PLAN, "workload") == [NAMES["sub-in-0"], NAMES["sub-in-1"]]
    assert pulls(PLAN, "supervisor") == [NAMES["sub-out"]]


def log(*events):
    return "\n".join(json.dumps(event) for event in events)


def passing(**change):
    expected = expectations(PLAN, "workload")
    events = [
        {"event": "identity", "email": MEMBERS["workload"]},
        {"event": "attempt", "seen": {n: w["held"] for n, w in expected.items()}},
        {"event": "pulled", "name": NAMES["sub-in-0"]},
        {"event": "pulled", "name": NAMES["sub-in-1"]},
        {"event": "passed"},
    ]
    for event in events:
        event.update(change.get(event["event"], {}))
    return events


def test_a_passing_log_is_read_into_the_workload_s_access():
    summary = evaluate_workload_log(PLAN, log(*passing()))
    assert summary == {
        "attempts": 1,
        "pull_attempts": 2,
        "granted": sorted([NAMES["out"], NAMES["sub-in-0"], NAMES["sub-in-1"]]),
        "pulled": [NAMES["sub-in-0"], NAMES["sub-in-1"]],
    }


@pytest.mark.parametrize(
    "events",
    [
        [],
        passing(identity={"email": MEMBERS["supervisor"]}),
        passing()[:-1],
        [*passing(), {"event": "refused"}],
        passing(attempt={"seen": {NAMES["out"]: True}}),
        passing(attempt={"seen": {name: True for name in NAMES.values()}}),
        passing(attempt={"seen": {name: False for name in NAMES.values()}}),
        [e for e in passing() if e["event"] != "pulled"],
        [passing()[0], *passing()],
        ["not an object"],
    ],
)
def test_a_log_that_does_not_show_the_plan_met_is_refused(events):
    with pytest.raises(Failure):
        evaluate_workload_log(PLAN, log(*events))


def test_an_unparsable_log_is_refused():
    with pytest.raises(Failure, match="malformed"):
        evaluate_workload_log(PLAN, "{not json")


def schedule(deadline):
    """A schedule whose admission deadline is `deadline`."""
    return SimpleNamespace(started=deadline - 900, cleanup_at=deadline + 900)


def test_the_probe_spec_carries_the_plan_and_the_deadline():
    spec = probe_spec(PLAN, schedule(1234.0))
    assert spec["member"] == MEMBERS["workload"]
    assert spec["expected"] == expectations(PLAN, "workload")
    assert spec["pulls"] == pulls(PLAN, "workload")
    assert spec["deadline"] == 1234.0 and spec["interval"] == 15


def test_the_probe_program_is_self_contained():
    """It runs alone from `python3 -I -c`; no package module is beside it."""
    tree = ast.parse(package_sources()["pubsub_probe.py"])
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            assert node.level == 0 and not (node.module or "").startswith("flink_tier3")
        elif isinstance(node, ast.Import):
            assert not any(a.name.startswith("flink_tier3") for a in node.names)
    # The supervisor never runs it, so its delivery need not carry it.
    assert "pubsub_probe.py" not in delivered_sources()


def test_the_supervisor_s_source_pin_covers_the_pub_sub_actors():
    """The pin is the delivered subset; it must include what the actor runs."""
    delivered = delivered_sources()
    for name in (
        "pubsub_actors.py",
        "pubsub_auth.py",
        "pubsub_guard.py",
        "pubsub_quiesce.py",
        "pubsub_access.py",
        "actor_auth.py",
        "quiesce.py",
    ):
        assert name in delivered, name


class Session:
    """The probe's authorized session, answered from a script of statuses."""

    def __init__(self, permissions, pulls=()):
        self.permissions, self.pulls, self.calls = permissions, list(pulls), []

    def post(self, url, json, **kwargs):
        # The probe never follows a redirect, reads every answer streamed, and
        # sends the token it applied itself.
        assert kwargs == {
            "headers": {"authorization": "Bearer token"},
            "timeout": 20,
            "allow_redirects": False,
            "stream": True,
        }
        self.calls.append((url, json))
        name, _, operation = url.removeprefix(probe.BASE).partition(":")
        if operation == "testIamPermissions":
            granted = self.permissions(name, len(self.calls))
            if isinstance(granted, int):
                return Reply(granted, {})
            return Reply(200, {"permissions": granted})
        return Reply(*self.pulls.pop(0))


class Reply:
    def __init__(self, status, value):
        self.status_code, self.data = status, json.dumps(value).encode()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        pass

    def iter_content(self, chunk_size):
        yield self.data


def held(name, _count):
    want = expectations(PLAN, "workload")[name]
    return [want["permission"]] if want["held"] else []


class Clock:
    def __init__(self):
        self.now = 0.0

    def __call__(self):
        return self.now

    def sleep(self, seconds):
        self.now += seconds


def bearer(headers):
    headers["authorization"] = "Bearer token"


def run(session, capsys, email=None, deadline=900.0):
    spec = probe_spec(PLAN, schedule(deadline))
    clock = Clock()
    code = probe.run(
        spec,
        session,
        email or MEMBERS["workload"],
        bearer,
        clock=clock,
        sleep=clock.sleep,
    )
    events = [json.loads(line) for line in capsys.readouterr().out.splitlines()]
    return code, events, clock


def test_the_probe_passes_on_the_plan_s_grants_and_empty_pulls(capsys):
    session = Session(held, pulls=[(200, {}), (200, {})])
    code, events, _ = run(session, capsys)
    assert code == 0 and events[-1]["event"] == "passed"
    assert evaluate_workload_log(PLAN, log(*events))["pulled"] == pulls(
        PLAN, "workload"
    )


def test_the_probe_waits_for_grants_and_for_the_data_plane(capsys):
    def late(name, count):
        return held(name, count) if count > 12 else []

    session = Session(late, pulls=[(403, {}), (200, {}), (200, {})])
    code, events, clock = run(session, capsys)
    assert code == 0
    assert [e["event"] for e in events].count("attempt") == 3
    assert [e["event"] for e in events].count("pull-refused") == 1
    assert clock.now == 3 * 15
    evaluate_workload_log(PLAN, log(*events))


@pytest.mark.parametrize(
    "permissions, pulled, email, code",
    [
        (held, [], MEMBERS["runner"], 2),
        (lambda name, count: [CONSUME, PUBLISH], [], None, 4),
        (lambda name, count: [], [], None, 5),
        (held, [(200, {"receivedMessages": [{}]})], None, 6),
        (held, [(404, {})], None, 6),
        # A refused test is not an absent grant, which a negative control
        # would otherwise read as passing.
        (lambda name, count: 403, [], None, 3),
    ],
)
def test_the_probe_refuses_what_its_reader_would(
    capsys, permissions, pulled, email, code
):
    session = Session(permissions, pulls=pulled)
    actual, events, _ = run(session, capsys, email=email, deadline=200.0)
    assert actual == code
    assert events[-1]["event"] == "refused"
    with pytest.raises(Failure):
        evaluate_workload_log(PLAN, log(*events))


def test_the_probe_stops_while_a_whole_round_still_fits(capsys):
    """Six tests of up to 20 seconds each follow every 15-second wait."""
    code, _, clock = run(Session(lambda name, count: []), capsys, deadline=300.0)
    assert code == 5
    # At 180 s another round would end at 180 + 15 + 120 > 300; at 165 s it fit.
    assert clock.now == 180


def test_the_probe_s_entrypoint_runs_as_its_credentials_and_silences_logging(
    monkeypatch, capsys
):
    import logging

    class Credentials:
        service_account_email = MEMBERS["workload"].split(":", 1)[1]

        def refresh(self, request):
            pass

        def before_request(self, request, method, url, headers):
            bearer(headers)

    sessions = []

    class Plain(Session):
        def __init__(self):
            super().__init__(held, pulls=[(200, {}), (200, {})])
            sessions.append(self)

    monkeypatch.setattr(
        probe.google.auth, "default", lambda scopes: (Credentials(), "p")
    )
    # A plain session: an authorized one would replay a 401 past the check.
    monkeypatch.setattr(probe.requests, "Session", Plain)
    monkeypatch.setattr(probe, "Request", lambda: None)
    spec = json.dumps(probe_spec(PLAN, schedule(10**10)))
    try:
        assert probe.main([spec]) == 0
        assert logging.root.manager.disable == logging.CRITICAL
    finally:
        logging.disable(logging.NOTSET)
    [session] = sessions
    assert session.trust_env is False and session.max_redirects == 0
    assert session.get_redirect_target(None) is None


def test_an_error_inside_the_probe_is_reported_not_raised(monkeypatch, capsys):
    import logging

    class Credentials:
        service_account_email = MEMBERS["workload"].split(":", 1)[1]

        def refresh(self, request):
            pass

        def before_request(self, request, method, url, headers):
            bearer(headers)

    class Broken:
        def post(self, *args, **kwargs):
            raise RuntimeError("socket")

    monkeypatch.setattr(
        probe.google.auth, "default", lambda scopes: (Credentials(), "p")
    )
    monkeypatch.setattr(probe.requests, "Session", Broken)
    monkeypatch.setattr(probe, "Request", lambda: None)
    try:
        assert probe.main([json.dumps(probe_spec(PLAN, schedule(10**10)))]) == 7
    finally:
        logging.disable(logging.NOTSET)
    last = json.loads(capsys.readouterr().out.splitlines()[-1])
    assert last == {"event": "refused", "reason": "error", "error": "RuntimeError"}


def test_the_wait_stops_while_a_whole_round_of_slow_tests_still_fits():
    """Six tests of 10 seconds each, then a 15-second wait, against 200 seconds.

    The second round ends at 135 s, and a third would need 15 s plus six 20-s
    budgets more, past the deadline, so the wait ends there with two rounds.
    Budgeting one request instead would start a third that overruns it.
    """
    from flink_tier3.pubsub_access import wait_for_access

    clock = Clock()
    calls = []

    class Slow:
        def holds(self, phase, name, permission):
            calls.append(name)
            clock.now += 10
            return False

    env = SimpleNamespace(
        approval=SimpleNamespace(pubsub_plan=PLAN),
        clock=clock,
        sleep=clock.sleep,
        stopping=False,
        evidence_failed=False,
    )
    with pytest.raises(Failure, match="did not take effect"):
        wait_for_access(env, Slow(), "runner", deadline=200.0)
    assert len(calls) == 2 * 6 and clock.now == 135


def test_the_probe_sends_nothing_that_could_outlast_the_deadline(capsys):
    """Granted at once, it still checks the time before each pull."""
    clock = Clock()

    class Ticking(Session):
        def post(self, url, json, **kwargs):
            clock.now += 1
            return super().post(url, json, **kwargs)

    session = Ticking(held, pulls=[(200, {}), (200, {})])
    spec = probe_spec(PLAN, schedule(25.0))
    code = probe.run(
        spec, session, MEMBERS["workload"], bearer, clock=clock, sleep=clock.sleep
    )
    events = [json.loads(line) for line in capsys.readouterr().out.splitlines()]
    assert code == 8 and events[-1] == {"event": "refused", "reason": "deadline"}
    # Six permission tests fit; the first pull, at 6 s, would end past 25 s.
    assert [url for url, _ in session.calls if url.endswith(":pull")] == []


def test_the_probe_checks_the_deadline_after_refreshing_its_token(capsys):
    """A refresh is not a Pub/Sub request, but it spends the request's time."""
    clock = Clock()

    def slow_refresh(headers):
        clock.now += 10
        bearer(headers)

    session = Session(held, pulls=[(200, {}), (200, {})])
    spec = probe_spec(PLAN, schedule(25.0))
    code = probe.run(
        spec, session, MEMBERS["workload"], slow_refresh, clock=clock, sleep=clock.sleep
    )
    events = [json.loads(line) for line in capsys.readouterr().out.splitlines()]
    # At 0 s the first test would fit, but not once the refresh took 10 s.
    assert code == 8 and events[-1] == {"event": "refused", "reason": "deadline"}
    assert session.calls == []
