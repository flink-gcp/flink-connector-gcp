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
"""Per-method operation bounds reserved before each Pub/Sub lifecycle method."""

from dataclasses import replace

import pytest
from flink_tier3 import pubsub_guard as guards
from flink_tier3.common import ApiError, Failure
from flink_tier3.environment import Environment
from flink_tier3.model import Phase
from flink_tier3.policy import HTTP_TIMEOUT
from flink_tier3.pubsub_guard import PubSubGuard
from flink_tier3.pubsub_handoff import PubSubHandoff
from flink_tier3.pubsub_lifecycle import PubSubLifecycle
from flink_tier3.pubsub_traffic import PubSubTraffic
from test_pubsub_approval import prepared as prepared  # noqa: PLC0414
from test_pubsub_handoff import Service
from test_pubsub_messages import Response, output
from test_pubsub_plan import trial as trial  # noqa: PLC0414
from test_tier3_lifecycle import env as env  # noqa: PLC0414
from test_tier3_lifecycle import rt


class Chain:
    """Both actors, each with its own production guard, over one service."""

    def __init__(self, environment, application, deadline, settled=None):
        self.environment, self.service = environment, Service()
        self.guards, self.actors = {}, {}
        for role, token in (("runner", "b" * 32), ("supervisor", "c" * 32)):
            env = (
                environment
                if role == "runner"
                else Environment(
                    environment.kube,
                    environment.store,
                    environment.approval,
                    environment.clock,
                    environment.sleep,
                    actor="supervisor",
                )
            )
            guard = PubSubGuard(env, deadline=deadline)
            controller = PubSubLifecycle(env, self.service, application, guard)
            self.guards[role] = guard
            self.actors[role] = PubSubHandoff(
                PubSubTraffic(
                    controller, application, env.approval.pubsub_traffic_limits
                ),
                actor_token=token,
                settled=settled,
            )

    def used(self, role):
        return self.guards[role].last["used"]


@pytest.fixture
def chain(prepared):
    environment, application = prepared
    return Chain(environment, application, environment.clock() + 900)


def run_lifecycle(chain):
    runner, supervisor = chain.actors["runner"], chain.actors["supervisor"]
    records = chain.environment.records
    used = {}
    runner.initialize()
    used["initialize"] = chain.used("runner")
    runner.prepare()
    used["prepare"] = chain.used("runner")
    records.set_phase(Phase.READY)
    runner.verify()
    used["verify"] = chain.used("runner")
    supervisor.join()
    used["join"] = chain.used("supervisor")
    chain.service.messages.responses.append(Response({"messageIds": ["a"]}))
    runner.publish(0, 0, 1)
    used["publish"] = chain.used("runner")
    records.set_phase(Phase.RUNNING)
    chain.service.messages.responses.extend(
        [Response({"receivedMessages": [output()]}), Response({})]
    )
    supervisor.collect("one", max_messages=1)
    used["collect"] = chain.used("supervisor")
    runner.release()
    used["release"] = chain.used("runner")
    supervisor.released()
    used["released"] = chain.used("supervisor")
    supervisor.release()
    supervisor.cleanup(lambda: True)
    used["cleanup"] = chain.used("supervisor")
    assert chain.environment.refresh().pubsub["stage"] == "cleaned"
    return used


def within(used, method):
    bounds = guards.METHOD_BOUNDS[method]
    assert set(used) <= set(bounds), method
    assert all(count <= bounds[key] for key, count in used.items()), method


def test_a_whole_lifecycle_runs_within_every_reserved_bound(chain):
    used = run_lifecycle(chain)
    for method, counts in used.items():
        within(counts, method)
    # The resource helpers' callbacks are exactly the runbook's table.
    assert {k: v for k, v in used["prepare"].items() if "control" not in k} == {
        "provision": 20,
        "inspect": 21,
        "grant": 42,
        "inspect-grants": 12,
    }
    assert {k: v for k, v in used["verify"].items() if "control" not in k} == {
        "inspect": 7,
        "inspect-grants": 12,
    }
    assert used["cleanup"]["cleanup"] == 26


def conflicting(chain, monkeypatch, exhaust=lambda: False):
    """Lose four generation races on every control write, the adapter's worst
    success; when ``exhaust()`` says so, lose all five and let it give up.

    Returns the number of races lost by each update, in order.
    """
    store, path = chain.environment.store, chain.environment.records.path
    original, lost, current = store.write, [], {"left": None}

    def write(name, value, generation=None, **kwargs):
        if name == path and generation not in (None, "0"):
            if current["left"] is None:
                current["left"] = 5 if exhaust() else 4
                lost.append(0)
            if current["left"] > 0:
                current["left"] -= 1
                lost[-1] += 1
                if lost[-1] == 5:
                    current["left"] = None
                raise ApiError(412, "PUT", name)
            current["left"] = None
        return original(name, value, generation, **kwargs)

    monkeypatch.setattr(store, "write", write)
    return lost


def test_control_conflicts_on_every_write_stay_within_the_bounds(chain, monkeypatch):
    lost = conflicting(chain, monkeypatch)
    used = run_lifecycle(chain)
    assert lost and all(count == 4 for count in lost)
    for method, counts in used.items():
        within(counts, method)
        # Each update then takes all six of its callbacks.
        assert counts.get("control-UPDATE", 0) % 6 == 0, method


def exhausting_finish(handoff, monkeypatch):
    """Make the call's completion lose every race twice before it lands."""
    original, state = handoff._finish, {"finishing": False, "exhausted": 0}

    def finish(marker):
        state["finishing"] = True
        try:
            return original(marker)
        finally:
            state["finishing"] = False

    def exhaust():
        if state["finishing"] and state["exhausted"] < 2:
            state["exhausted"] += 1
            return True
        return False

    monkeypatch.setattr(handoff, "_finish", finish)
    return exhaust


@pytest.fixture
def settling(prepared):
    environment, application = prepared
    chain = Chain(environment, application, environment.clock() + 900, lambda: True)
    runner = chain.actors["runner"]
    runner.initialize()
    return chain


def test_a_failed_publication_s_worst_case_fits_its_bound(settling, monkeypatch):
    """Batch, POST, two uploads, a failure record, a stop and three
    completion attempts: every update the bound allows for."""
    chain, runner = settling, settling.actors["runner"]
    runner.prepare()
    chain.environment.records.set_phase(Phase.READY)
    store, original = chain.environment.store, chain.environment.store.write

    def write(name, value, generation=None, **kwargs):
        if name.endswith("/response.json"):
            raise Failure("response upload lost")
        return original(name, value, generation, **kwargs)

    monkeypatch.setattr(store, "write", write)
    lost = conflicting(chain, monkeypatch, exhausting_finish(runner, monkeypatch))
    chain.service.messages.responses.append(Response({"messageIds": ["a"]}))
    with pytest.raises(Failure, match="response upload lost"):
        runner.publish(0, 0, 1)
    # Six callbacks for each of ten updates; a smaller bound refuses the last
    # completion attempt, which keeps the marker.
    assert chain.used("runner")["control-UPDATE"] == 6 * 10
    assert lost.count(5) == 2
    assert chain.environment.refresh().evidence_failed
    assert inflight(chain) is None


def test_a_failed_collection_s_worst_case_fits_its_bound(settling, monkeypatch):
    chain = settling
    runner, supervisor = chain.actors["runner"], chain.actors["supervisor"]
    runner.prepare()
    chain.environment.records.set_phase(Phase.READY)
    supervisor.join()
    chain.environment.records.set_phase(Phase.RUNNING)
    store, original = chain.environment.store, chain.environment.store.write

    def write(name, value, generation=None, **kwargs):
        if name.endswith("/acknowledged.json"):
            raise Failure("receipt upload lost")
        return original(name, value, generation, **kwargs)

    monkeypatch.setattr(store, "write", write)
    lost = conflicting(chain, monkeypatch, exhausting_finish(supervisor, monkeypatch))
    chain.service.messages.responses.extend(
        [Response({"receivedMessages": [output()]}), Response({})]
    )
    with pytest.raises(Failure, match="receipt upload lost"):
        supervisor.collect("one", max_messages=1)
    # Batch, pull, three uploads, acknowledgement, receipt, failure record,
    # stop and three completion attempts.
    assert chain.used("supervisor")["control-UPDATE"] == 6 * 13
    assert lost.count(5) == 2
    actors = chain.environment.refresh().pubsub["handoff"]["actors"]
    assert actors["supervisor"]["inflight"] is None


def test_a_preparation_failing_at_its_last_update_fits_its_bound(settling, monkeypatch):
    chain, runner = settling, settling.actors["runner"]
    lost = conflicting(chain, monkeypatch, exhausting_finish(runner, monkeypatch))
    traffic = runner.traffic

    def initialize():
        raise Failure("traffic binding lost")

    monkeypatch.setattr(traffic, "initialize", initialize)
    with pytest.raises(Failure, match="traffic binding lost"):
        runner.prepare()
    within(chain.used("runner"), "prepare")
    assert lost.count(5) == 2
    assert inflight(chain) is None
    assert chain.environment.refresh().pubsub["stage"] == "prepared"


def test_an_operation_outside_a_reserved_method_is_refused(chain):
    controller = chain.actors["runner"].controller
    with pytest.raises(Failure, match="outside a reserved method"):
        controller.initialize()
    assert chain.environment.refresh().pubsub is None


def test_a_phase_the_method_does_not_use_is_refused(chain):
    runner = chain.actors["runner"]
    with (
        runner.controller.reserve("join"),
        pytest.raises(Failure, match="exceeded its reserved provision"),
    ):
        runner.controller.before_operation("provision", "GET", "projects/p/topics/t")


def inflight(chain):
    return chain.environment.refresh().pubsub["handoff"]["actors"]["runner"]["inflight"]


def test_a_method_past_its_bound_is_refused_before_the_request(chain, monkeypatch):
    runner = chain.actors["runner"]
    runner.initialize()
    # Each policy preflight takes two callbacks: the ownership read and the GET.
    bounds = dict(guards.METHOD_BOUNDS["prepare"], grant=3)
    monkeypatch.setitem(guards.METHOD_BOUNDS, "prepare", bounds)
    with pytest.raises(Failure, match="exceeded its reserved grant"):
        runner.prepare()
    policies = [c for c in chain.service.resources.calls if ":" in c[1]]
    assert [c[0] for c in policies] == ["GET"]
    assert chain.environment.stopping
    # Without a settled-call check every failure keeps its marker.
    assert inflight(chain) is not None


def test_unknown_methods_are_refused(chain):
    with (
        pytest.raises(Failure, match="Unknown Pub/Sub lifecycle method"),
        chain.guards["runner"].method("provision"),
    ):
        pass


@pytest.mark.parametrize("margin", [HTTP_TIMEOUT, HTTP_TIMEOUT - 1])
def test_a_request_too_close_to_the_deadline_is_refused(prepared, margin):
    environment, application = prepared
    chain = Chain(environment, application, environment.clock() + margin)
    runner = chain.actors["runner"]
    runner.initialize()
    if margin < HTTP_TIMEOUT:
        with pytest.raises(Failure, match="too close to its deadline"):
            runner.prepare()
        assert chain.service.resources.calls == []
    else:
        runner.prepare()
        assert chain.environment.refresh().pubsub["stage"] == "prepared"
    # Settlement is never held to the admission deadline.
    environment.clock.now += 3600
    runner.stop()
    assert environment.refresh().stop_requested


def test_a_request_under_an_approval_that_no_longer_validates_is_refused(chain):
    runner = chain.actors["runner"]
    runner.initialize()
    environment = chain.environment
    environment.approval = replace(environment.approval, pubsub_trial={})
    with pytest.raises(Failure, match="Pub/Sub trial"):
        runner.prepare()
    assert chain.service.resources.calls == []


@pytest.mark.parametrize("settled", [True, False])
def test_a_settled_failure_releases_its_marker_so_the_run_can_clean(prepared, settled):
    environment, application = prepared
    seen = []

    def check():
        seen.append(inflight(chain))
        return settled

    chain = Chain(
        environment, application, environment.clock() + HTTP_TIMEOUT - 1, check
    )
    runner, supervisor = chain.actors["runner"], chain.actors["supervisor"]
    runner.initialize()
    with pytest.raises(Failure, match="too close to its deadline"):
        runner.prepare()
    # Asked once, after the stop and with the call's marker still recorded.
    assert len(seen) == 1 and seen[0] is not None
    assert environment.refresh().stop_requested
    if not settled:
        assert inflight(chain) is not None
        with pytest.raises(Failure, match="unresolved"):
            runner.release()
        return
    assert inflight(chain) is None
    runner.release()
    supervisor.cleanup(lambda: True)
    assert environment.refresh().pubsub["stage"] == "cleaned"


def test_a_lost_marker_release_keeps_the_marker_and_the_original_error(
    prepared, monkeypatch
):
    environment, application = prepared
    chain = Chain(
        environment,
        application,
        environment.clock() + HTTP_TIMEOUT - 1,
        lambda: True,
    )
    runner = chain.actors["runner"]
    runner.initialize()

    def lost(marker):
        raise Failure("control write lost")

    monkeypatch.setattr(runner, "_finish", lost)
    with pytest.raises(Failure, match="too close to its deadline"):
        runner.prepare()
    assert inflight(chain) is not None


@pytest.fixture
def ready(chain):
    runner = chain.actors["runner"]
    runner.initialize()
    runner.prepare()
    chain.environment.records.set_phase(Phase.READY)
    return chain


def test_verification_rereads_every_resource_and_policy(ready):
    calls = ready.service.resources.calls
    before = len(calls)
    observed = ready.actors["runner"].verify()
    assert len(observed) == 6
    assert [c[0] for c in calls[before:]] == ["GET"] * 12


def subscription(chain):
    return next(
        name for name in chain.service.resources.resources if "/subscriptions/" in name
    )


@pytest.mark.parametrize(
    "drift, match",
    [
        ("settings", "settings"),
        ("policy", "IAM policy"),
        ("ownership", "ownership"),
        ("deleted", "is absent"),
    ],
)
def test_drift_after_preparation_is_refused_without_writing(ready, drift, match):
    service = ready.service.resources
    name = subscription(ready)
    if drift == "settings":
        service.resources[name]["ackDeadlineSeconds"] += 1
    elif drift == "policy":
        service.policies[name]["bindings"][0]["members"].append(
            "serviceAccount:other@example.com"
        )
    elif drift == "ownership":
        service.resources[name]["labels"] = {}
    else:
        del service.resources[name]
    before = len(service.calls)
    with pytest.raises(Failure, match=match):
        ready.actors["supervisor"].verify()
    assert all(c[0] == "GET" for c in service.calls[before:])


@pytest.mark.parametrize("state", ["unprepared", "stopped", "running"])
def test_verification_requires_open_prepared_admission(chain, state):
    runner = chain.actors["runner"]
    runner.initialize()
    if state != "unprepared":
        runner.prepare()
        chain.environment.records.set_phase(Phase.READY)
    if state == "stopped":
        runner.stop()
    if state == "running":
        chain.environment.records.set_phase(Phase.RUNNING)
    with pytest.raises(Failure, match="not prepared|has stopped"):
        runner.verify()


def test_a_publication_given_the_admission_deadline_stops_at_it(ready):
    runner, supervisor = ready.actors["runner"], ready.actors["supervisor"]
    environment, messages = ready.environment, ready.service.messages
    supervisor.join()
    admission = runner.controller.before_operation.deadline
    environment.clock.now = admission
    with pytest.raises(Failure, match="too close to its deadline"):
        runner.publish(0, 0, 1, deadline=admission)
    assert messages.calls == []


def test_later_cohorts_and_collection_use_the_traffic_window(ready):
    runner, supervisor = ready.actors["runner"], ready.actors["supervisor"]
    environment, messages = ready.environment, ready.service.messages
    supervisor.join()
    environment.records.set_phase(Phase.RUNNING)
    environment.clock.now = runner.controller.before_operation.deadline
    messages.responses.extend([Response({"messageIds": ["a"]}), Response({})])
    runner.publish(0, 0, 1)
    supervisor.collect("one", max_messages=1)
    assert len(messages.calls) == 2
    environment.clock.now = environment.schedule.cleanup_at - HTTP_TIMEOUT + 1
    with pytest.raises(Failure, match="too close to its deadline"):
        runner.publish(0, 1, 1)
    assert len(messages.calls) == 2


def test_the_evidence_of_a_request_already_sent_is_kept_past_the_deadline(ready):
    """Only new service requests stop at the deadline, not their receipts."""
    runner, supervisor = ready.actors["runner"], ready.actors["supervisor"]
    environment, messages = ready.environment, ready.service.messages
    supervisor.join()
    deadline = runner.controller.before_operation.deadline
    environment.clock.now = deadline - HTTP_TIMEOUT - 1
    original = messages.request

    def slow(*args, **kwargs):
        # The publication is answered after a later request could start.
        environment.clock.now = deadline - 1
        return original(*args, **kwargs)

    messages.request = slow
    messages.responses.append(Response({"messageIds": ["a"]}))
    assert runner.publish(0, 0, 1, deadline=deadline)["message_ids"] == ["a"]
    assert not environment.evidence_failed
    assert (
        environment.refresh().pubsub["handoff"]["actors"]["runner"]["inflight"] is None
    )


def test_a_stop_during_verification_refuses_its_remaining_reads(ready):
    service = ready.service.resources
    before = len(service.calls)

    def stop(method, name):
        service.before = lambda method, name: None
        ready.actors["supervisor"].stop()

    service.before = stop
    with pytest.raises(Failure, match="has stopped"):
        ready.actors["runner"].verify()
    assert len(service.calls) - before == 1


def test_reclaiming_an_abandoned_runner_fits_its_bound_under_conflicts(
    ready, monkeypatch
):
    supervisor = ready.actors["supervisor"]
    supervisor.join()
    lost = conflicting(ready, monkeypatch)
    supervisor.stop()
    supervisor.reclaim(lambda snapshot: True)
    within(ready.used("supervisor"), "reclaim")
    assert ready.used("supervisor")["cleanup"] == 26
    assert all(count == 4 for count in lost)
    assert ready.environment.refresh().pubsub["stage"] == "cleaned"


def test_a_cleanup_before_creation_intent_sends_nothing(chain):
    runner, supervisor = chain.actors["runner"], chain.actors["supervisor"]
    runner.initialize()
    runner.release()
    supervisor.cleanup(lambda: True)
    assert "cleanup" not in chain.used("supervisor")
    assert chain.service.resources.calls == []
    assert chain.environment.refresh().pubsub["stage"] == "cleaned"


def test_settlement_outlives_the_admission_deadline_and_the_approval(ready):
    """Cleanup deletes what preparation created whenever it runs."""
    runner, supervisor = ready.actors["runner"], ready.actors["supervisor"]
    environment = ready.environment
    # Past the admission deadline and the window in which the approval
    # validates, so either check would refuse a request.
    environment.clock.now = rt.timestamp(environment.approval.cleanup_at) + 60
    runner.release()
    supervisor.release()
    supervisor.cleanup(lambda: True)
    deletes = [c for c in ready.service.resources.calls if c[0] == "DELETE"]
    assert len(deletes) == 6
    assert environment.refresh().pubsub["stage"] == "cleaned"


def test_verification_stops_at_the_admission_deadline(ready):
    runner = ready.actors["runner"]
    deadline = runner.controller.before_operation.deadline
    ready.environment.clock.now = deadline - HTTP_TIMEOUT + 1
    calls = ready.service.resources.calls
    before = len(calls)
    with pytest.raises(Failure, match="too close to its deadline"):
        runner.verify()
    assert len(calls) == before


def test_the_stop_is_written_before_a_settled_failure_releases_its_marker(
    prepared, monkeypatch
):
    """A marker released first would leave a cleared call on a running run."""
    environment, application = prepared
    chain = Chain(
        environment,
        application,
        environment.clock() + HTTP_TIMEOUT - 1,
        lambda: True,
    )
    runner = chain.actors["runner"]
    runner.initialize()

    def lost():
        raise Failure("stop write lost")

    monkeypatch.setattr(runner, "stop", lost)
    with pytest.raises(Failure, match="stop write lost"):
        runner.prepare()
    assert inflight(chain) is not None


def test_a_nested_method_cannot_set_its_own_deadline(chain):
    guard = chain.guards["runner"]
    with (
        guard.method("release"),
        pytest.raises(Failure, match="nested Pub/Sub method cannot set a deadline"),
        guard.method("publish", deadline=1),
    ):
        pass
    with guard.method("release"), guard.method("stop"):
        pass


def test_control_io_before_a_resource_request_counts_against_its_deadline(
    prepared, monkeypatch
):
    """The check is taken after the control reads, just before the send."""
    environment, application = prepared
    deadline = environment.clock() + 900
    chain = Chain(environment, application, deadline, lambda: True)
    runner = chain.actors["runner"]
    runner.initialize()
    store, path = environment.store, environment.records.path
    original, sent = store.read, []

    def slow(name, *args, **kwargs):
        if name == path:
            environment.clock.now += 30  # a slow control read
        return original(name, *args, **kwargs)

    monkeypatch.setattr(store, "read", slow)
    chain.service.resources.before = lambda method, name: sent.append(
        environment.clock()
    )
    with pytest.raises(Failure, match="too close to its deadline"):
        runner.prepare()
    assert sent
    assert all(at + HTTP_TIMEOUT <= deadline for at in sent)


def test_a_reservation_before_a_publication_counts_against_its_deadline(
    ready, monkeypatch
):
    runner, environment = ready.actors["runner"], ready.environment
    # Three control writes (the call's begin, the batch and the intent's
    # evidence) precede the POST's own reservation: a check taken before that
    # reservation, at +45 s, would pass; the one taken after it, at +60 s,
    # must not.
    deadline = environment.clock() + HTTP_TIMEOUT + 50
    store, path = environment.store, environment.records.path
    original = store.write

    def slow(name, value, generation=None, **kwargs):
        if name == path:
            environment.clock.now += 15  # a slow reservation write
        return original(name, value, generation, **kwargs)

    monkeypatch.setattr(store, "write", slow)
    ready.service.messages.responses.append(Response({"messageIds": ["a"]}))
    with pytest.raises(Failure, match="too close to its deadline"):
        runner.publish(0, 0, 1, deadline=deadline)
    assert ready.service.messages.calls == []
