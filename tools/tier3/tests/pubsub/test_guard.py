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
"""The production guard: each Pub/Sub service request against its deadline."""

import time
from dataclasses import replace

import pytest
from flink_tier3.common import ApiError, Failure
from flink_tier3.environment import Environment
from flink_tier3.model import Phase
from flink_tier3.policy import HTTP_TIMEOUT
from flink_tier3.pubsub.guard import PubSubGuard
from flink_tier3.pubsub.handoff import PubSubHandoff
from flink_tier3.pubsub.lifecycle import PubSubLifecycle

from ..test_lifecycle import env as env  # noqa: PLC0414
from ..test_lifecycle import rt
from .test_handoff import Service
from .test_messages import Response, output
from .test_plan import trial as trial  # noqa: PLC0414
from .test_runtime_approval import prepared as prepared  # noqa: PLC0414


class Chain:
    """Both actors, each with its own production guard, over one service."""

    def __init__(self, environment, deadline, settled=None):
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
            guard = PubSubGuard(env)
            guard.deadline = deadline
            self.guards[role] = guard
            self.actors[role] = PubSubHandoff(
                PubSubLifecycle(env, self.service, guard),
                actor_token=token,
                settled=settled,
            )


@pytest.fixture
def chain(prepared):
    environment, _ = prepared
    return Chain(environment, environment.clock() + 900)


def run_lifecycle(chain):
    runner, supervisor = chain.actors["runner"], chain.actors["supervisor"]
    records = chain.environment.records
    runner.initialize()
    runner.prepare()
    records.set_phase(Phase.READY)
    runner.verify()
    supervisor.join()
    chain.service.messages.responses.append(Response({"messageIds": ["a"]}))
    runner.publish(0, 0, 1)
    records.set_phase(Phase.RUNNING)
    chain.service.messages.responses.extend(
        [Response({"receivedMessages": [output()]}), Response({})]
    )
    supervisor.collect("one", max_messages=1)
    runner.release()
    assert not supervisor.released()
    supervisor.release()
    supervisor.cleanup(lambda: True)
    assert chain.environment.refresh().pubsub["stage"] == "cleaned"


def test_a_whole_lifecycle_runs_under_the_production_guards(chain):
    run_lifecycle(chain)
    deletes = [c for c in chain.service.resources.calls if c[0] == "DELETE"]
    assert len(deletes) == 6


def conflicting(chain, monkeypatch, exhaust=lambda: False):
    """Refuse six writes for the object's mutation rate, then lose nineteen
    generation races on every control write, the adapter's worst success;
    when ``exhaust()`` says so, lose all twenty and let it give up.

    Returns the number of races lost by each update, in order.
    """
    store, path = chain.environment.store, chain.environment.records.path
    original, lost, current = store.write, [], {"left": None}
    backoffs = []
    monkeypatch.setattr(time, "sleep", backoffs.append)

    def write(name, value, generation=None, **kwargs):
        if name == path and generation not in (None, "0"):
            if current["left"] is None:
                current["left"] = 20 if exhaust() else 19
                current["throttle"] = 6
                lost.append(0)
            if current["throttle"] > 0:
                current["throttle"] -= 1
                raise ApiError(429, "PUT", name)
            if current["left"] > 0:
                current["left"] -= 1
                lost[-1] += 1
                if lost[-1] == 20:
                    current["left"] = None
                raise ApiError(412, "PUT", name)
            current["left"] = None
        return original(name, value, generation, **kwargs)

    monkeypatch.setattr(store, "write", write)
    return lost


def test_control_conflicts_on_every_write_do_not_stop_the_lifecycle(chain, monkeypatch):
    lost = conflicting(chain, monkeypatch)
    run_lifecycle(chain)
    assert lost and all(count == 19 for count in lost)


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
    environment, _ = prepared
    chain = Chain(environment, environment.clock() + 900, lambda: True)
    chain.actors["runner"].initialize()
    return chain


def inflight(chain, role="runner"):
    actors = chain.environment.refresh().pubsub["handoff"]["actors"]
    return actors[role]["inflight"]


def test_a_failed_publication_records_it_and_releases_its_settled_marker(
    settling, monkeypatch
):
    """Under conflicts, and with the completion losing every race twice."""
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
    assert lost.count(20) == 2
    control = chain.environment.refresh()
    assert control.evidence_failed and control.stop_requested
    assert inflight(chain) is None


def test_a_failed_collection_records_it_and_releases_its_settled_marker(
    settling, monkeypatch
):
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
    assert lost.count(20) == 2
    assert chain.environment.refresh().evidence_failed
    assert inflight(chain, "supervisor") is None


@pytest.mark.parametrize("margin", [HTTP_TIMEOUT, HTTP_TIMEOUT - 1])
def test_a_request_too_close_to_the_deadline_is_refused(prepared, margin):
    environment, _ = prepared
    chain = Chain(environment, environment.clock() + margin)
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


def test_every_resource_request_is_checked_against_the_deadline(prepared):
    """Not once per phase: the last request that fits is the last one sent."""
    environment, _ = prepared
    deadline = environment.clock() + 900
    chain = Chain(environment, deadline, lambda: True)
    runner = chain.actors["runner"]
    runner.initialize()
    sent = []

    def slow(_method, _name):
        sent.append(environment.clock())
        environment.clock.now += 30  # a slow request

    chain.service.resources.before = slow
    with pytest.raises(Failure, match="too close to its deadline"):
        runner.prepare()
    assert sent
    assert all(at + HTTP_TIMEOUT <= deadline for at in sent)
    assert sent[-1] + 30 + HTTP_TIMEOUT > deadline


def test_a_slow_begin_marker_counts_against_the_publication_deadline(
    chain, monkeypatch
):
    """The check is taken just before the POST, after the call's begin write."""
    runner, environment = chain.actors["runner"], chain.environment
    runner.initialize()
    runner.prepare()
    environment.records.set_phase(Phase.READY)
    deadline = environment.clock() + HTTP_TIMEOUT + 10
    store, path = environment.store, environment.records.path
    original = store.write

    def slow(name, value, generation=None, **kwargs):
        if name == path:
            environment.clock.now += 15  # a slow control write
        return original(name, value, generation, **kwargs)

    monkeypatch.setattr(store, "write", slow)
    chain.service.messages.responses.append(Response({"messageIds": ["a"]}))
    with pytest.raises(Failure, match="too close to its deadline"):
        runner.publish(0, 0, 1, deadline=deadline)
    assert chain.service.messages.calls == []


def test_a_request_under_an_approval_that_no_longer_validates_is_refused(chain):
    runner = chain.actors["runner"]
    runner.initialize()
    environment = chain.environment
    environment.approval = replace(environment.approval, pubsub_trial={})
    with pytest.raises(Failure, match="Pub/Sub trial"):
        runner.prepare()
    assert chain.service.resources.calls == []


def test_a_held_deadline_is_restored_after_its_block(chain):
    guard = chain.guards["runner"]
    admission = guard.deadline
    with guard.until(None):
        guard("cleanup", "DELETE", "projects/p/topics/t")
        with guard.until(1), pytest.raises(Failure, match="too close"):
            guard("publish", "POST", "projects/p/topics/t:publish")
        assert guard.deadline is None
    assert guard.deadline == admission
    # Storage and control operations are never held to a deadline.
    with guard.until(1):
        guard("publish", "PUT", "runs/r/pubsub/messages/intent.json")


@pytest.mark.parametrize("settled", [True, False])
def test_a_settled_failure_releases_its_marker_so_the_run_can_clean(prepared, settled):
    environment, _ = prepared
    seen = []

    def check():
        seen.append(inflight(chain))
        return settled

    chain = Chain(environment, environment.clock() + HTTP_TIMEOUT - 1, check)
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
    environment, _ = prepared
    chain = Chain(environment, environment.clock() + HTTP_TIMEOUT - 1, lambda: True)
    runner = chain.actors["runner"]
    runner.initialize()

    def lost(marker):
        raise Failure("control write lost")

    monkeypatch.setattr(runner, "_finish", lost)
    with pytest.raises(Failure, match="too close to its deadline"):
        runner.prepare()
    assert inflight(chain) is not None


def test_the_stop_is_written_before_a_settled_failure_releases_its_marker(
    prepared, monkeypatch
):
    """A marker released first would leave a cleared call on a running run."""
    environment, _ = prepared
    chain = Chain(environment, environment.clock() + HTTP_TIMEOUT - 1, lambda: True)
    runner = chain.actors["runner"]
    runner.initialize()

    def lost():
        raise Failure("stop write lost")

    monkeypatch.setattr(runner, "stop", lost)
    with pytest.raises(Failure, match="stop write lost"):
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
    before = len(chain.service.resources.calls)
    with pytest.raises(Failure, match="not prepared|has stopped"):
        runner.verify()
    assert len(chain.service.resources.calls) == before


def test_a_publication_given_the_admission_deadline_stops_at_it(ready):
    runner, supervisor = ready.actors["runner"], ready.actors["supervisor"]
    environment, messages = ready.environment, ready.service.messages
    supervisor.join()
    admission = ready.guards["runner"].deadline
    environment.clock.now = admission
    with pytest.raises(Failure, match="too close to its deadline"):
        runner.publish(0, 0, 1, deadline=admission)
    assert messages.calls == []


def test_later_cohorts_and_collection_use_the_traffic_window(ready):
    runner, supervisor = ready.actors["runner"], ready.actors["supervisor"]
    environment, messages = ready.environment, ready.service.messages
    supervisor.join()
    environment.records.set_phase(Phase.RUNNING)
    environment.clock.now = ready.guards["runner"].deadline
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
    deadline = ready.guards["runner"].deadline
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
    assert inflight(ready) is None


def test_reclaiming_an_abandoned_runner_succeeds_under_conflicts(ready, monkeypatch):
    supervisor = ready.actors["supervisor"]
    supervisor.join()
    lost = conflicting(ready, monkeypatch)
    supervisor.stop()
    supervisor.reclaim(lambda snapshot: True)
    assert all(count == 19 for count in lost)
    assert ready.environment.refresh().pubsub["stage"] == "cleaned"
    assert not ready.service.resources.resources


def test_a_cleanup_before_creation_intent_sends_nothing(chain):
    runner, supervisor = chain.actors["runner"], chain.actors["supervisor"]
    runner.initialize()
    runner.release()
    supervisor.cleanup(lambda: True)
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
    deadline = ready.guards["runner"].deadline
    ready.environment.clock.now = deadline - HTTP_TIMEOUT + 1
    calls = ready.service.resources.calls
    before = len(calls)
    with pytest.raises(Failure, match="too close to its deadline"):
        runner.verify()
    assert len(calls) == before
