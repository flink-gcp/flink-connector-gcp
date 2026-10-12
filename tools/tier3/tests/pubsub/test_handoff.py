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
"""Actor handoff composed with production Pub/Sub resources, messages and records."""

import copy
from types import SimpleNamespace

import pytest
import requests
from flink_tier3.common import Failure, TransportError
from flink_tier3.model import Phase
from flink_tier3.pubsub.handoff import PubSubHandoff
from flink_tier3.pubsub.lifecycle import PubSubLifecycle, require_pubsub_clean
from flink_tier3.records import Records

from .test_lifecycle import Environment
from .test_messages import Http, Response, output
from .test_resources import FakeIamHttp


class Service:
    def __init__(self):
        self.resources = FakeIamHttp()
        self.messages = Http([])

    def request(self, method, url, **kwargs):
        if url.endswith((":publish", ":pull", ":acknowledge")):
            return self.messages.request(method, url, **kwargs)
        return self.resources.request(method, url, **kwargs)


@pytest.fixture
def setup(request):
    app = {
        "metadata": {"name": "lifecycle-1361", "namespace": "tier3-pubsub"},
        "spec": {
            "job": {
                "args": ["--run-id=lifecycle-1361", "--records-per-subscription=1000"]
            }
        },
    }
    env = Environment(app)
    now = [1800000000]
    env.clock = lambda: now[0]
    env.schedule = SimpleNamespace(cleanup_at=now[0] + 200, expires_at=now[0] + 300)
    env.stopping, env.evidence_failure = False, None
    http, guards = Service(), []
    tokens = {"runner": "b" * 32, "supervisor": "c" * 32}

    def make(role="runner", token=None):
        view = copy.copy(env)
        view.actor = role
        view.records = Records(env.store, env.approval, env.clock)
        controller = PubSubLifecycle(view, http, lambda *args: guards.append(args))
        return PubSubHandoff(controller, actor_token=token or tokens[role])

    make().initialize()
    if getattr(request, "param", True):
        make().prepare()
        env.records.set_phase(Phase.READY)
        make("supervisor").join()
        env.records.set_phase(Phase.RUNNING)
    return make, env, http, now, guards


def actors(env):
    return env.refresh().pubsub["handoff"]["actors"]


def deletions(http):
    return [c for c in http.resources.calls if c[0] == "DELETE"]


def control_writes(env):
    """Count the writes of the shared control record from here on."""
    original, writes = env.store.write, []

    def write(name, value, generation="0", **kwargs):
        if name == env.records.path:
            writes.append(name)
        return original(name, value, generation, **kwargs)

    env.store.write = write
    return writes


def test_a_publication_and_a_pull_each_update_control_twice(setup):
    """Their begin and completion markers, and nothing else, on the normal path.

    Cloud Storage takes about one mutation a second on one object, and both
    actors write the same record.
    """
    make, env, http, _, _ = setup
    writes = control_writes(env)
    http.messages.responses.append(Response({"messageIds": ["a", "b"]}))
    make().publish(0, 0, 2)
    assert len(writes) == 2
    writes.clear()
    http.messages.responses.extend(
        [Response({"receivedMessages": [output()]}), Response({})]
    )
    make("supervisor").collect("one", max_messages=1)
    assert len(writes) == 2
    writes.clear()
    http.messages.responses.append(Response({}))
    make("supervisor").collect("two")
    assert len(writes) == 2
    assert len(http.messages.calls) == 4


@pytest.mark.parametrize(
    "phase, publishes, collects",
    [
        (Phase.APPROVED, False, False),
        # The first cohort is published before the application exists.
        (Phase.READY, True, False),
        (Phase.RUNNING, True, True),
    ],
)
def test_the_runner_publishes_from_ready_and_the_supervisor_only_once_running(
    setup, phase, publishes, collects
):
    make, env, http, _, _ = setup
    env.records._change(lambda r: setattr(r, "phase", phase))
    http.messages.responses.extend([Response({"messageIds": ["a"]}), Response({})])
    if publishes:
        assert make().publish(0, 0, 1)["message_ids"] == ["a"]
    else:
        with pytest.raises(Failure, match="admission has stopped"):
            make().publish(0, 0, 1)
    if collects:
        make("supervisor").collect("one", max_messages=1)
    else:
        with pytest.raises(Failure, match="admission has stopped"):
            make("supervisor").collect("one", max_messages=1)
    assert len(http.messages.calls) == publishes + collects


def test_message_traffic_closes_at_cleanup(setup):
    make, _, http, now, _ = setup
    now[0] += 200
    with pytest.raises(Failure, match="admission has stopped"):
        make().publish(0, 0, 1)
    with pytest.raises(Failure, match="admission has stopped"):
        make("supervisor").collect("one", max_messages=1)
    assert not http.messages.calls


@pytest.mark.parametrize("closed", ["recorded-evidence", "local-evidence", "stage"])
def test_each_admission_condition_alone_closes_message_traffic(setup, closed):
    """With no stop recorded and no call marker left to refuse first."""
    make, env, http, _, _ = setup
    runner, supervisor = make(), make("supervisor")
    if closed == "recorded-evidence":
        env.records._change(lambda r: setattr(r, "evidence_failed", True))
    elif closed == "local-evidence":
        runner.env.evidence_failure = supervisor.env.evidence_failure = {
            "operation": "observe",
            "cause": "Failure: local",
        }
    else:
        env.records._change(lambda r: r.pubsub.update(stage="cleaning"))
    assert not env.refresh().stop_requested
    writes = control_writes(env)
    with pytest.raises(Failure, match="^Pub/Sub traffic admission has stopped$"):
        runner.publish(0, 0, 1)
    with pytest.raises(Failure, match="^Pub/Sub traffic admission has stopped$"):
        supervisor.collect("one", max_messages=1)
    # A refusal records nothing, so there is no marker to clear either.
    assert not writes and not http.messages.calls
    assert all(a["inflight"] is None for a in actors(env).values())


def test_a_conflicting_begin_rechecks_admission_after_the_race(setup):
    """The begin edit runs again on the record that won, at the later time."""
    make, env, http, now, _ = setup

    def later():
        now[0] += 200
        env.records._change(lambda r: None)

    env.store.before_write = later
    with pytest.raises(Failure, match="admission has stopped"):
        make().publish(0, 0, 1)
    assert env.store.conflicts == 1 and not http.messages.calls
    assert actors(env)["runner"]["inflight"] is None


@pytest.mark.parametrize("failure", ["upload", "lost-upload-response"])
def test_evidence_failure_stops_current_and_restarted_actors(setup, failure):
    make, env, http, _, _ = setup
    original = env.store.write
    supervisor = make("supervisor")

    def write(name, value, generation="0", **kwargs):
        if name.endswith("/response.json"):
            if failure == "lost-upload-response":
                original(name, value, generation, **kwargs)
            raise Failure("response upload failed")
        return original(name, value, generation, **kwargs)

    env.store.write = write
    http.messages.responses.append(Response({"receivedMessages": [output()]}))
    with pytest.raises(Failure, match="response upload failed"):
        supervisor.collect("one", max_messages=1)
    assert supervisor.env.evidence_failed
    assert env.refresh().evidence_failed
    # The cause reaches the control record with the flag, for the receipt.
    failure = env.refresh().evidence_failure
    assert failure["operation"] == "pubsub-message-evidence"
    assert failure["actor"] == "supervisor"
    assert failure["cause"] == "Failure: response upload failed"
    assert supervisor.env.evidence_failure == failure
    env.store.write = original
    # The failed call keeps its marker, because nothing settled it; the runner
    # is refused by the stop that failure wrote.
    for actor in (supervisor, make("supervisor")):
        with pytest.raises(Failure, match="unresolved"):
            actor.collect("two", max_messages=1)
    with pytest.raises(Failure, match="admission has stopped"):
        make().publish(0, 0, 1)
    # The pull's evidence was not complete, so nothing was acknowledged.
    assert len(http.messages.calls) == 1


def test_failed_failure_record_keeps_local_latch_and_reports_restart_barrier(setup):
    make, env, http, _, _ = setup
    supervisor = make("supervisor")
    original = env.store.write

    def write(name, value, generation="0", **kwargs):
        if name.endswith("/response.json") or (
            name == env.records.path and value["evidence_failed"]
        ):
            raise Failure("storage unavailable")
        return original(name, value, generation, **kwargs)

    env.store.write = write
    http.messages.responses.append(Response({"receivedMessages": [output()]}))
    with pytest.raises(Failure, match="stop actors independently before restarting"):
        supervisor.collect("one", max_messages=1)
    assert supervisor.env.evidence_failed and not env.refresh().evidence_failed
    assert env.refresh().evidence_failure is None
    assert supervisor.env.evidence_failure["operation"] == "pubsub-message-evidence"
    with pytest.raises(Failure, match="unresolved"):
        supervisor.collect("two", max_messages=1)
    assert len(http.messages.calls) == 1


def test_both_actors_have_bound_calls_that_may_overlap(setup):
    make, env, http, _, _ = setup
    original = http.messages.request
    observed = []

    def request(*args, **kwargs):
        response = original(*args, **kwargs)
        active = actors(env)
        if args[1].endswith(":publish"):
            observed.append(active["runner"]["inflight"]["operation"])
            make("supervisor").collect("concurrent", max_messages=1)
        else:
            assert active["runner"]["inflight"]["operation"] == "publish"
            observed.append(active["supervisor"]["inflight"]["operation"])
        return response

    http.messages.request = request
    http.messages.responses.extend([Response({"messageIds": ["a"]}), Response({})])
    make().publish(0, 0, 1)
    assert observed == ["publish", "collect"]
    assert all(a["inflight"] is None for a in actors(env).values())


def test_same_actor_reentry_cannot_start_another_call(setup):
    make, env, http, _, _ = setup
    original = http.messages.request

    def request(*args, **kwargs):
        with pytest.raises(Failure, match="unresolved"):
            make().publish(0, 1, 1)
        return original(*args, **kwargs)

    http.messages.request = request
    http.messages.responses.append(Response({"messageIds": ["a"]}))
    make().publish(0, 0, 1)
    assert len(http.messages.calls) == 1
    assert actors(env)["runner"]["inflight"] is None


def test_competing_call_claim_rechecks_after_cas_conflict(setup):
    make, env, http, _, _ = setup
    http.messages.responses.append(requests.Timeout())

    def competing():
        with pytest.raises(TransportError):
            make().publish(0, 1, 1)

    env.store.before_write = competing
    with pytest.raises(Failure, match="unresolved"):
        make().publish(0, 0, 1)
    assert len(http.messages.calls) == 1 and env.store.conflicts == 1
    assert actors(env)["runner"]["inflight"]["operation"] == "publish"


@pytest.mark.parametrize("role", ["runner", "supervisor"])
def test_process_token_cannot_be_replaced_or_reused_by_another_actor(setup, role):
    make, _, http, _, _ = setup
    replacement = make(role, "d" * 32)
    with pytest.raises(Failure, match="identity changed"):
        if role == "runner":
            replacement.publish(0, 0, 1)
        else:
            replacement.collect("one", max_messages=1)
    assert not http.messages.calls


@pytest.mark.parametrize("setup", [False], indirect=True)
def test_preparation_is_covered_before_the_first_service_mutation(setup):
    make, env, http, _, _ = setup
    observed = []
    http.resources.after = lambda *args: observed.append(
        copy.deepcopy(actors(env)["runner"]["inflight"])
    )
    make().prepare()
    assert observed and all(m["operation"] == "prepare" for m in observed)
    assert len({m["id"] for m in observed}) == 1
    assert actors(env)["runner"]["inflight"] is None


@pytest.mark.parametrize("setup", [False], indirect=True)
def test_competing_supervisor_join_does_not_replace_winner(setup):
    make, env, _, _, _ = setup
    make().prepare()
    env.records.set_phase(Phase.READY)
    env.store.before_write = lambda: make("supervisor", "d" * 32).join()
    with pytest.raises(Failure, match="identity changed"):
        make("supervisor").join()
    assert actors(env)["supervisor"]["token"] == "d" * 32
    assert env.store.conflicts == 1


def test_release_during_collect_waits_for_the_call_it_cannot_release(setup):
    """A stop admits no new call, but the one under way acknowledges what it
    has durably observed and then completes."""
    make, env, http, _, _ = setup
    original = http.messages.request
    supervisor = make("supervisor")

    def request(*args, **kwargs):
        response = original(*args, **kwargs)
        with pytest.raises(Failure, match="unresolved"):
            supervisor.release()
        return response

    http.messages.request = request
    http.messages.responses.extend(
        [Response({"receivedMessages": [output()]}), Response({})]
    )
    supervisor.collect("one", max_messages=1)
    assert len(http.messages.calls) == 2
    assert env.refresh().stop_requested
    assert actors(env)["supervisor"]["inflight"] is None
    assert not actors(env)["supervisor"]["released"]
    assert any(name.endswith("/acknowledged.json") for _, name in env.store.data)
    with pytest.raises(Failure, match="admission has stopped"):
        supervisor.collect("two", max_messages=1)
    supervisor.release()
    assert actors(env)["supervisor"]["released"]


@pytest.mark.parametrize("clearing", ["succeeds", "fails"])
def test_lost_begin_response_never_calls_service_and_clears_its_marker(setup, clearing):
    """Nothing was sent, so the landed marker is cleared rather than kept for
    a reclamation proof; only a failed clearing keeps it, and the lock."""
    make, env, http, _, _ = setup
    original = env.store.write

    def write(name, value, generation="0", **kwargs):
        if name == env.records.path and clearing == "fails" and lost:
            raise Failure("control storage unavailable")
        result = original(name, value, generation, **kwargs)
        if (
            name == env.records.path
            and value["pubsub"]["handoff"]["actors"]["runner"]["inflight"]
        ):
            lost.append(name)
            raise Failure("lost begin response")
        return result

    lost = []
    env.store.write = write
    with pytest.raises(Failure, match="lost begin"):
        make().publish(0, 0, 1)
    assert lost and not http.messages.calls
    env.store.write = original
    if clearing == "fails":
        assert actors(env)["runner"]["inflight"]["operation"] == "publish"
        with pytest.raises(Failure, match="unresolved"):
            make().release()
        return
    assert actors(env)["runner"]["inflight"] is None
    assert not env.refresh().stop_requested
    make().release()
    assert actors(env)["runner"]["released"]


def test_lost_finish_response_retries_only_acknowledgement(setup):
    make, env, http, _, _ = setup
    original = env.store.write
    lost = []

    def write(name, value, generation="0", **kwargs):
        previous = actors(env)["runner"]["inflight"]
        result = original(name, value, generation, **kwargs)
        if (
            name == env.records.path
            and previous
            and value["pubsub"]["handoff"]["actors"]["runner"]["inflight"] is None
            and not lost
        ):
            lost.append(previous)
            raise Failure("lost finish response")
        return result

    env.store.write = write
    http.messages.responses.append(Response({"messageIds": ["a"]}))
    make().publish(0, 0, 1)
    assert lost and len(http.messages.calls) == 1
    assert actors(env)["runner"]["inflight"] is None


def test_old_finish_retry_does_not_clear_a_newer_unresolved_call(setup):
    make, env, http, _, _ = setup
    original = env.store.write
    lost = []

    def write(name, value, generation="0", **kwargs):
        previous = actors(env)["runner"]["inflight"]
        result = original(name, value, generation, **kwargs)
        if (
            name == env.records.path
            and previous
            and value["pubsub"]["handoff"]["actors"]["runner"]["inflight"] is None
            and not lost
        ):
            lost.append(previous)
            with pytest.raises(TransportError):
                make().publish(0, 1, 1)
            raise Failure("lost finish response")
        return result

    env.store.write = write
    http.messages.responses.extend(
        [Response({"messageIds": ["a"]}), requests.Timeout()]
    )
    with pytest.raises(Failure, match="identity changed before completion"):
        make().publish(0, 0, 1)
    assert len(http.messages.calls) == 2
    assert actors(env)["runner"]["inflight"]["id"] != lost[0]["id"]


@pytest.mark.parametrize("failure", ["transport", "evidence"])
def test_failed_call_keeps_its_marker_and_blocks_cleanup(setup, failure):
    make, env, http, _, _ = setup
    if failure == "transport":
        http.messages.responses.append(requests.Timeout())
    else:
        env.store.fail_evidence = True
    with pytest.raises(Failure):
        make().publish(0, 0, 1)
    marker = copy.deepcopy(actors(env)["runner"]["inflight"])
    assert marker and env.refresh().stop_requested
    with pytest.raises(Failure, match="unresolved"):
        make().release()
    with pytest.raises(Failure, match="released"):
        make("supervisor").cleanup(lambda: True)
    assert not deletions(http)
    # Nothing reclaims the call: its marker, and with it the lock, stays for
    # an operator.
    assert actors(env)["runner"]["inflight"] == marker
    if failure == "evidence":
        recorded = env.refresh().evidence_failure
        assert recorded["operation"] == "pubsub-message-evidence"
        assert recorded["cause"] == "Failure: Evidence unavailable"


def test_service_cleanup_cannot_bypass_unreleased_handoff(setup):
    make, env, http, _, _ = setup
    with pytest.raises(Failure, match="released"):
        make("supervisor").controller.cleanup(lambda: True)
    assert not deletions(http)
    env.records._change(lambda r: r.pubsub.update(stage="cleaned"))
    with pytest.raises(Failure, match="released"):
        require_pubsub_clean(env.refresh())


def test_cooperative_cleanup_requires_both_actors_and_external_barrier(setup):
    make, env, http, _, _ = setup
    make().release()
    with pytest.raises(Failure, match="released"):
        make("supervisor").cleanup(lambda: True)
    make("supervisor").release()
    with pytest.raises(Failure, match="not quiescent"):
        make("supervisor").cleanup(lambda: False)
    assert not deletions(http)
    make("supervisor").cleanup(lambda: True)
    assert not http.resources.resources
    assert env.refresh().pubsub["stage"] == "cleaned"


@pytest.mark.parametrize("setup", [False], indirect=True)
def test_ambiguous_preparation_keeps_its_marker_and_resources(setup):
    make, env, http, _, _ = setup

    def lost(method, name):
        if method == "PUT":
            raise requests.Timeout("create accepted")

    http.resources.after = lost
    with pytest.raises(TransportError):
        make().prepare()
    marker = copy.deepcopy(actors(env)["runner"]["inflight"])
    assert marker["operation"] == "prepare" and len(http.resources.resources) == 1
    http.resources.after = lambda *args: None
    with pytest.raises(Failure, match="released"):
        make("supervisor").cleanup(lambda: True)
    assert len(http.resources.resources) == 1 and not deletions(http)
    assert actors(env)["runner"]["inflight"] == marker
    assert actors(env)["supervisor"] is None


def test_exhausted_finish_acknowledgements_leave_call_unresolved(setup):
    make, env, http, _, _ = setup
    original = env.store.write
    attempts = []

    def write(name, value, generation="0", **kwargs):
        if (
            name == env.records.path
            and actors(env)["runner"]["inflight"] is not None
            and value["pubsub"]["handoff"]["actors"]["runner"]["inflight"] is None
        ):
            attempts.append(name)
            raise Failure("completion storage unavailable")
        return original(name, value, generation, **kwargs)

    env.store.write = write
    http.messages.responses.append(Response({"messageIds": ["a"]}))
    with pytest.raises(Failure, match="completion storage unavailable"):
        make().publish(0, 0, 1)
    assert len(attempts) == 3 and len(http.messages.calls) == 1
    assert actors(env)["runner"]["inflight"]["operation"] == "publish"
    env.store.write = original
    with pytest.raises(Failure, match="unresolved"):
        make().release()
    assert not actors(env)["runner"]["released"]


def test_lost_release_response_can_be_acknowledged_without_restoring_authority(setup):
    make, env, http, _, _ = setup
    original = env.store.write
    lost = []

    def write(name, value, generation="0", **kwargs):
        result = original(name, value, generation, **kwargs)
        if (
            name == env.records.path
            and value["pubsub"]["handoff"]["actors"]["runner"]["released"]
            and not lost
        ):
            lost.append(name)
            raise Failure("release response lost")
        return result

    env.store.write = write
    with pytest.raises(Failure, match="release response lost"):
        make().release()
    make().release()
    with pytest.raises(Failure, match="released"):
        make().publish(0, 0, 1)
    assert not http.messages.calls and actors(env)["runner"]["released"]


@pytest.mark.parametrize("setup", [False], indirect=True)
def test_unclaimed_supervisor_cannot_join_after_runner_release(setup):
    make, env, http, _, _ = setup
    make().prepare()
    env.records.set_phase(Phase.READY)
    make().release()
    with pytest.raises(Failure, match="admission"):
        make("supervisor").join()
    make("supervisor").cleanup(lambda: True)
    assert actors(env)["supervisor"] is None and not http.resources.resources


@pytest.mark.parametrize("setup", [False], indirect=True)
def test_binding_cannot_adopt_prepared_resources(setup):
    make, env, _, _, _ = setup
    make().controller.prepare()
    with pytest.raises(Failure, match="binding must precede"):
        env.records._change(lambda r: r.pubsub.pop("handoff"))
        make().initialize()
    # A pre-existing prepared resource set cannot be adopted by this protocol.
    assert "handoff" not in env.refresh().pubsub


@pytest.mark.parametrize("setup", [False], indirect=True)
@pytest.mark.parametrize("change", ["deadline", "stop", "evidence"])
def test_local_stop_or_deadline_refuses_preparation_without_call_marker(setup, change):
    make, env, http, now, _ = setup
    runner = make()
    if change == "deadline":
        now[0] += 200
    elif change == "stop":
        runner.env.stopping = True
    else:
        runner.env.evidence_failure = {
            "operation": "observe",
            "cause": "Failure: local",
        }
    with pytest.raises(Failure, match="expired or stopped"):
        runner.initialize()
    with pytest.raises(Failure, match="expired or stopped"):
        runner.prepare()
    assert not http.resources.calls
    assert actors(env)["runner"]["inflight"] is None
