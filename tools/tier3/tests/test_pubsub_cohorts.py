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
"""Cohorts the supervisor requests and the runner publishes while it settles."""

import base64
import contextlib
import math
import uuid

import pytest
import requests
from flink_tier3.common import Failure
from flink_tier3.model import Phase
from flink_tier3.policy import HTTP_TIMEOUT
from flink_tier3.pubsub_handoff import CohortUnstarted
from flink_tier3.pubsub_messages import COHORTS, cohort_ranges
from flink_tier3.pubsub_output import OutputCollector, parse
from flink_tier3.pubsub_traffic import TrafficLimits
from flink_tier3.runner import Runner
from flink_tier3.supervisor import Supervisor
from test_pubsub_handoff import actors
from test_pubsub_messages import Response
from test_pubsub_runtime import compose
from test_tier3_lifecycle import env as env  # noqa: PLC0414
from test_tier3_lifecycle import obj, rt

RECORDS = 6
FIRST, REPLAY, LAST = COHORTS


@pytest.fixture
def run(env):
    return composed(env, publish_calls=20)


@pytest.fixture
def scarce(env):
    """Publication calls for two cohorts of the three."""
    return composed(env, publish_calls=4)


def composed(env, *, publish_calls):
    clock = env[3]
    a = compose(
        env,
        "running",
        records=RECORDS,
        limits=TrafficLimits(
            publish_calls, 20, 40, 2000, 400, 100, 200000, clock() + 1000
        ),
    )
    messages = a.service.messages
    original = messages.request
    a.published = []

    def request(method, url, **kwargs):
        if url.endswith(":publish") and not messages.responses:
            data = [m["data"] for m in kwargs["json"]["messages"]]
            a.published.append(
                {
                    "at": a.clock(),
                    "payloads": [base64.b64decode(d).decode() for d in data],
                    # The control record as the request leaves.
                    "cohorts": cohorts(a),
                }
            )
            # Each publication takes time, so a cohort's marks can be told
            # apart from its requests.
            a.clock.sleep(1)
            messages.responses.append(
                Response({"messageIds": [uuid.uuid4().hex for _ in data]})
            )
        return original(method, url, **kwargs)

    messages.request = request
    job = a.kube.put(obj("Job", "supervisor", rt.SYSTEM))
    a.sender.env.remember("supervisor", job)
    return a


def cohorts(a):
    return a.sender.env.refresh().pubsub.get("cohorts", {})


def payloads(a):
    return [p for batch in a.published for p in batch["payloads"]]


def cohort_payloads(a, name):
    cohort = cohort_ranges(RECORDS)[name]
    return [
        f"v1|{a.approval.run_id}|{index}|{sequence}"
        for index in range(2)
        for sequence in range(cohort["start"], cohort["start"] + cohort["count"])
    ]


def first(a):
    a.sender.publish_cohort(FIRST, deadline=a.clock() + 100)


@pytest.mark.parametrize(
    "records,bounds",
    [(3, (0, 1, 2, 3)), (5, (0, 1, 3, 5)), (1000, (0, 333, 666, 1000))],
)
def test_cohorts_split_at_one_and_two_thirds(records, bounds):
    assert cohort_ranges(records) == {
        name: {"start": bounds[i], "count": bounds[i + 1] - bounds[i]}
        for i, name in enumerate(COHORTS)
    }


@pytest.mark.parametrize("records", [2, 10001, True, 3.0])
def test_cohorts_need_three_to_ten_thousand_records(records):
    with pytest.raises(Failure, match="cohort domain"):
        cohort_ranges(records)


def test_cohorts_are_published_once_each_and_in_order(run):
    a = run
    first(a)
    with pytest.raises(Failure, match="before the previous one"):
        a.observer.request_cohort(LAST, deadline=a.clock() + 100)
    deadline = a.clock() + 100
    a.observer.request_cohort(REPLAY, deadline=deadline)
    # The same request again changes nothing; another deadline is refused.
    a.observer.request_cohort(REPLAY, deadline=deadline)
    with pytest.raises(Failure, match="cannot be changed"):
        a.observer.request_cohort(REPLAY, deadline=deadline + 1)
    with pytest.raises(Failure, match="before the previous one"):
        a.observer.request_cohort(LAST, deadline=deadline)
    assert a.sender.serve() == REPLAY
    assert a.sender.serve() is None
    with pytest.raises(Failure, match="already started"):
        a.sender.publish_cohort(REPLAY)
    a.observer.request_cohort(LAST, deadline=deadline)
    assert a.sender.serve() == LAST
    assert payloads(a) == [
        *cohort_payloads(a, FIRST),
        *cohort_payloads(a, REPLAY),
        *cohort_payloads(a, LAST),
    ]
    recorded = cohorts(a)
    assert list(recorded) == list(COHORTS)
    assert recorded[FIRST]["requested_at"] is None
    for name in (REPLAY, LAST):
        assert recorded[name]["deadline"] == deadline
    assert a.observer.cohorts() == recorded


def test_a_cohort_s_marks_bracket_its_publications(run):
    a = run
    first(a)
    a.observer.request_cohort(REPLAY, deadline=a.clock() + 100)
    a.clock.sleep(7)
    before = len(a.published)
    a.sender.serve()
    batches = a.published[before:]
    value = cohorts(a)[REPLAY]
    # The first request left with the start already durable, after the
    # request, and the end was recorded after the last one returned.
    assert batches[0]["cohorts"][REPLAY]["started_at"] == value["started_at"]
    assert value["requested_at"] < value["started_at"] <= batches[0]["at"]
    assert value["published_at"] > batches[-1]["at"]


def test_admission_s_cohort_is_published_once(run):
    a = run
    first(a)
    with pytest.raises(Failure, match="already started"):
        first(a)
    with pytest.raises(ValueError, match="first Pub/Sub cohort"):
        a.sender.publish_cohort(FIRST)
    with pytest.raises(ValueError, match="first Pub/Sub cohort"):
        a.sender.publish_cohort(REPLAY, deadline=a.clock() + 100)


def test_a_started_cohort_is_never_started_again(run):
    a = run
    first(a)
    a.observer.request_cohort(REPLAY, deadline=a.clock() + 100)
    a.service.messages.responses.append(requests.Timeout())
    with pytest.raises(Failure):
        a.sender.serve()
    value = cohorts(a)[REPLAY]
    assert value["started_at"] is not None and value["published_at"] is None
    # The failed batch stopped the run; nothing republishes the cohort.
    assert a.sender.env.refresh().stop_requested
    assert a.sender.serve() is None
    with pytest.raises(Failure, match="admission has stopped"):
        a.sender.publish_cohort(REPLAY)


@pytest.mark.parametrize("left", [0, HTTP_TIMEOUT - 1])
def test_a_request_too_close_to_its_deadline_is_not_started(run, left):
    """A start the guard could not follow with a request is never recorded."""
    a = run
    first(a)
    a.observer.request_cohort(REPLAY, deadline=a.clock() + 30)
    a.clock.now += 30 - left
    with pytest.raises(Failure, match="expired"):
        a.sender.serve()
    assert cohorts(a)[REPLAY]["started_at"] is None
    assert payloads(a) == cohort_payloads(a, FIRST)


def test_a_request_exactly_one_request_budget_from_its_deadline_starts(run):
    """The start's rule is the guard's: a request may begin at its budget."""
    a = run
    first(a)
    a.observer.request_cohort(REPLAY, deadline=a.clock() + 30)
    a.clock.now += 30 - HTTP_TIMEOUT
    assert a.sender.serve() == REPLAY
    assert cohorts(a)[REPLAY]["published_at"] is not None


def test_a_restarted_runner_continues_from_the_recorded_cohorts(run):
    a = run
    first(a)
    a.observer.request_cohort(REPLAY, deadline=a.clock() + 100)
    # Another process of the same runner binding, with nothing in memory.
    restarted = a.make("runner", "b" * 32)
    assert restarted.serve() == REPLAY
    assert restarted.serve() is None
    with pytest.raises(Failure, match="already started"):
        a.sender.publish_cohort(REPLAY)
    # Another process identity has no authority to publish what waits.
    a.observer.request_cohort(LAST, deadline=a.clock() + 100)
    with pytest.raises(Failure, match="identity changed"):
        a.make("runner", "d" * 32).serve()
    assert cohorts(a)[LAST]["started_at"] is None
    assert payloads(a) == [*cohort_payloads(a, FIRST), *cohort_payloads(a, REPLAY)]


def test_a_request_whose_write_response_was_lost_is_confirmed_by_a_retry(run):
    a = run
    first(a)
    original = a.store.write
    lost = []

    def write(name, value, generation="0", **kwargs):
        result = original(name, value, generation, **kwargs)
        requested = ((value.get("pubsub") or {}).get("cohorts") or {}).get(REPLAY)
        if name == a.sender.env.records.path and requested and not lost:
            lost.append(name)
            raise Failure("lost request acknowledgement")
        return result

    a.store.write = write
    deadline = a.clock() + 100
    with pytest.raises(Failure, match="lost request"):
        a.observer.request_cohort(REPLAY, deadline=deadline)
    a.observer.request_cohort(REPLAY, deadline=deadline)
    assert lost and cohorts(a)[REPLAY]["deadline"] == deadline
    assert a.sender.serve() == REPLAY


def test_an_exhausted_publication_budget_fails_the_cohort_and_stops(scarce):
    a = scarce
    first(a)
    a.observer.request_cohort(REPLAY, deadline=a.clock() + 100)
    assert a.sender.serve() == REPLAY
    a.observer.request_cohort(LAST, deadline=a.clock() + 100)
    with pytest.raises(Failure, match="budget exhausted: publish_calls"):
        a.sender.serve()
    value = cohorts(a)[LAST]
    assert value["started_at"] is not None and value["published_at"] is None
    assert a.sender.env.refresh().stop_requested
    assert payloads(a) == [*cohort_payloads(a, FIRST), *cohort_payloads(a, REPLAY)]


@pytest.mark.parametrize("offset", [0, None])
def test_a_request_deadline_lies_inside_the_run_window(run, offset):
    a = run
    first(a)
    cleanup_at = a.sender.env.schedule.cleanup_at
    if offset is None:
        a.observer.request_cohort(REPLAY, deadline=cleanup_at)
        assert cohorts(a)[REPLAY]["deadline"] == cleanup_at
        return
    for deadline in (a.clock(), cleanup_at + 1):
        with pytest.raises(Failure, match="outside the run window"):
            a.observer.request_cohort(REPLAY, deadline=deadline)
    for deadline in (math.nan, math.inf, True, "soon"):
        with pytest.raises(ValueError, match="finite time"):
            a.observer.request_cohort(REPLAY, deadline=deadline)


def test_a_repeated_request_confirms_a_recorded_one_after_its_deadline_or_a_stop(
    run,
):
    a = run
    first(a)
    deadline = a.clock() + 30
    a.observer.request_cohort(REPLAY, deadline=deadline)
    a.clock.now = deadline + 1
    a.observer.stop()
    # A retry after a lost response learns the request is recorded.
    a.observer.request_cohort(REPLAY, deadline=deadline)
    with pytest.raises(Failure, match="cannot be changed"):
        a.observer.request_cohort(REPLAY, deadline=deadline + 60)


def test_a_stopped_run_takes_no_new_request(run):
    a = run
    first(a)
    a.observer.stop()
    with pytest.raises(Failure, match="admission has stopped"):
        a.observer.request_cohort(REPLAY, deadline=a.clock() + 100)


def test_each_side_keeps_its_own_role(run):
    a = run
    with pytest.raises(Failure, match="runner"):
        a.observer.publish_cohort(FIRST, deadline=a.clock() + 100)
    with pytest.raises(Failure, match="supervisor"):
        a.sender.request_cohort(REPLAY, deadline=a.clock() + 100)
    with pytest.raises(ValueError):
        a.observer.request_cohort(FIRST, deadline=a.clock() + 100)


@pytest.mark.parametrize(
    "tamper",
    [
        lambda c: c.update(unknown=dict(c[FIRST])),
        lambda c: c[FIRST].update(requested_at=1),
        lambda c: c[FIRST].update(started_at=None),
        lambda c: c[FIRST].pop("deadline"),
        lambda c: c[FIRST].update(deadline=math.nan),
        lambda c: c[FIRST].update(deadline=True),
        lambda c: c[FIRST].update(published_at=math.inf),
        # A later cohort exists only as requested, after the one before it.
        lambda c: c.update(
            after_checkpoint={
                "deadline": 1,
                "requested_at": None,
                "started_at": None,
                "published_at": None,
            }
        ),
        lambda c: c.update(
            after_recovery={
                "deadline": 1,
                "requested_at": 1,
                "started_at": None,
                "published_at": None,
            }
        ),
    ],
)
def test_a_malformed_cohort_record_is_refused(run, tamper):
    a = run
    first(a)

    def edit(record):
        tamper(record.pubsub["cohorts"])

    a.sender.env.records._change(edit)
    with pytest.raises(Failure, match="cohort record"):
        a.observer.cohorts()


def sleeping(a, steps):
    """Run the supervisor's side, one step after each of the runner's sleeps."""
    original = a.sender.env.sleep
    pending = list(steps)

    def sleep(seconds):
        original(seconds)
        if pending:
            pending.pop(0)()

    a.sender.env.sleep = sleep
    return pending


def runner_events(a):
    return [
        a.store.read(item["name"])[0]["event"]
        for item in a.store.objects("runs/" + a.approval.run_id + "/runner/")
    ]


def test_settlement_serves_requests_until_the_supervisor_stops(run):
    a = run
    first(a)
    runner = Runner(a.sender.env, pubsub=a.sender)
    supervisor = Supervisor(a.observer.env, pubsub=a.observer, quiesce=lambda: True)
    seen = {}

    def request():
        assert not actors(a.sender.env)["runner"]["released"]
        a.observer.request_cohort(REPLAY, deadline=a.clock() + 100)

    def stop():
        seen["served"] = cohorts(a)[REPLAY]["published_at"] is not None
        a.observer.stop()

    def clean():
        seen["released"] = actors(a.sender.env)["runner"]["released"]
        supervisor.cleanup.run("synthetic completion", True)

    pending = sleeping(a, [request, stop, clean])
    runner.settle()
    assert not pending
    assert seen == {"served": True, "released": True}
    assert payloads(a) == [*cohort_payloads(a, FIRST), *cohort_payloads(a, REPLAY)]
    assert a.sender.env.refresh().idle and a.sender.env.refresh().success


def ended(a):
    job = a.sender.env.root("supervisor")
    job["status"] = {"succeeded": 1}
    a.kube.put(job)


def evidence_failed(a):
    a.observer.env.records.mark_evidence_failed()


def left_running(a):
    def edit(record):
        record.phase = Phase.CLEANING

    a.sender.env.records._change(edit)


def past_cleanup(a):
    a.clock.now = a.sender.env.schedule.cleanup_at


@pytest.mark.parametrize("pending", [True, False])
@pytest.mark.parametrize("close", [ended, evidence_failed, left_running, past_cleanup])
def test_settlement_stops_serving_and_releases_once_the_run_closes(run, close, pending):
    """A request left behind is not served once the run closed without a stop.

    Only a supervisor deletes Pub/Sub resources, so the runner's settlement
    then stops short of completion and the lock stays for an operator.
    """
    a = run
    first(a)
    runner = Runner(a.sender.env, pubsub=a.sender)
    seen = {}

    def closing():
        # Without a request waiting, nothing but the close itself can end
        # serving: no refused start stands in for it.
        if pending:
            a.observer.request_cohort(REPLAY, deadline=a.sender.env.schedule.cleanup_at)
        close(a)

    def after():
        seen["released"] = actors(a.sender.env)["runner"]["released"]

    sleeping(a, [closing, after])
    with contextlib.suppress(Failure):
        runner.settle()
    assert actors(a.sender.env)["runner"]["released"]
    assert seen.get("released", True)
    assert payloads(a) == cohort_payloads(a, FIRST)
    # Released because the run closed, not after trying to publish into it.
    assert "pubsub-publication-failed" not in runner_events(a)


def test_a_failed_publication_releases_and_withholds_success(run):
    a = run
    first(a)
    runner = Runner(a.sender.env, pubsub=a.sender)
    supervisor = Supervisor(a.observer.env, pubsub=a.observer, quiesce=lambda: True)
    seen = {}

    def request():
        # Refused before its start, so no call is left unresolved.
        a.observer.request_cohort(REPLAY, deadline=a.clock() + HTTP_TIMEOUT - 1)

    def clean():
        seen["released"] = actors(a.sender.env)["runner"]["released"]
        supervisor.cleanup.run("synthetic completion", True)

    pending = sleeping(a, [request, clean])
    runner.settle()
    assert not pending and seen == {"released": True}
    assert "pubsub-publication-failed" in runner_events(a)
    control = a.sender.env.refresh()
    assert control.idle and not control.success


def test_a_stop_between_the_poll_and_the_start_is_a_close_not_a_failure(run):
    """The supervisor may stop after the runner read a pending request."""
    a = run
    first(a)
    runner = Runner(a.sender.env, pubsub=a.sender)
    supervisor = Supervisor(a.observer.env, pubsub=a.observer, quiesce=lambda: True)
    read = a.sender.cohorts
    seen = {}

    def stopped_after_read():
        value = read()
        a.observer.stop()
        return value

    def request():
        a.observer.request_cohort(REPLAY, deadline=a.clock() + 100)
        a.sender.cohorts = stopped_after_read

    def clean():
        seen["released"] = actors(a.sender.env)["runner"]["released"]
        supervisor.cleanup.run("synthetic completion", True)

    pending = sleeping(a, [request, clean])
    runner.settle()
    assert not pending and seen == {"released": True}
    assert cohorts(a)[REPLAY]["started_at"] is None
    assert "pubsub-publication-failed" not in runner_events(a)
    control = a.sender.env.refresh()
    assert control.idle and control.success


def test_a_replaced_traffic_binding_is_a_failure_not_a_close(run):
    a = run
    first(a)
    a.observer.request_cohort(REPLAY, deadline=a.clock() + 100)

    def edit(record):
        record.pubsub["traffic"]["binding"]["records_per_subscription"] = 9

    a.sender.env.records._change(edit)
    with pytest.raises(Failure, match="traffic binding") as raised:
        a.sender.publish_cohort(REPLAY)
    assert not isinstance(raised.value, CohortUnstarted)


def test_an_unresolved_publication_keeps_its_marker_and_the_lock(run):
    a = run
    first(a)
    runner = Runner(a.sender.env, pubsub=a.sender)

    def request():
        a.observer.request_cohort(REPLAY, deadline=a.clock() + 100)
        a.service.messages.responses.append(requests.Timeout())

    def check():
        assert actors(a.sender.env)["runner"]["inflight"] is not None
        assert actors(a.sender.env)["runner"]["released"] is False

    pending = sleeping(a, [request, check])
    with pytest.raises(Failure, match="cleanup is incomplete"):
        runner.settle()
    assert not pending
    events = runner_events(a)
    assert "pubsub-publication-failed" in events
    assert "pubsub-release-blocked" in events


@pytest.mark.parametrize("request_stop", [True, False])
def test_a_stopping_runner_releases_before_it_waits(run, request_stop):
    a = run
    runner = Runner(a.sender.env, pubsub=a.sender)
    a.sender.env.stopping = not request_stop
    released = []

    def wait(predicate, deadline):
        released.append(actors(a.sender.env)["runner"]["released"])
        raise Failure("stop here")

    a.sender.env.wait = wait
    with pytest.raises(Failure):
        runner.settle(request_stop=request_stop)
    assert released == [True]


def test_a_settlement_that_fails_before_its_wait_still_releases(run, monkeypatch):
    a = run
    runner = Runner(a.sender.env, pubsub=a.sender)

    def adopt_root(key):
        raise Failure("Temporary object differs from persisted creation intent")

    monkeypatch.setattr(runner, "adopt_root", adopt_root)
    with pytest.raises(Failure, match="creation intent"):
        runner.settle()
    assert actors(a.sender.env)["runner"]["released"]


def output_line(message_id, payload):
    def encode(value):
        return base64.urlsafe_b64encode(value.encode()).decode().rstrip("=")

    return encode(message_id) + "\t" + encode(payload)


def relayed(run_id, index, sequence, *, attempt=None, phase="initial", restored=False):
    message = base64.urlsafe_b64encode(b"input-1").decode().rstrip("=")
    return "|".join(
        [
            "v1",
            run_id,
            str(index),
            str(sequence),
            message,
            attempt or str(uuid.uuid4()),
            str(uuid.uuid4()),
            phase,
            "true" if restored else "false",
        ]
    )


def test_parse_accepts_only_this_relay_s_output():
    attempt = str(uuid.uuid4())
    good = relayed("run", 1, 4, attempt=attempt, phase="upgrade", restored=True)
    parsed = parse(output_line("out-1", good), "run", 6)
    assert parsed["input_index"] == 1 and parsed["sequence"] == 4
    assert parsed["input_message_id"] == "input-1"
    assert parsed["attempt"] == attempt and parsed["restored"] is True
    assert parsed["output_message_id"] == "out-1" and parsed["phase"] == "upgrade"
    fields = good.split("|")
    for index, value in [
        (0, "v2"),
        (1, "other"),
        (2, "2"),
        (3, "6"),
        (3, "04"),
        (4, ""),
        (5, "attempt"),
        (7, "final"),
        (8, "yes"),
    ]:
        changed = list(fields)
        changed[index] = value
        assert parse(output_line("out-1", "|".join(changed)), "run", 6) is None
    assert parse(output_line("out-1", good + "|extra"), "run", 6) is None
    assert parse("not\ttwo\tfields", "run", 6) is None
    assert parse(output_line("", good), "run", 6) is None
    # Padded base64 is not the collector's canonical form.
    assert parse(output_line("out-1", good) + "=", "run", 6) is None


def received(lines):
    return Response(
        {
            "receivedMessages": [
                {
                    "ackId": f"ack-{n}",
                    "message": {
                        "messageId": base64.urlsafe_b64decode(
                            line.split("\t")[0] + "=="
                        ).decode(),
                        "data": line.split("\t")[1],
                    },
                }
                for n, line in enumerate(lines)
            ]
        }
    )


def test_the_collector_pulls_through_the_handoff_until_a_short_batch(run):
    a = run
    run_id = a.approval.run_id
    lines = [
        output_line(f"out-{i}", relayed(run_id, i % 2, i // 2)) for i in range(RECORDS)
    ] + [output_line("foreign", "v1|other|0|0")]
    collector = OutputCollector(a.observer)
    a.service.messages.responses.extend(
        # Each nonempty pull is followed by its acknowledgement.
        [
            received(lines[:4]),
            Response({}),
            Response({}),
            received(lines[4:]),
            Response({}),
        ]
    )
    assert collector.drain(5) == 4  # the first batch is short: four of 100
    assert collector.drain(5) == 0
    assert collector.drain(1) == 3
    assert collector.complete(FIRST)
    assert not collector.complete(LAST)
    assert collector.processed(0, 0, RECORDS) == {0, 1, 2}
    assert collector.processed(1, 0, RECORDS, where=lambda o: o["restored"]) == set()
    batches = [f["batch"] for f in collector.foreign]
    assert len(batches) == 1 and batches[0].endswith("-000003")
    assert len(collector.observations) == RECORDS
    with pytest.raises(ValueError):
        collector.drain(0)


def test_a_drain_stops_at_its_pull_cap(run):
    a = run
    run_id = a.approval.run_id
    full = [output_line(f"o{i}", relayed(run_id, 0, 0)) for i in range(100)]
    a.service.messages.responses.extend(
        [received(full), Response({}), received(full), Response({})]
    )
    collector = OutputCollector(a.observer)
    assert collector.drain(2) == 200
    assert collector.batches == 2 and not a.service.messages.responses


def test_two_collectors_never_share_a_batch_id(run):
    a = run
    a.service.messages.responses.extend([Response({}), Response({})])
    one, two = OutputCollector(a.observer), OutputCollector(a.observer)
    one.pull()
    two.pull()
    assert one.prefix != two.prefix


def test_a_collection_whose_lines_differ_from_its_count_is_refused(run, monkeypatch):
    a = run
    collect = a.observer.collect

    def short(batch_id, **kwargs):
        return {**collect(batch_id, **kwargs), "count": 1}

    monkeypatch.setattr(a.observer, "collect", short)
    a.service.messages.responses.append(Response({}))
    with pytest.raises(Failure, match="differ from their count"):
        OutputCollector(a.observer).pull()


def test_the_collector_follows_its_handoff_s_admission(run):
    a = run
    collector = OutputCollector(a.observer)
    a.observer.stop()
    with pytest.raises(Failure, match="admission has stopped"):
        collector.pull()
