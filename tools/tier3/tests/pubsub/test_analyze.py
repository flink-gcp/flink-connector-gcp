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
"""Recomputing a deployed Pub/Sub verdict from the evidence a run exported.

Every case starts from a directory a simulated run actually wrote, through the
same supervisor, collector and message helpers a deployed run uses, so the
seam between the exercise and the analyzer is crossed for real; the cases
then remove or edit what a partial download or a forged record would.
"""

import base64
import json
import shutil

import pytest
from flink_tier3 import analyze
from flink_tier3.common import INCONCLUSIVE, INCONSISTENT, TAMPERED, UNEXPORTED, USABLE
from flink_tier3.pubsub import analyze as pubsub_analyze
from flink_tier3.pubsub.messages import COHORTS, cohort_ranges
from flink_tier3.pubsub.output import parse

from ..test_lifecycle import env as env  # noqa: PLC0414
from ..test_lifecycle import rt
from .test_actors import prepared as prepared  # noqa: PLC0414
from .test_admission import admitting as admitting  # noqa: PLC0414
from .test_bundle import approval as approval  # noqa: PLC0414
from .test_exercise import RECORDS, World, receipt, stage
from .test_exercise import trial as trial  # noqa: PLC0414
from .test_plan import inputs as inputs  # noqa: PLC0414
from .test_plan import renderer as renderer  # noqa: PLC0414


def one(kind):
    return pytest.mark.parametrize("trial", [kind], indirect=True)


def export(world, control, root):
    """The run directory as `gcloud storage cp --recursive runs/<id>/` leaves it.

    The receipt is derived by the runner from the run's own control record,
    and the approval is the one the run was admitted under.
    """
    approval = world.a.environment.approval
    prefix = f"runs/{approval.run_id}/"
    for (bucket, name), (value, _) in world.a.environment.store.data.items():
        if bucket == rt.EVIDENCE and name.startswith(prefix):
            path = root / name[len("runs/") :]
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(json.dumps(value))
    run_dir = root / approval.run_id
    (run_dir / "approval.json").write_text(json.dumps(approval.to_dict()))
    (run_dir / "result.json").write_text(json.dumps(receipt(world, control)))
    return run_dir


def assess(run_dir):
    (run,) = analyze.discover_runs(run_dir)
    return pubsub_analyze.assess(run)


@pytest.fixture
def exported(admitting, monkeypatch, tmp_path):
    """One completed trial's exported evidence, and the record it wrote."""
    world = World(admitting, monkeypatch)
    control = world.run()
    assert control.recovery["stage"] == "complete", control.reason
    return export(world, control, tmp_path / "evidence"), control


def record_file(run_dir):
    """The supervisor's `recovery-complete` evidence document."""
    for path in (run_dir / "supervisor").glob("*.json"):
        if json.loads(path.read_text())["event"] == "recovery-complete":
            return path
    raise AssertionError("no recovery-complete evidence")


def edit_record(run_dir, change):
    """Edit the record in both places it lives: the evidence and the receipt."""
    path = record_file(run_dir)
    document = json.loads(path.read_text())
    change(document["payload"])
    path.write_text(json.dumps(document))
    result = json.loads((run_dir / "result.json").read_text())
    result["recovery"] = document["payload"]
    (run_dir / "result.json").write_text(json.dumps(result))


def edit_everywhere(run_dir, name, change):
    """Edit one outcome alike in every `recovery-*` record holding it, and the receipt."""
    for path in (run_dir / "supervisor").glob("*.json"):
        document = json.loads(path.read_text())
        payload = document["payload"]
        outcomes = payload.get("outcomes") if isinstance(payload, dict) else None
        if document["event"].startswith("recovery-") and name in (outcomes or {}):
            change(outcomes[name])
            path.write_text(json.dumps(document))
    result = json.loads((run_dir / "result.json").read_text())
    change(result["recovery"]["outcomes"][name])
    (run_dir / "result.json").write_text(json.dumps(result))


def edit_result(run_dir, change):
    result = json.loads((run_dir / "result.json").read_text())
    change(result)
    (run_dir / "result.json").write_text(json.dumps(result))


def encode(value):
    return base64.urlsafe_b64encode(value).decode().rstrip("=")


def received(line):
    """The pull response's message a collected line was derived from."""
    message_id, payload = (
        base64.urlsafe_b64decode(part + "=" * (-len(part) % 4))
        for part in line.split("\t")
    )
    return {
        "ackId": "ack-" + message_id.decode(),
        "message": {
            "messageId": message_id.decode(),
            "data": base64.b64encode(payload).decode(),
        },
    }


def append_line(path, line):
    """One more collected line in a batch, and the response message it came from."""
    document = json.loads(path.read_text())
    document["tsv"] += line + "\n"
    document["count"] += 1
    path.write_text(json.dumps(document))
    response_path = path.parent / "response.json"
    response = json.loads(response_path.read_text())
    response.setdefault("receivedMessages", []).append(received(line))
    response_path.write_text(json.dumps(response))


def replace_line(path, old, new):
    """Replace one collected line in a batch and in its pull response alike."""
    document = json.loads(path.read_text())
    lines = document["tsv"].splitlines()
    index = lines.index(old)
    lines[index] = new
    document["tsv"] = "".join(line + "\n" for line in lines)
    path.write_text(json.dumps(document))
    response_path = path.parent / "response.json"
    response = json.loads(response_path.read_text())
    response["receivedMessages"][index] = received(new)
    response_path.write_text(json.dumps(response))


def relay_line(run_dir, sequence, attempt, *, phase="initial", restored=False, index=0):
    """A well-formed relay output for one input, as an attempt would publish it."""
    published = json.loads(
        next(
            run_dir.glob(f"pubsub/messages/*/input/{index}/*/response.json")
        ).read_text()
    )
    payload = "|".join(
        [
            "v1",
            run_dir.name,
            str(index),
            str(sequence),
            encode(published["messageIds"][0].encode()),
            attempt,
            "a0000000-0000-4000-8000-00000000000f",
            phase,
            "true" if restored else "false",
        ]
    )
    return encode(b"injected") + "\t" + encode(payload.encode())


def lines_before(run_dir, path):
    return sum(
        json.loads(p.read_text())["count"] for p in all_batches(run_dir) if p < path
    )


def all_batches(run_dir):
    return sorted(run_dir.glob("pubsub/messages/*/output/*/observations.json"))


def batches(run_dir):
    """The exported output batches that collected something, in order."""
    return [
        path
        for path in sorted(run_dir.glob("pubsub/messages/*/output/*/observations.json"))
        if json.loads(path.read_text())["count"]
    ]


def test_each_trial_s_exported_evidence_recomputes_to_its_verdict(exported):
    run_dir, control = exported
    assessed = assess(run_dir)
    assert assessed["problems"] == []
    assert assessed["status"] == control.recovery["verdict"] == USABLE
    assert assessed["reasons"] == control.recovery["reasons"]
    # The oracle rebuilt from every exported batch is the one the Pod held.
    assert assessed["oracle"] == control.recovery["oracle"]
    assert assessed["oracle"]["logical_inputs"] == 2 * RECORDS
    assert assessed["extra_publications"] == 0
    report = analyze.analyze(run_dir.parent)
    assert [item["status"] for item in report["pubsub"]] == [USABLE]
    assert "## Deployed Pub/Sub runs" in analyze.render_markdown(report)
    assert "pubsub runs: usable 1" in analyze.summary_line(report, run_dir)


@pytest.mark.parametrize(
    "change, reason",
    [
        (lambda r: r["requests"].update(exhausted=True), "request-budget-exhausted"),
        (lambda r: r["requests"].update(incomplete=True), "request-meter-incomplete"),
        (lambda r: r.pop("requests"), "request-meter-missing"),
        (lambda r: r.update(requests={"exhausted": False}), "request-meter-malformed"),
        (
            lambda r: r["requests"].update(reserved=r["requests"]["reserved"] - 1),
            "request-meter-malformed",
        ),
    ],
)
def test_a_run_past_its_request_ceiling_recomputes_inconclusive(
    exported, change, reason
):
    run_dir, _ = exported

    def withheld(result):
        change(result)
        result["success"] = False

    edit_result(run_dir, withheld)
    assessed = assess(run_dir)
    assert (assessed["status"], assessed["problems"]) == (INCONCLUSIVE, [])
    assert assessed["reasons"] == [reason]
    # A receipt that claims success beside it overstates the run.
    edit_result(run_dir, lambda result: result.update(success=True))
    assert assess(run_dir)["problems"] == ["receipt-success-mismatch"]
    assert assess(run_dir)["status"] == TAMPERED


@pytest.mark.parametrize("trial", ["jm-replacement", "tm-replacement"], indirect=True)
def test_a_lost_boundary_recomputes_inconclusive(admitting, monkeypatch, tmp_path):
    world = World(admitting, monkeypatch, interval=15)
    control = world.run()
    assessed = assess(export(world, control, tmp_path))
    assert (assessed["status"], assessed["problems"]) == (INCONCLUSIVE, [])
    assert assessed["reasons"] == ["replay-unobserved"] == control.recovery["reasons"]


@one("jm-replacement")
def test_an_interrupted_run_is_inconclusive_not_an_evidence_problem(
    admitting, monkeypatch, tmp_path
):
    world = World(admitting, monkeypatch, redeliver=False)
    control = world.run()
    assert control.recovery["stage"] == "recovering"
    assessed = assess(export(world, control, tmp_path))
    assert (assessed["status"], assessed["problems"]) == (INCONCLUSIVE, [])
    assert "recovery-incomplete" in assessed["reasons"]


@pytest.fixture
def lost_boundary(admitting, monkeypatch, tmp_path):
    """An honest, completed, inconclusive run's export."""
    world = World(admitting, monkeypatch, interval=15)
    control = world.run()
    return export(world, control, tmp_path / "evidence")


@one("rescale-out")
def test_a_rescale_hiding_its_redelivery_is_tampered(admitting, monkeypatch, tmp_path):
    world = World(admitting, monkeypatch, savepoint_acks=False)
    control = world.run()
    run_dir = export(world, control, tmp_path)
    assessed = assess(run_dir)
    assert (assessed["status"], assessed["problems"]) == (INCONCLUSIVE, [])
    # Only the count changes: the verdict and its reasons stay what they were,
    # so the one problem is the count its verdict reads.
    edit_record(
        run_dir, lambda r: r["outcomes"]["after"].update(replay_by_new_attempts=0)
    )
    assessed = assess(run_dir)
    assert (assessed["status"], assessed["problems"]) == (
        TAMPERED,
        ["replay-understated"],
    )
    assert assessed["reasons"] == ["replay-after-savepoint"]


@one("jm-replacement")
def test_a_replacement_s_count_differing_only_disagrees(exported):
    """A replacement's verdict does not read the count, so no claim is gained."""
    run_dir, _ = exported
    edit_record(
        run_dir, lambda r: r["outcomes"]["after"].update(replay_by_new_attempts=0)
    )
    assessed = assess(run_dir)
    assert (assessed["status"], assessed["problems"]) == (
        INCONSISTENT,
        ["replay-disagrees-with-its-evidence"],
    )


@one("rescale-in")
@pytest.mark.parametrize(
    "line",
    [
        # A new attempt that restored nothing, and a restored one of the
        # initial phase: neither is the upgraded job's.
        {"phase": "initial", "restored": False},
        {"phase": "initial", "restored": True},
    ],
)
def test_only_the_rescale_s_upgraded_attempts_count_as_replay(exported, line):
    run_dir, _ = exported
    start = cohort_ranges(RECORDS)[COHORTS[1]]["start"]
    append_line(
        all_batches(run_dir)[-1],
        relay_line(run_dir, start, "b0000000-0000-4000-8000-000000000001", **line),
    )
    edit_record(
        run_dir, lambda r: r["observed"].update(foreign=r["observed"]["foreign"] + 1)
    )
    problems = assess(run_dir)["problems"]
    assert not [p for p in problems if p.startswith("replay-")], problems


@one("jm-replacement")
def test_a_missing_output_batch_is_unexported(exported):
    run_dir, _ = exported
    # A gap in the batch counter, and fewer lines than the record collected.
    shutil.rmtree(batches(run_dir)[0].parent)
    assessed = assess(run_dir)
    assert assessed["status"] == UNEXPORTED and assessed["verdict"] is None
    assert assessed["problems"] and all(
        p.startswith("missing-output-batches:") for p in assessed["problems"]
    )


@one("jm-replacement")
def test_a_lost_last_batch_is_found_by_the_record_s_count(exported):
    run_dir, _ = exported
    path = batches(run_dir)[-1]
    lost = json.loads(path.read_text())["count"]
    shutil.rmtree(path.parent)
    assessed = assess(run_dir)
    assert assessed["status"] == UNEXPORTED
    assert f"missing-output-batches:{lost}" in assessed["problems"]


@one("jm-replacement")
def test_a_partial_download_does_not_excuse_an_overstatement(exported):
    run_dir, _ = exported
    shutil.rmtree(batches(run_dir)[-1].parent)
    edit_record(
        run_dir,
        lambda r: r["coverage"].update(
            recovering={"attempts": 1, "observed": ["source"]}
        ),
    )
    assessed = assess(run_dir)
    assert assessed["status"] == TAMPERED
    assert "coverage-unsupported-by-its-readings" in assessed["problems"]


@one("jm-replacement")
@pytest.mark.parametrize("target", ["response", "interval"])
def test_a_missing_input_receipt_is_unexported(exported, target):
    run_dir, _ = exported
    receipt_path = next(run_dir.glob("pubsub/messages/*/input/0/0-*/response.json"))
    if target == "response":
        receipt_path.unlink()
    else:
        shutil.rmtree(receipt_path.parent)
    assessed = assess(run_dir)
    assert assessed["status"] == UNEXPORTED
    assert [p.split(":")[0] for p in assessed["problems"]] == ["missing-input-receipts"]


@one("jm-replacement")
def test_unreadable_documents_and_approvals_are_excluded(exported):
    run_dir, _ = exported
    assert assess(run_dir)["status"] == USABLE
    approval = json.loads((run_dir / "approval.json").read_text())
    approval["pubsub_trial"]["records_per_subscription"] = 10001
    (run_dir / "approval.json").write_text(json.dumps(approval))
    assert assess(run_dir)["problems"] == ["approval-without-pubsub-trial"]
    approval["pubsub_trial"]["records_per_subscription"] = RECORDS
    (run_dir / "approval.json").write_text(json.dumps(approval))
    path = next(
        p
        for p in (run_dir / "supervisor").glob("*.json")
        if json.loads(p.read_text())["event"] == "pubsub-measurement"
    )
    document = json.loads(path.read_text())
    path.write_text(json.dumps({**document, "payload": "lost"}))
    assert assess(run_dir)["problems"] == ["malformed-evidence:1"]
    path.write_text(json.dumps(document))
    receipt_path = next(run_dir.glob("pubsub/messages/*/input/*/*/response.json"))
    receipt_path.write_text(json.dumps({"messageIds": ["only-one"]}))
    assert assess(run_dir)["problems"] == ["malformed-message-evidence:1"]
    receipt_path.unlink()
    batches(run_dir)[0].write_text("{not json")
    assert assess(run_dir)["problems"] == ["malformed-message-evidence:1"]
    (run_dir / "result.json").unlink()
    assert assess(run_dir)["problems"] == ["missing-result.json"]


@one("jm-replacement")
def test_a_missing_completion_record_is_unexported(exported):
    run_dir, _ = exported
    record_file(run_dir).unlink()
    assessed = assess(run_dir)
    assert assessed["status"] == UNEXPORTED
    assert assessed["problems"] == ["missing-recovery-complete"]
    # An evidence set without its own account publishes no verdict.
    assert assessed["verdict"] is None


@one("jm-replacement")
def test_an_output_of_an_unpublished_input_id_is_inconsistent(exported):
    run_dir, _ = exported
    # An input of the first cohort, processed before any fault, renamed to
    # the message ID the other subscription's input of the same sequence was
    # published under: published, but not for this input.
    path = batches(run_dir)[0]
    document = json.loads(path.read_text())
    line = next(
        line
        for line in document["tsv"].splitlines()
        if (item := parse(line, run_dir.name, RECORDS)) and item["input_index"] == 0
    )
    item = parse(line, run_dir.name, RECORDS)
    other = json.loads(
        next(run_dir.glob("pubsub/messages/*/input/1/0-*/response.json")).read_text()
    )["messageIds"][item["sequence"]]
    message_id, payload = line.split("\t")
    fields = base64.urlsafe_b64decode(payload + "==").decode().split("|")
    fields[4] = encode(other.encode())
    replace_line(path, line, message_id + "\t" + encode("|".join(fields).encode()))
    assessed = assess(run_dir)
    assert assessed["status"] == INCONSISTENT, assessed["problems"]
    assert assessed["problems"] == ["unpublished-input-ids:1"]


@one("jm-replacement")
def test_extra_publications_are_counted_apart_from_the_verdict(exported):
    run_dir, _ = exported
    interval = next(run_dir.glob("pubsub/messages/*/input/0/0-*"))
    extra = interval.parent / "0-1"
    extra.mkdir()
    (extra / "intent.json").write_text((interval / "intent.json").read_text())
    (extra / "response.json").write_text(json.dumps({"messageIds": ["again"]}))
    assessed = assess(run_dir)
    assert (assessed["status"], assessed["problems"]) == (USABLE, [])
    assert assessed["extra_publications"] == 1


@one("jm-replacement")
def test_a_record_hiding_a_refusal_its_batches_show_is_tampered(exported):
    run_dir, _ = exported
    path = batches(run_dir)[-1]
    append_line(path, "Zm9yZWlnbg\tbm90IHRoZSByZWxheSdz")
    position = lines_before(run_dir, path) + json.loads(path.read_text())["count"]
    edit_record(
        run_dir,
        lambda r: (
            r["observed"].update(foreign=r["observed"]["foreign"] + 1),
            r.update(verdict=INCONCLUSIVE, reasons=["oracle-rejected"]),
        ),
    )
    edit_result(run_dir, lambda r: r.update(success=False))
    assessed = assess(run_dir)
    assert (assessed["status"], assessed["problems"]) == (
        TAMPERED,
        ["oracle-overstates-its-evidence"],
    )
    assert assessed["reasons"] == ["oracle-rejected"]
    # Batches are read in collection order: the refused line is the last.
    assert assessed["oracle"]["rejected_line"] == position


@pytest.mark.parametrize(
    "change,expected",
    [
        (
            lambda r: r["oracle"].update(
                repeated_input_processing=r["oracle"]["repeated_input_processing"] + 1
            ),
            (INCONSISTENT, ["oracle-disagrees-with-its-evidence"]),
        ),
        (
            lambda r: r["coverage"].update(
                recovering={"attempts": 1, "observed": ["source"]}
            ),
            (TAMPERED, ["coverage-unsupported-by-its-readings"]),
        ),
        (
            lambda r: r["coverage"]["after"].update(
                observed=[*r["coverage"]["after"]["observed"], "memory"]
            ),
            (TAMPERED, ["coverage-unsupported-by-its-readings"]),
        ),
        (
            lambda r: r["coverage"]["before"].update(
                attempts=r["coverage"]["before"]["attempts"] - 1
            ),
            (INCONSISTENT, ["readings-the-record-does-not-account-for"]),
        ),
        (
            lambda r: r["observed"].update(
                observations=r["observed"]["observations"] - 1
            ),
            (INCONSISTENT, ["output-the-record-does-not-account-for:1"]),
        ),
        (lambda r: r.update(observed=[1]), (TAMPERED, ["observed-unreadable"])),
        (
            lambda r: r["outcomes"]["checkpoint"].update(id=-1),
            (TAMPERED, ["outcomes-differ-from-earlier-records"]),
        ),
    ],
    ids=[
        "oracle-counter",
        "coverage-window",
        "coverage-family",
        "readings-unaccounted",
        "output-unaccounted",
        "observed-unreadable",
        "outcome-edited-once",
    ],
)
@one("jm-replacement")
def test_each_record_edit_is_named_alone(exported, change, expected):
    run_dir, _ = exported
    edit_record(run_dir, change)
    assessed = assess(run_dir)
    assert (assessed["status"], assessed["problems"]) == expected


@one("tm-replacement")
@pytest.mark.parametrize(
    "name,change,problem",
    [
        ("fault", lambda record: lambda f: f.update(before=None), "fault-unreadable"),
        (
            "fault",
            lambda record: lambda f: f.update(kind="rescale-out"),
            "fault-kind-differs-from-the-approval",
        ),
        (
            "fault",
            lambda record: (
                lambda f: f.update(
                    before=[*f["before"], *record["outcomes"]["recovery"]["attempts"]]
                )
            ),
            "fault-names-a-later-attempt",
        ),
        (
            "recovery",
            lambda record: (
                lambda r: r.update(
                    expected_replay=r["expected_replay"] + 1,
                    replayed=r["replayed"] + 1,
                )
            ),
            "replay-overstated",
        ),
    ],
    ids=["unreadable", "kind", "later-attempt", "replay-count"],
)
def test_an_outcome_edited_in_every_record_is_still_caught(
    exported, name, change, problem
):
    """Edited consistently, so no earlier record contradicts it."""
    run_dir, control = exported
    edit_everywhere(run_dir, name, change(control.recovery))
    assessed = assess(run_dir)
    assert assessed["status"] == TAMPERED, assessed["problems"]
    assert problem in assessed["problems"]


@one("jm-replacement")
def test_a_stored_verdict_its_inputs_do_not_support_is_tampered(lost_boundary):
    edit_record(lost_boundary, lambda r: r.update(verdict=USABLE, reasons=[]))
    edit_result(lost_boundary, lambda r: r.update(success=False))
    assessed = assess(lost_boundary)
    assert (assessed["status"], assessed["problems"]) == (
        TAMPERED,
        ["verdict-unsupported-by-its-inputs"],
    )


@one("jm-replacement")
def test_an_under_reported_reason_is_inconsistent(lost_boundary):
    edit_record(lost_boundary, lambda r: r.update(reasons=["something-else"]))
    assessed = assess(lost_boundary)
    assert (assessed["status"], assessed["problems"]) == (
        INCONSISTENT,
        ["verdict-disagrees-with-its-evidence"],
    )


@pytest.mark.parametrize(
    "change,expected",
    [
        (lambda r: r.update(success=True), (TAMPERED, ["receipt-success-mismatch"])),
        (
            lambda r: r.update(scenario="smoke"),
            (INCONSISTENT, ["receipt-scenario-mismatch"]),
        ),
        (
            lambda r: r["recovery"].update(at="2000-01-01T00:00:00Z"),
            (INCONSISTENT, ["receipt-record-mismatch"]),
        ),
    ],
    ids=["success", "scenario", "record"],
)
@one("jm-replacement")
def test_each_receipt_disagreement_is_named_alone(lost_boundary, change, expected):
    edit_result(lost_boundary, change)
    assessed = assess(lost_boundary)
    assert (assessed["status"], assessed["problems"]) == expected


@one("jm-replacement")
def test_a_repeated_completion_record_is_inconsistent(lost_boundary):
    path = record_file(lost_boundary)
    (path.parent / ("z" + path.name)).write_text(path.read_text())
    assessed = assess(lost_boundary)
    assert (assessed["status"], assessed["problems"]) == (
        INCONSISTENT,
        ["repeated-recovery-complete:2"],
    )


def test_the_section_states_what_the_verdict_does_not_establish():
    rendered = pubsub_analyze.render(
        [
            {
                "run_id": "ps-1",
                "status": USABLE,
                "verdict": USABLE,
                "reasons": [],
                "problems": [],
                "samples": 4,
                "oracle": {
                    "rejected": None,
                    "logical_inputs": 60,
                    "input_publication_duplicates": 0,
                    "repeated_input_processing": 10,
                    "output_publication_duplicates": 0,
                    "repeated_output_delivery": 1,
                },
                "extra_publications": 0,
            },
            {
                "run_id": "ps-2",
                "status": INCONCLUSIVE,
                "verdict": INCONCLUSIVE,
                "reasons": ["oracle-rejected"],
                "problems": [],
                "samples": 4,
                "oracle": {"rejected": "Invalid output observation", "lines": 3},
                "extra_publications": 0,
            },
        ]
    )
    assert (
        "| ps-1 | usable | usable | 4 | 60 | 0 / 10 / 0 / 1 | 0 | - | - |" in rendered
    )
    assert "| ps-2 | inconclusive | inconclusive | 4 | - | - | 0 |" in rendered
    assert "not ordering across the replay and not exactly-once output" in rendered
    assert "decide nothing" in rendered
    assert "read from the record" in rendered
    assert pubsub_analyze.render([]) == ""


@one("tm-replacement")
def test_a_replacement_s_unrestored_new_attempt_is_not_replay(exported):
    """On the input the fault did not displace, which no new attempt replayed."""
    run_dir, control = exported
    start = cohort_ranges(RECORDS)[COHORTS[1]]["start"]
    before = control.recovery["outcomes"]["fault"]["before"]
    seen = [
        item
        for path in all_batches(run_dir)
        for line in json.loads(path.read_text())["tsv"].splitlines()
        if (item := parse(line, run_dir.name, RECORDS))
    ]
    index = next(
        i
        for i in (0, 1)
        if not any(
            item["input_index"] == i
            and item["sequence"] == start
            and item["attempt"] not in before
            for item in seen
        )
    )
    append_line(
        all_batches(run_dir)[-1],
        relay_line(run_dir, start, "b0000000-0000-4000-8000-000000000002", index=index),
    )
    edit_record(
        run_dir, lambda r: r["observed"].update(foreign=r["observed"]["foreign"] + 1)
    )
    problems = assess(run_dir)["problems"]
    assert not [p for p in problems if p.startswith("replay-")], problems


@one("jm-replacement")
def test_a_lost_empty_batch_is_found_by_its_counter(exported):
    """An empty pull lost from the middle changes no count; its number shows it."""
    run_dir, _ = exported
    paths = all_batches(run_dir)
    lost = next(
        path for path in paths[:-1] if json.loads(path.read_text())["count"] == 0
    )
    shutil.rmtree(lost.parent)
    assessed = assess(run_dir)
    assert (assessed["status"], assessed["problems"]) == (
        UNEXPORTED,
        ["missing-output-batches:1"],
    )


@one("jm-replacement")
def test_a_batch_its_pull_response_does_not_match_is_tampered(exported):
    """The oracle reads what the service answered, not the file derived from it."""
    run_dir, _ = exported
    response_path = batches(run_dir)[0].parent / "response.json"
    response = json.loads(response_path.read_text())
    response["receivedMessages"] = response["receivedMessages"][1:]
    response_path.write_text(json.dumps(response))
    assessed = assess(run_dir)
    assert assessed["status"] == TAMPERED
    assert "observations-differ-from-pull-response" in assessed["problems"]


@one("rescale-out")
def test_an_observations_file_hiding_a_rescale_s_replay_is_tampered(
    admitting, monkeypatch, tmp_path
):
    """Upgraded attempts' lines rewritten as initial ones, the response untouched."""
    world = World(admitting, monkeypatch, savepoint_acks=False)
    control = world.run()
    run_dir = export(world, control, tmp_path)
    for path in all_batches(run_dir):
        document = json.loads(path.read_text())
        rewritten = []
        for line in document["tsv"].splitlines():
            message_id, payload = line.split("\t")
            text = base64.urlsafe_b64decode(payload + "==").decode()
            text = text.replace("|upgrade|", "|initial|")
            rewritten.append(message_id + "\t" + encode(text.encode()))
        document["tsv"] = "".join(line + "\n" for line in rewritten)
        path.write_text(json.dumps(document))
    edit_record(
        run_dir,
        lambda r: (
            r["outcomes"]["after"].update(replay_by_new_attempts=0),
            r.update(verdict=USABLE, reasons=[]),
        ),
    )
    edit_result(run_dir, lambda r: r.update(success=True))
    assessed = assess(run_dir)
    assert assessed["status"] == TAMPERED
    assert "observations-differ-from-pull-response" in assessed["problems"]
    assert assessed["reasons"] == ["replay-after-savepoint"]


@one("jm-replacement")
def test_a_replay_under_an_id_no_pre_fault_attempt_processed_is_caught(exported):
    """A republished copy replayed under its new ID, the record still preserving."""
    run_dir, control = exported
    fault = control.recovery["outcomes"]["fault"]
    (index, sequence) = fault["expected_replay"][0]
    path, line = next(
        (path, line)
        for path in all_batches(run_dir)
        for line in json.loads(path.read_text())["tsv"].splitlines()
        if (item := parse(line, run_dir.name, RECORDS))
        and (item["input_index"], item["sequence"]) == (index, sequence)
        and item["attempt"] not in fault["before"]
    )
    # The republication is published, so nothing else about it is wrong.
    interval = next(run_dir.glob(f"pubsub/messages/*/input/{index}/*"))
    extra = interval.parent / f"{sequence}-1"
    extra.mkdir()
    (extra / "intent.json").write_text((interval / "intent.json").read_text())
    (extra / "response.json").write_text(json.dumps({"messageIds": ["republished"]}))
    message_id, payload = line.split("\t")
    fields = base64.urlsafe_b64decode(payload + "==").decode().split("|")
    fields[4] = encode(b"republished")
    replace_line(path, line, message_id + "\t" + encode("|".join(fields).encode()))
    assessed = assess(run_dir)
    assert assessed["status"] == TAMPERED, assessed["problems"]
    assert "replay-ids-overstated" in assessed["problems"]


@one("tm-replacement")
@pytest.mark.parametrize("expected", [1, [[[], 0]], [[0, "1"]], [[0]]])
def test_an_unreadable_expected_replay_is_named_not_raised(exported, expected):
    run_dir, _ = exported
    edit_everywhere(run_dir, "fault", lambda f: f.update(expected_replay=expected))
    assessed = assess(run_dir)
    assert assessed["status"] == TAMPERED
    assert "fault-unreadable" in assessed["problems"]


@one("jm-replacement")
def test_a_partial_download_still_refuses_a_verdict_its_record_cannot_carry(
    admitting, monkeypatch, tmp_path
):
    """A lost boundary forged usable, with an empty batch lost from the middle."""
    world = World(admitting, monkeypatch, interval=15)
    control = world.run()
    run_dir = export(world, control, tmp_path)
    lost = next(
        path
        for path in all_batches(run_dir)[:-1]
        if json.loads(path.read_text())["count"] == 0
    )
    shutil.rmtree(lost.parent)
    edit_record(run_dir, lambda r: r.update(verdict=USABLE, reasons=[]))
    edit_result(run_dir, lambda r: r.update(success=True))
    assessed = assess(run_dir)
    assert assessed["status"] == TAMPERED
    assert {
        "missing-output-batches:1",
        "receipt-success-mismatch",
        "verdict-unsupported-by-its-inputs",
    } == set(assessed["problems"])


@one("rescale-out")
def test_the_verdict_is_decided_over_the_readings_not_the_record_s_coverage(
    admitting, monkeypatch, tmp_path
):
    """A run that never read its connector metrics, claimed to have read them."""
    world = World(admitting, monkeypatch)
    world.relay.listed = ()
    control = world.run()
    run_dir = export(world, control, tmp_path)
    honest = assess(run_dir)
    assert (honest["status"], honest["problems"]) == (INCONCLUSIVE, [])

    def claim(record):
        for window in record["coverage"].values():
            window["observed"] = sorted({*window["observed"], "sink", "source"})
        record.update(verdict=USABLE, reasons=[])

    edit_record(run_dir, claim)
    edit_result(run_dir, lambda r: r.update(success=False))
    assessed = assess(run_dir)
    assert assessed["status"] == TAMPERED
    assert assessed["reasons"] == honest["reasons"]
    assert assessed["problems"] == [
        "coverage-unsupported-by-its-readings",
        "verdict-unsupported-by-its-inputs",
    ]


@one("jm-replacement")
def test_a_late_republished_replay_honestly_recorded_is_not_accused(
    admitting, monkeypatch, tmp_path
):
    """Preserved at recovery, then a republished copy replayed: the record says so."""
    world = World(admitting, monkeypatch)
    relay = world.relay
    start = cohort_ranges(RECORDS)[COHORTS[1]]["start"]

    def republish():
        if stage(world) == "after" and "late" not in relay.events:
            relay.events.append("late")
            relay.pending[0].append(
                {
                    "id": "late-copy",
                    "payload": f"v1|{relay.run_id}|0|{start}",
                    "index": 0,
                }
            )

    relay.hooks.append(republish)
    control = world.run()
    outcomes = control.recovery["outcomes"]
    assert outcomes["recovery"]["replay_ids_preserved"] is True
    assert outcomes["after"]["replay_ids_preserved"] is False
    run_dir = export(world, control, tmp_path)
    # The copy's ID was never published as the receipts know it, which is the
    # one thing the evidence holds against the run; the replay claims stand.
    assessed = assess(run_dir)
    assert "replay-ids-overstated" not in assessed["problems"], assessed["problems"]
    assert assessed["reasons"] == ["replay-ids-not-preserved"]
    # A completion claiming preservation over the same evidence is refused.
    edit_record(
        run_dir,
        lambda r: r["outcomes"]["after"].update(replay_ids_preserved=True),
    )
    assert "replay-ids-overstated" in assess(run_dir)["problems"]
