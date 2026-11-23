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
from flink_tier3.common import INCONCLUSIVE, USABLE
from flink_tier3.pubsub import analyze as pubsub_analyze
from flink_tier3.pubsub import plan
from flink_tier3.pubsub.output import parse
from flink_tier3.pubsub.verdict import COMPLETE_EVENT
from flink_tier3.recovery_analysis import (
    SINGLE_FAULT,
    SINGLE_FAULT_NOTE,
)

from ..test_lifecycle import env as env  # noqa: PLC0414
from ..test_lifecycle import rt
from .relay import World, receipt
from .test_actors import approval as approval  # noqa: PLC0414
from .test_actors import prepared as prepared  # noqa: PLC0414
from .test_admission import admitting as admitting  # noqa: PLC0414
from .test_plan import inputs as inputs  # noqa: PLC0414
from .test_plan import renderer as renderer  # noqa: PLC0414

# The kinds the single-fault trials named, the reactive and adaptive scheduler
# kinds among them, written out so that dropping one from the analyzer's list
# fails a test here rather than shrinking it.
ARCHIVED_KINDS = (
    "jm-replacement",
    "tm-replacement",
    "rescale-out",
    "rescale-in",
    "reactive-scale-out",
    "reactive-scale-in",
    "reactive-tm-eviction",
    "reactive-tm-kill",
    "adaptive-scale-in",
)
# The input per subscription, as small as the sequence's cohorts allow.
RECORDS = 60


@pytest.fixture
def trial():
    return {**plan.trial("sequence"), "records_per_subscription": RECORDS}


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
    """One completed sequence run's exported evidence, and the record it wrote."""
    world = World(admitting, monkeypatch, pace=1)
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


DIFFERS = "verdict-differs-from-the-record"


def test_the_exported_evidence_recomputes_to_its_verdict(exported):
    run_dir, control = exported
    assessed = assess(run_dir)
    assert assessed["problems"] == []
    assert assessed["verdict"] == assessed["stored"] == USABLE
    assert control.recovery["verdict"] == USABLE
    assert assessed["reasons"] == control.recovery["reasons"]
    # The oracle rebuilt from every exported batch is the one the Pod held.
    assert assessed["oracle"] == control.recovery["oracle"]
    assert assessed["oracle"]["logical_inputs"] == 2 * RECORDS
    assert assessed["extra_publications"] == 0
    report = analyze.analyze(run_dir.parent)
    assert [item["verdict"] for item in report["pubsub"]] == [USABLE]
    assert "## Deployed Pub/Sub runs" in analyze.render_markdown(report)
    assert "pubsub runs: usable 1" in analyze.summary_line(report, run_dir)


def test_a_missing_output_batch_leaves_no_verdict(exported):
    run_dir, _ = exported
    path = batches(run_dir)[0]
    lost = json.loads(path.read_text())["count"]
    # A gap in the batch counter, and fewer lines than the record collected.
    shutil.rmtree(path.parent)
    assessed = assess(run_dir)
    assert assessed["verdict"] is None and assessed["stored"] == USABLE
    assert assessed["problems"] == [
        "missing-output-batches:1",
        f"missing-output-lines:{lost}",
    ]


def test_a_lost_last_batch_is_found_by_the_record_s_count(exported):
    """A lost tail leaves no gap in a counter; only the record's count shows it."""
    run_dir, _ = exported
    path = batches(run_dir)[-1]
    lost = json.loads(path.read_text())["count"]
    for later in all_batches(run_dir):
        if later >= path:
            shutil.rmtree(later.parent)
    assessed = assess(run_dir)
    assert assessed["verdict"] is None
    assert assessed["problems"] == [f"missing-output-lines:{lost}"]


@pytest.mark.parametrize("count", ["observations", "foreign"])
def test_every_line_the_record_counted_must_be_exported(exported, count):
    run_dir, _ = exported
    edit_record(
        run_dir, lambda r: r["observed"].update({count: r["observed"][count] + 1})
    )
    assessed = assess(run_dir)
    assert (assessed["verdict"], assessed["problems"]) == (
        None,
        ["missing-output-lines:1"],
    )


def test_an_export_holding_more_lines_than_the_record_counted_is_read(exported):
    run_dir, _ = exported
    edit_record(
        run_dir,
        lambda r: r["observed"].update(observations=r["observed"]["observations"] - 1),
    )
    assessed = assess(run_dir)
    assert (assessed["verdict"], assessed["problems"]) == (USABLE, [])


def test_a_pull_response_without_its_observations_file_is_not_the_collector_s(
    exported,
):
    """The collector saves the response first; a batch is its own once both are."""
    run_dir, _ = exported
    honest = assess(run_dir)
    path = all_batches(run_dir)[-1]
    later = path.parent.parent / (path.parent.name[:-6] + "999999")
    later.mkdir()
    (later / "response.json").write_text(
        (batches(run_dir)[0].parent / "response.json").read_text()
    )
    assert assess(run_dir) == honest


def test_a_lost_empty_batch_is_found_by_its_counter(exported):
    """An empty pull lost from the middle changes no count; its number shows it."""
    run_dir, _ = exported
    paths = all_batches(run_dir)
    lost = next(
        path for path in paths[:-1] if json.loads(path.read_text())["count"] == 0
    )
    shutil.rmtree(lost.parent)
    assessed = assess(run_dir)
    assert (assessed["verdict"], assessed["problems"]) == (
        None,
        ["missing-output-batches:1"],
    )


@pytest.mark.parametrize("target", ["response", "interval"])
def test_a_missing_input_receipt_leaves_no_verdict(exported, target):
    run_dir, _ = exported
    receipt_path = next(run_dir.glob("pubsub/messages/*/input/0/0-*/response.json"))
    if target == "response":
        receipt_path.unlink()
    else:
        shutil.rmtree(receipt_path.parent)
    assessed = assess(run_dir)
    assert assessed["verdict"] is None
    assert [p.split(":")[0] for p in assessed["problems"]] == ["missing-input-receipts"]


@pytest.mark.parametrize("kind", ARCHIVED_KINDS)
def test_a_single_fault_run_has_no_verdict_here(exported, kind):
    """Its approval names a kind no trial runs any more; its record is still read."""
    run_dir, control = exported
    approval = json.loads((run_dir / "approval.json").read_text())
    approval["pubsub_trial"]["trial"] = kind
    (run_dir / "approval.json").write_text(json.dumps(approval))
    assessed = assess(run_dir)
    assert (assessed["verdict"], assessed["stored"], assessed["problems"]) == (
        None,
        control.recovery["verdict"],
        [SINGLE_FAULT],
    )
    rendered = pubsub_analyze.render([assessed])
    assert f"| {SINGLE_FAULT} |" in rendered
    assert SINGLE_FAULT_NOTE in rendered


@pytest.mark.parametrize("kind", [None, 3, [], "", "sequnce"])
def test_an_approval_whose_trial_names_no_known_kind_is_no_trial(exported, kind):
    """A kind no trial had is malformed, not an archived single fault."""
    run_dir, _ = exported
    approval = json.loads((run_dir / "approval.json").read_text())
    if kind is None:
        del approval["pubsub_trial"]["trial"]
    else:
        approval["pubsub_trial"]["trial"] = kind
    (run_dir / "approval.json").write_text(json.dumps(approval))
    assert assess(run_dir)["problems"] == ["approval-without-pubsub-trial"]


def test_unreadable_documents_and_approvals_leave_no_verdict(exported):
    run_dir, _ = exported
    assert assess(run_dir)["verdict"] == USABLE
    approval = json.loads((run_dir / "approval.json").read_text())
    approval["pubsub_trial"]["records_per_subscription"] = 10001
    (run_dir / "approval.json").write_text(json.dumps(approval))
    assert assess(run_dir)["problems"] == ["approval-without-pubsub-trial"]
    assert assess(run_dir)["stored"] is None
    approval["pubsub_trial"]["records_per_subscription"] = RECORDS
    (run_dir / "approval.json").write_text(json.dumps(approval))
    # A completed record nothing can read is a download to fetch again.
    path = run_dir / "supervisor" / "zz-complete.json"
    path.write_text(json.dumps({"event": COMPLETE_EVENT, "payload": "lost"}))
    assessed = assess(run_dir)
    assert (assessed["stored"], assessed["problems"]) == (
        USABLE,
        ["malformed-evidence:1"],
    )
    path.unlink()
    receipt_path = next(run_dir.glob("pubsub/messages/*/input/*/*/response.json"))
    receipt_path.write_text(json.dumps({"messageIds": ["only-one"]}))
    assessed = assess(run_dir)
    assert (assessed["stored"], assessed["problems"]) == (
        USABLE,
        ["malformed-message-evidence:1"],
    )
    receipt_path.unlink()
    (batches(run_dir)[0].parent / "response.json").write_text("{not json")
    assert assess(run_dir)["problems"] == ["malformed-message-evidence:1"]
    (run_dir / "result.json").unlink()
    assessed = assess(run_dir)
    assert (assessed["verdict"], assessed["problems"]) == (
        None,
        ["missing-result.json"],
    )


def test_a_missing_completion_record_leaves_no_verdict(exported):
    run_dir, _ = exported
    record_file(run_dir).unlink()
    assessed = assess(run_dir)
    assert assessed["problems"] == ["missing-recovery-complete"]
    # An evidence set without its own account publishes no verdict; the
    # receipt's copy of the record still says what the run stored.
    assert (assessed["verdict"], assessed["stored"]) == (None, USABLE)


def test_an_output_of_an_unpublished_input_id_is_named(exported):
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
    # The sequence's verdict checks every output's input ID against the
    # receipts, so the recomputed verdict, not a problem, names it.
    assert "input-ids-unpublished" in assessed["reasons"]
    assert assessed["problems"] == [DIFFERS]


def test_extra_publications_are_counted_apart_from_the_verdict(exported):
    run_dir, _ = exported
    interval = next(run_dir.glob("pubsub/messages/*/input/0/0-*"))
    extra = interval.parent / "0-1"
    extra.mkdir()
    (extra / "intent.json").write_text((interval / "intent.json").read_text())
    (extra / "response.json").write_text(json.dumps({"messageIds": ["again"]}))
    assessed = assess(run_dir)
    assert (assessed["verdict"], assessed["problems"]) == (USABLE, [])
    assert assessed["extra_publications"] == 1


def test_the_oracle_reads_every_batch_in_collection_order(exported):
    run_dir, _ = exported
    path = batches(run_dir)[-1]
    append_line(path, "Zm9yZWlnbg\tbm90IHRoZSByZWxheSdz")
    position = lines_before(run_dir, path) + json.loads(path.read_text())["count"]
    edit_record(
        run_dir,
        lambda r: r["observed"].update(foreign=r["observed"]["foreign"] + 1),
    )
    assessed = assess(run_dir)
    assert assessed["verdict"] == INCONCLUSIVE
    assert assessed["reasons"] == ["oracle-rejected"]
    assert assessed["problems"] == [DIFFERS]
    # Batches are read in collection order: the refused line is the last.
    assert assessed["oracle"]["rejected_line"] == position


def test_the_analysis_reads_the_pull_responses_not_the_observations_files(exported):
    """An input renamed to another published ID in the observations file only.

    Read from that file, the output would name an input this run did not
    publish under that ID, which the sequence's verdict refuses.
    """
    run_dir, _ = exported
    honest = assess(run_dir)
    path = batches(run_dir)[0]
    document = json.loads(path.read_text())
    lines = document["tsv"].splitlines()
    index, item = next(
        (i, item)
        for i, line in enumerate(lines)
        if (item := parse(line, run_dir.name, RECORDS)) and item["input_index"] == 0
    )
    other = json.loads(
        next(run_dir.glob("pubsub/messages/*/input/1/0-*/response.json")).read_text()
    )["messageIds"][item["sequence"]]
    message_id, payload = lines[index].split("\t")
    fields = base64.urlsafe_b64decode(payload + "==").decode().split("|")
    fields[4] = encode(other.encode())
    lines[index] = message_id + "\t" + encode("|".join(fields).encode())
    document["tsv"] = "".join(line + "\n" for line in lines)
    path.write_text(json.dumps(document))
    assert assess(run_dir) == honest


def test_a_stored_verdict_its_evidence_does_not_recompute_to_differs(exported):
    run_dir, _ = exported
    edit_record(run_dir, lambda r: r.update(verdict=INCONCLUSIVE, reasons=["x"]))
    assessed = assess(run_dir)
    assert (assessed["verdict"], assessed["stored"], assessed["problems"]) == (
        USABLE,
        INCONCLUSIVE,
        [DIFFERS],
    )


def test_stored_reasons_its_evidence_does_not_recompute_to_differ(exported):
    run_dir, _ = exported
    edit_record(run_dir, lambda r: r.update(reasons=["something-else"]))
    assessed = assess(run_dir)
    assert (assessed["verdict"], assessed["problems"]) == (USABLE, [DIFFERS])


@pytest.mark.parametrize(
    "change",
    [
        lambda r: r.update(success=True),
        lambda r: r.update(scenario="smoke"),
        lambda r: r["recovery"].update(at="2000-01-01T00:00:00Z"),
    ],
    ids=["success", "scenario", "record"],
)
def test_the_receipt_s_own_claims_decide_nothing(exported, change):
    run_dir, _ = exported
    honest = assess(run_dir)
    edit_result(run_dir, change)
    assert assess(run_dir) == honest


def test_the_later_of_two_completion_records_is_the_one_read(exported):
    run_dir, _ = exported
    path = record_file(run_dir)
    honest = path.read_text()
    (path.parent / ("z" + path.name)).write_text(honest)
    # The earlier copy disagrees; were it the one read, it would differ.
    earlier = json.loads(honest)
    earlier["payload"].update(verdict=INCONCLUSIVE, reasons=["something-else"])
    path.write_text(json.dumps(earlier))
    assessed = assess(run_dir)
    assert (assessed["verdict"], assessed["stored"], assessed["problems"]) == (
        USABLE,
        USABLE,
        [],
    )


def test_the_section_states_what_the_verdict_does_not_establish():
    rendered = pubsub_analyze.render(
        [
            {
                "run_id": "ps-1",
                "verdict": USABLE,
                "stored": USABLE,
                "reasons": [],
                "problems": [],
                "oracle": {
                    "rejected": None,
                    "logical_inputs": 60,
                    "input_publication_duplicates": 0,
                    "repeated_input_processing": 10,
                    "output_publication_duplicates": 0,
                    "repeated_output_delivery": 1,
                    "covered_redeliveries": 3,
                },
                "extra_publications": 0,
            },
            {
                "run_id": "ps-2",
                "verdict": INCONCLUSIVE,
                "stored": None,
                "reasons": ["oracle-rejected"],
                "problems": [],
                "oracle": {"rejected": "Invalid output observation", "lines": 3},
                "extra_publications": 0,
            },
        ]
    )
    assert (
        "| ps-1 | usable | usable | 60 | 0 / 10 / 0 / 1 | 3 | 0 | - | - | - |"
        in rendered
    )
    assert "| ps-2 | inconclusive | - | - | - | - | 0 |" in rendered
    assert "not ordering and not exactly-once output" in rendered
    assert "decide nothing" in rendered
    assert "read from the record" in rendered
    assert pubsub_analyze.render([]) == ""
