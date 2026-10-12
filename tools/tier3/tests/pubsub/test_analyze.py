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
from flink_tier3.pubsub.messages import COHORTS, cohort_ranges
from flink_tier3.pubsub.output import parse

from ..test_lifecycle import env as env  # noqa: PLC0414
from ..test_lifecycle import rt
from .test_actors import approval as approval  # noqa: PLC0414
from .test_actors import prepared as prepared  # noqa: PLC0414
from .test_admission import admitting as admitting  # noqa: PLC0414
from .test_exercise import RECORDS, World, receipt
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
    """A well-formed relay output for one input, as an attempt would publish it.

    The input message ID is the one its publication receipt holds for that
    sequence, found by the interval its path names rather than by glob order,
    which differs between file systems.
    """
    for path in run_dir.glob(f"pubsub/messages/*/input/{index}/*/response.json"):
        start, count = (int(part) for part in path.parent.name.split("-"))
        if start <= sequence < start + count:
            message_id = json.loads(path.read_text())["messageIds"][sequence - start]
            break
    else:
        raise AssertionError(f"no publication receipt holds input {index}/{sequence}")
    payload = "|".join(
        [
            "v1",
            run_dir.name,
            str(index),
            str(sequence),
            encode(message_id.encode()),
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


DIFFERS = "verdict-differs-from-the-record"


def test_each_trial_s_exported_evidence_recomputes_to_its_verdict(exported):
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


@pytest.mark.parametrize("trial", ["jm-replacement", "tm-replacement"], indirect=True)
def test_a_lost_boundary_recomputes_inconclusive(admitting, monkeypatch, tmp_path):
    world = World(admitting, monkeypatch, interval=15)
    control = world.run()
    assessed = assess(export(world, control, tmp_path))
    assert (assessed["verdict"], assessed["problems"]) == (INCONCLUSIVE, [])
    assert assessed["reasons"] == ["replay-unobserved"] == control.recovery["reasons"]


@one("jm-replacement")
def test_an_interrupted_run_is_inconclusive_not_an_evidence_problem(
    admitting, monkeypatch, tmp_path
):
    world = World(admitting, monkeypatch, redeliver=False)
    control = world.run()
    assert control.recovery["stage"] == "recovering"
    assessed = assess(export(world, control, tmp_path))
    assert (assessed["verdict"], assessed["problems"]) == (INCONCLUSIVE, [])
    assert "recovery-incomplete" in assessed["reasons"]


@one("jm-replacement")
def test_an_interrupted_run_s_lost_publication_response_is_not_unpublished(
    admitting, monkeypatch, tmp_path
):
    """An output of an input whose publish response was lost is an unknown outcome."""
    world = World(admitting, monkeypatch, redeliver=False)
    control = world.run()
    run_dir = export(world, control, tmp_path)
    honest = assess(run_dir)
    next(run_dir.glob("pubsub/messages/*/input/0/0-*/response.json")).unlink()
    assessed = assess(run_dir)
    assert (assessed["verdict"], assessed["problems"]) == (INCONCLUSIVE, [])
    assert assessed["reasons"] == honest["reasons"]


@pytest.fixture
def lost_boundary(admitting, monkeypatch, tmp_path):
    """An honest, completed, inconclusive run's export."""
    world = World(admitting, monkeypatch, interval=15)
    control = world.run()
    return export(world, control, tmp_path / "evidence")


@one("rescale-out")
def test_a_rescale_s_replay_count_is_rebuilt_from_its_observations(
    admitting, monkeypatch, tmp_path
):
    world = World(admitting, monkeypatch, savepoint_acks=False)
    control = world.run()
    run_dir = export(world, control, tmp_path)
    honest = assess(run_dir)
    assert (honest["verdict"], honest["problems"]) == (INCONCLUSIVE, [])
    assert honest["reasons"] == ["replay-after-savepoint"]
    # The record's count is not what the verdict reads, so a record claiming
    # no redelivery changes nothing the analysis decides.
    edit_record(
        run_dir, lambda r: r["outcomes"]["after"].update(replay_by_new_attempts=0)
    )
    assert assess(run_dir) == honest


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
    assessed = assess(run_dir)
    assert (assessed["verdict"], assessed["problems"]) == (USABLE, [])


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
    assessed = assess(run_dir)
    assert (assessed["verdict"], assessed["problems"]) == (USABLE, [])
    # A replacement's verdict does not read the rebuilt count, so ask it directly.
    lines = pubsub_analyze._collected(next(run_dir.glob("pubsub/messages/*")))[0]
    outcomes = control.recovery["outcomes"]
    rebuilt = pubsub_analyze._replayed(
        outcomes, pubsub_analyze._observations(lines, run_dir.name, RECORDS), RECORDS
    )
    assert (
        rebuilt["after"]["replay_by_new_attempts"]
        == outcomes["after"]["replay_by_new_attempts"]
    )


@one("jm-replacement")
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


@one("jm-replacement")
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


@one("jm-replacement")
def test_a_lost_measurement_sample_leaves_no_verdict(exported):
    run_dir, _ = exported
    # A sample taken while recovering counts in no window, so lose one that does.
    next(
        p
        for p in sorted((run_dir / "supervisor").glob("*.json"))
        if (document := json.loads(p.read_text()))["event"] == "pubsub-measurement"
        and document["payload"]["stage"] == "after"
    ).unlink()
    assessed = assess(run_dir)
    assert (assessed["verdict"], assessed["problems"]) == (None, ["missing-samples:1"])


@one("jm-replacement")
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


@one("jm-replacement")
def test_an_export_holding_more_lines_than_the_record_counted_is_read(exported):
    run_dir, _ = exported
    edit_record(
        run_dir,
        lambda r: r["observed"].update(observations=r["observed"]["observations"] - 1),
    )
    assessed = assess(run_dir)
    assert (assessed["verdict"], assessed["problems"]) == (USABLE, [])


@one("jm-replacement")
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
    assert (assessed["verdict"], assessed["problems"]) == (
        None,
        ["missing-output-batches:1"],
    )


@one("jm-replacement")
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


@one("jm-replacement")
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
    path = next(
        p
        for p in (run_dir / "supervisor").glob("*.json")
        if json.loads(p.read_text())["event"] == "pubsub-measurement"
    )
    document = json.loads(path.read_text())
    path.write_text(json.dumps({**document, "payload": "lost"}))
    assessed = assess(run_dir)
    assert (assessed["stored"], assessed["problems"]) == (
        USABLE,
        ["malformed-evidence:1"],
    )
    path.write_text(json.dumps(document))
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


@one("jm-replacement")
def test_a_missing_completion_record_leaves_no_verdict(exported):
    run_dir, _ = exported
    record_file(run_dir).unlink()
    assessed = assess(run_dir)
    assert assessed["problems"] == ["missing-recovery-complete"]
    # An evidence set without its own account publishes no verdict; the
    # receipt's copy of the record still says what the run stored.
    assert (assessed["verdict"], assessed["stored"]) == (None, USABLE)


@one("jm-replacement")
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
    assert (assessed["verdict"], assessed["problems"]) == (USABLE, [])
    assert assessed["extra_publications"] == 1


@one("jm-replacement")
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


@one("rescale-out")
def test_the_analysis_reads_the_pull_responses_not_the_observations_files(
    admitting, monkeypatch, tmp_path
):
    """Upgraded attempts' lines rewritten as initial ones, the responses untouched."""
    world = World(admitting, monkeypatch, savepoint_acks=False)
    control = world.run()
    run_dir = export(world, control, tmp_path)
    honest = assess(run_dir)
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
    assert assess(run_dir) == honest


@one("jm-replacement")
def test_a_stored_verdict_its_evidence_does_not_recompute_to_differs(lost_boundary):
    edit_record(lost_boundary, lambda r: r.update(verdict=USABLE, reasons=[]))
    assessed = assess(lost_boundary)
    assert (assessed["verdict"], assessed["stored"], assessed["problems"]) == (
        INCONCLUSIVE,
        USABLE,
        [DIFFERS],
    )


@one("jm-replacement")
def test_stored_reasons_its_evidence_does_not_recompute_to_differ(lost_boundary):
    edit_record(lost_boundary, lambda r: r.update(reasons=["something-else"]))
    assessed = assess(lost_boundary)
    assert (assessed["verdict"], assessed["problems"]) == (INCONCLUSIVE, [DIFFERS])


@one("jm-replacement")
@pytest.mark.parametrize(
    "change",
    [
        lambda r: r.update(success=True),
        lambda r: r.update(scenario="smoke"),
        lambda r: r["recovery"].update(at="2000-01-01T00:00:00Z"),
    ],
    ids=["success", "scenario", "record"],
)
def test_the_receipt_s_own_claims_decide_nothing(lost_boundary, change):
    honest = assess(lost_boundary)
    edit_result(lost_boundary, change)
    assert assess(lost_boundary) == honest


@one("jm-replacement")
def test_the_later_of_two_completion_records_is_the_one_read(lost_boundary):
    path = record_file(lost_boundary)
    (path.parent / ("z" + path.name)).write_text(path.read_text())
    assessed = assess(lost_boundary)
    assert (assessed["verdict"], assessed["problems"]) == (INCONCLUSIVE, [])


@one("rescale-out")
def test_the_verdict_is_decided_over_the_readings_not_the_record_s_coverage(
    admitting, monkeypatch, tmp_path
):
    """A run that never read its connector metrics, recorded as having read them."""
    world = World(admitting, monkeypatch)
    world.relay.listed = ()
    control = world.run()
    run_dir = export(world, control, tmp_path)
    honest = assess(run_dir)
    assert (honest["verdict"], honest["problems"]) == (INCONCLUSIVE, [])

    def claim(record):
        for window in record["coverage"].values():
            window["observed"] = sorted({*window["observed"], "sink", "source"})
        record.update(verdict=USABLE, reasons=[])

    edit_record(run_dir, claim)
    assessed = assess(run_dir)
    assert assessed["verdict"] == INCONCLUSIVE
    assert assessed["reasons"] == honest["reasons"]
    assert assessed["problems"] == [DIFFERS]


def test_the_section_states_what_the_verdict_does_not_establish():
    rendered = pubsub_analyze.render(
        [
            {
                "run_id": "ps-1",
                "verdict": USABLE,
                "stored": USABLE,
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
                "verdict": INCONCLUSIVE,
                "stored": None,
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
    assert "| ps-2 | inconclusive | - | 4 | - | - | 0 |" in rendered
    assert "not ordering across the replay and not exactly-once output" in rendered
    assert "decide nothing" in rendered
    assert "read from the record" in rendered
    assert pubsub_analyze.render([]) == ""
