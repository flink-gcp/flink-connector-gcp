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
"""The campaign journal: admission, resume, budgets and what stops a campaign."""

import json
import os

import pytest
from flink_tier3 import campaign
from flink_tier3.bundle import delivered_sources
from flink_tier3.common import Failure

INPUTS = {"protocolSha256": "a" * 64, "classpathSha256": "b" * 64}
BUDGETS = {"bytes": 10_000, "seconds": 3_600}
OWNER = {"pid": 4242, "started": "2026-09-22T00:00:00Z"}
OTHER = {"pid": 4343, "started": "2026-09-22T00:05:00Z"}
USED = {"bytes": 400, "seconds": 120}


def runs(count=3, reserves=None):
    reserves = reserves or {"bytes": 1_000, "seconds": 300}
    return [
        {
            "order": order,
            "cellId": f"k{order:02d}",
            "arm": "STAGED_HASH",
            "reserves": dict(reserves),
        }
        for order in range(1, count + 1)
    ]


class Clock:
    def __init__(self):
        self.now = 1_790_035_200.0

    def __call__(self):
        return self.now


def journal(directory, *, clock=None, alive=None, inputs=None):
    return campaign.Journal(
        directory,
        inputs or INPUTS,
        clock=clock or Clock(),
        alive=alive if alive is not None else (lambda owner: True),
    )


def planned(directory, count=3, reserves=None, budgets=None):
    campaign.plan_campaign(
        directory, "cal1246d", runs(count, reserves), INPUTS, budgets or BUDGETS
    )
    return directory


def claimed(book, owner=OWNER):
    """Claims a run and says what the campaign thinks if there was none to give."""
    run = book.claim(owner)
    assert run is not None, f"expected a run; counts={book.counts()}"
    return run


def work(book, outcome=campaign.OBSERVED, owner=OWNER, consumed=None, detail=None):
    run = claimed(book, owner)
    book.finish(run["order"], owner, outcome, consumed or run["reserves"], detail)
    return run


def test_a_planned_campaign_hands_out_its_runs_in_order(tmp_path):
    planned(tmp_path)
    book = journal(tmp_path)

    assert [work(book)["order"] for _ in range(3)] == [1, 2, 3]
    assert book.claim(OWNER) is None


def test_a_claim_carries_everything_the_controller_needs_to_run_the_cell(tmp_path):
    # The claim is the campaign's entire output to the controller: an order
    # with no cell and no arm would not say what to execute.
    planned(tmp_path)

    assert claimed(journal(tmp_path)) == {
        "order": 1,
        "cellId": "k01",
        "arm": "STAGED_HASH",
        "reserves": {"bytes": 1_000, "seconds": 300},
    }


def test_a_claim_the_caller_edits_does_not_change_what_the_budget_settles(tmp_path):
    planned(tmp_path)
    book = journal(tmp_path)
    run = claimed(book)

    run["reserves"]["seconds"] = 0
    book.finish(
        run["order"], OWNER, campaign.OBSERVED, {"bytes": 1_000, "seconds": 300}
    )

    assert book.counts()["consumed"] == {"bytes": 1_000, "seconds": 300}


def test_a_spent_run_is_not_handed_out_again_after_a_resume(tmp_path):
    # The reason the journal is durable at all: a campaign stopped after run 2
    # must not re-run it, because its tasks were already created.
    planned(tmp_path)
    book = journal(tmp_path)
    work(book)
    work(book, campaign.FAILED)

    resumed = journal(tmp_path)

    assert claimed(resumed)["cellId"] == "k03"
    assert resumed.counts()["runs"] == {
        campaign.PENDING: 0,
        campaign.CLAIMED: 1,
        campaign.OBSERVED: 1,
        campaign.FAILED: 1,
    }


def test_a_failed_run_does_not_stop_the_campaign(tmp_path):
    # ADR-0166's 2026-09-19 refinement: stopping on the first failure left most
    # cells unmeasured after one checkpoint timeout.
    planned(tmp_path)
    book = journal(tmp_path)

    work(book, campaign.FAILED, detail="checkpoint timeout")

    assert book.counts()["stopped"] is None
    assert claimed(book)["order"] == 2


def test_a_campaign_whose_runs_all_failed_is_visibly_not_a_measured_one(tmp_path):
    planned(tmp_path)
    book = journal(tmp_path)
    for _ in range(3):
        work(book, campaign.FAILED)

    assert book.counts()["runs"][campaign.FAILED] == 3
    assert book.counts()["runs"][campaign.OBSERVED] == 0


def test_an_outcome_that_cannot_be_recorded_stops_the_campaign(tmp_path):
    # ADR-0166: an outcome that cannot be recorded still stops the campaign.
    # A controller that cannot say what a run produced has lost the run.
    planned(tmp_path)
    book = journal(tmp_path)
    run = claimed(book)

    with pytest.raises(Failure, match="recorded no outcome"):
        book.finish(run["order"], OWNER, "UNKNOWN", USED)

    assert "recorded no outcome" in book.counts()["stopped"]["reason"]


def test_a_refusal_that_stopped_the_campaign_persists_the_stop(tmp_path):
    # The stop must not live only in the exception: a refusal that a caller
    # swallows would otherwise leave the next process opening a campaign that
    # looks like it is still running.
    planned(tmp_path)
    book = journal(tmp_path)
    claimed(book)
    with pytest.raises(Failure):
        book.claim(OWNER)

    assert "recorded no outcome" in journal(tmp_path).counts()["stopped"]["reason"]


def test_a_claim_still_standing_stops_the_campaign(tmp_path):
    # One at a time, so a leftover claim means the previous run's outcome never
    # reached the journal — including the case the owner check cannot see,
    # where the live controller itself failed to write it.
    planned(tmp_path)
    book = journal(tmp_path)
    claimed(book)

    with pytest.raises(Failure, match="recorded no outcome"):
        book.claim(OWNER)


def test_a_second_controller_cannot_start_a_run_beside_a_live_one(tmp_path):
    # Two measurement JVMs on one host invalidate both cells' numbers.
    planned(tmp_path)
    first = journal(tmp_path)
    claimed(first)

    with pytest.raises(Failure, match="recorded no outcome"):
        journal(tmp_path).claim(OTHER)


def test_an_owner_that_died_holding_a_claim_stops_the_campaign(tmp_path):
    # An interrupted observation, not a run to hand out again: the cell was
    # spent, so repeating it would measure against a queue that is not empty.
    planned(tmp_path)
    claimed(journal(tmp_path))

    resumed = journal(tmp_path, alive=lambda owner: False)

    assert "lost its owner" in resumed.counts()["stopped"]["reason"]


def test_opening_a_campaign_reports_a_lost_run_without_being_asked_for_one(tmp_path):
    # The natural "where is this campaign?" call is counts(), not claim().
    planned(tmp_path)
    claimed(journal(tmp_path))

    assert journal(tmp_path, alive=lambda owner: False).stopped() is not None


def test_the_owner_checked_for_liveness_is_the_one_that_holds_the_claim(tmp_path):
    planned(tmp_path)
    claimed(journal(tmp_path), owner=OTHER)
    seen = []

    journal(tmp_path, alive=lambda owner: seen.append(owner) or True)

    assert seen == [OTHER]


def test_a_claim_that_outlived_its_supervision_stops_the_campaign(tmp_path):
    clock = Clock()
    planned(tmp_path)
    book = journal(tmp_path, clock=clock)
    claimed(book)

    clock.now += campaign.HEARTBEAT_EXPIRY_SECONDS + 1

    assert "outlived its supervision" in book.stopped()["reason"]


def test_a_claim_at_exactly_the_expiry_is_still_supervised(tmp_path):
    clock = Clock()
    planned(tmp_path)
    book = journal(tmp_path, clock=clock)
    claimed(book)

    clock.now += campaign.HEARTBEAT_EXPIRY_SECONDS

    assert book.stopped() is None


def test_a_heartbeat_keeps_a_long_run_alive(tmp_path):
    clock = Clock()
    planned(tmp_path)
    book = journal(tmp_path, clock=clock)
    run = claimed(book)

    for _ in range(4):
        clock.now += campaign.HEARTBEAT_EXPIRY_SECONDS - 1
        book.heartbeat(run["order"], OWNER)

    assert book.stopped() is None


def test_the_first_stop_is_the_one_that_explains_the_campaign(tmp_path):
    clock = Clock()
    planned(tmp_path)
    book = journal(tmp_path, clock=clock)
    first = book.stop("lost its owner")

    clock.now += 60
    book.stop("does not fit the remaining bytes")

    assert book.stopped() == first
    assert book.stopped()["at"] == "2026-09-22T00:00:00Z"


def test_a_stopped_campaign_refuses_a_late_outcome(tmp_path):
    # A suspended host can stop a campaign while the JVM it stopped for is
    # still alive; recording its outcome would leave counts() showing a
    # complete campaign beside a stop reason naming that very run.
    clock = Clock()
    planned(tmp_path)
    book = journal(tmp_path, clock=clock)
    run = claimed(book)
    clock.now += campaign.HEARTBEAT_EXPIRY_SECONDS + 1
    book.stopped()

    with pytest.raises(Failure, match="Campaign stopped"):
        book.finish(run["order"], OWNER, campaign.OBSERVED, USED)


def test_a_run_is_finished_by_the_owner_that_holds_it(tmp_path):
    planned(tmp_path)
    book = journal(tmp_path)
    run = claimed(book)

    with pytest.raises(Failure, match="held by another owner"):
        book.finish(run["order"], OTHER, campaign.OBSERVED, USED)


def test_counts_carry_the_stop_so_a_report_cannot_look_clean(tmp_path):
    planned(tmp_path)
    book = journal(tmp_path)
    book.stop("lost its owner")

    assert book.counts()["stopped"]["reason"] == "lost its owner"


@pytest.mark.parametrize("axis", ["bytes", "seconds"])
def test_a_run_that_does_not_fit_the_remaining_budget_stops_it(tmp_path, axis):
    # Both axes, because the two are separate comparisons that look identical.
    budgets = {**BUDGETS, axis: {"bytes": 2_500, "seconds": 750}[axis]}
    planned(tmp_path, budgets=budgets)
    book = journal(tmp_path)
    work(book)
    work(book)

    with pytest.raises(Failure, match=f"remaining {axis}"):
        book.claim(OWNER)


def test_a_run_that_fits_the_budget_exactly_is_still_handed_out(tmp_path):
    planned(tmp_path, budgets={"bytes": 3_000, "seconds": 900})
    book = journal(tmp_path)
    work(book)
    work(book)

    assert claimed(book)["order"] == 3


def test_a_reservation_is_taken_at_claim_and_settled_at_finish(tmp_path):
    # Taken at claim, because a run that overruns has already spent it.
    planned(tmp_path)
    book = journal(tmp_path)
    run = claimed(book)

    assert book.counts()["consumed"] == {"bytes": 1_000, "seconds": 300}

    book.finish(run["order"], OWNER, campaign.OBSERVED, USED)

    assert book.counts()["consumed"] == USED
    assert book.counts()["remaining"] == {"bytes": 9_600, "seconds": 3_480}


def test_a_run_that_overran_its_reservation_is_charged_the_overage(tmp_path):
    # The half a clamp would silently drop, taking the budget stop with it.
    planned(tmp_path)
    book = journal(tmp_path)
    run = claimed(book)

    book.finish(
        run["order"], OWNER, campaign.OBSERVED, {"bytes": 4_000, "seconds": 900}
    )

    assert book.counts()["consumed"] == {"bytes": 4_000, "seconds": 900}
    assert book.counts()["remaining"] == {"bytes": 6_000, "seconds": 2_700}


def test_an_unfinished_run_is_charged_what_it_reserved(tmp_path):
    planned(tmp_path)
    claimed(journal(tmp_path))

    assert journal(tmp_path).counts()["remaining"]["bytes"] == 9_000


@pytest.mark.parametrize(
    "consumed",
    [
        {"bytes": -200, "seconds": 90},
        {"bytes": 400},
        {"bytes": 400, "seconds": 120, "extra": 1},
        {"bytes": 400.0, "seconds": 120},
    ],
)
def test_a_settlement_the_budget_could_not_trust_is_refused(tmp_path, consumed):
    # A negative settlement would grow the remaining budget past the plan.
    planned(tmp_path)
    book = journal(tmp_path)
    run = claimed(book)

    with pytest.raises(Failure, match="consumed|must report"):
        book.finish(run["order"], OWNER, campaign.OBSERVED, consumed)


def test_a_changed_frozen_input_refuses_to_open_the_campaign(tmp_path):
    planned(tmp_path)

    with pytest.raises(Failure, match="inputs differ"):
        journal(tmp_path, inputs={**INPUTS, "classpathSha256": "c" * 64})


def test_an_edited_manifest_refuses_the_inputs_it_was_planned_against(tmp_path):
    planned(tmp_path)
    manifest = json.loads((tmp_path / campaign.MANIFEST).read_bytes())
    manifest["inputs"]["protocolSha256"] = "c" * 64
    (tmp_path / campaign.MANIFEST).write_bytes(json.dumps(manifest).encode())

    with pytest.raises(Failure, match="inputs differ"):
        journal(tmp_path)


def test_planning_twice_refuses_rather_than_resetting_the_state(tmp_path):
    planned(tmp_path)
    work(journal(tmp_path))

    with pytest.raises(Failure, match="already planned"):
        planned(tmp_path)

    assert journal(tmp_path).counts()["runs"][campaign.OBSERVED] == 1


def test_an_identical_plan_produces_a_byte_identical_manifest(tmp_path):
    first, second = tmp_path / "a", tmp_path / "b"
    planned(first)
    planned(second)

    assert (first / campaign.MANIFEST).read_bytes() == (
        second / campaign.MANIFEST
    ).read_bytes()


def test_a_finished_run_cannot_be_finished_again(tmp_path):
    planned(tmp_path)
    book = journal(tmp_path)
    run = work(book)

    with pytest.raises(Failure, match="not CLAIMED"):
        book.finish(run["order"], OWNER, campaign.FAILED, USED)


@pytest.mark.parametrize(
    ("change", "message"),
    [
        ({"runs": []}, "at least one run"),
        ({"budgets": {"bytes": 1}}, "names bytes and seconds"),
        ({"budgets": {"bytes": 0, "seconds": 1}}, "positive integer"),
        ({"budgets": {"bytes": True, "seconds": 1}}, "positive integer"),
        ({"campaign_id": "Cal 1246"}, "lowercase DNS-style"),
    ],
)
def test_a_campaign_refuses_a_plan_it_could_not_admit(tmp_path, change, message):
    arguments = {
        "runs": runs(),
        "budgets": BUDGETS,
        "campaign_id": "cal1246d",
        **change,
    }

    with pytest.raises(Failure, match=message):
        campaign.plan_campaign(
            tmp_path,
            arguments["campaign_id"],
            arguments["runs"],
            INPUTS,
            arguments["budgets"],
        )

    assert not (tmp_path / campaign.MANIFEST).exists()


@pytest.mark.parametrize(
    ("reserves", "message"),
    [
        ({"bytes": 0, "seconds": 300}, "reserved bytes"),
        ({"bytes": -1, "seconds": 300}, "reserved bytes"),
        ({"bytes": True, "seconds": 300}, "reserved bytes"),
        ({"bytes": 1_000}, "must reserve"),
        ({"bytes": 1_000, "seconds": 300, "spare": 1}, "must reserve"),
    ],
)
def test_a_run_whose_reservation_the_budget_could_not_use_is_refused(
    tmp_path, reserves, message
):
    with pytest.raises(Failure, match=message):
        campaign.plan_campaign(tmp_path, "cal1246d", runs(1, reserves), INPUTS, BUDGETS)


def test_a_plan_with_a_gap_in_its_order_is_refused(tmp_path):
    gapped = runs(3)
    gapped[2]["order"] = 4

    with pytest.raises(Failure, match="ordered 1..n"):
        campaign.plan_campaign(tmp_path, "cal1246d", gapped, INPUTS, BUDGETS)


def test_a_run_without_a_cell_is_refused(tmp_path):
    nameless = runs(1)
    nameless[0]["cellId"] = "Not A Cell"

    with pytest.raises(Failure, match="must name a cell"):
        campaign.plan_campaign(tmp_path, "cal1246d", nameless, INPUTS, BUDGETS)


def test_a_failed_publication_leaves_the_previous_state_intact(tmp_path, monkeypatch):
    # What the rename is for. The campaign is killed between writing the new
    # bytes and publishing them; the ledger of which cells were spent must
    # survive, because nothing else records it.
    planned(tmp_path)
    book = journal(tmp_path)
    work(book)
    run = claimed(book)

    def refuse(source, target):
        raise OSError("no space left on device")

    monkeypatch.setattr(campaign.os, "replace", refuse)
    with pytest.raises(Failure, match="Could not write"):
        book.finish(run["order"], OWNER, campaign.OBSERVED, USED)
    monkeypatch.undo()

    resumed = journal(tmp_path)
    assert resumed.counts()["runs"][campaign.OBSERVED] == 1
    assert resumed.counts()["runs"][campaign.CLAIMED] == 1
    # The outcome never landed, so the campaign must not carry on past it.
    with pytest.raises(Failure, match="recorded no outcome"):
        resumed.claim(OWNER)


def test_a_failed_publication_leaves_no_temporary_file_behind(tmp_path, monkeypatch):
    planned(tmp_path)
    book = journal(tmp_path)
    run = claimed(book)

    def refuse(source, target):
        raise OSError("no space left on device")

    monkeypatch.setattr(campaign.os, "replace", refuse)
    with pytest.raises(Failure):
        book.finish(run["order"], OWNER, campaign.OBSERVED, USED)
    monkeypatch.undo()

    assert [path.name for path in tmp_path.iterdir() if ".writing" in path.name] == []


def test_a_write_is_fsynced_before_it_is_published(tmp_path, monkeypatch):
    # The rename alone survives a killed process; the fsyncs are what a host
    # crash needs as well.
    synced = []
    real = os.fsync
    monkeypatch.setattr(campaign.os, "fsync", lambda fd: synced.append(fd) or real(fd))

    planned(tmp_path)

    # Two files, each the payload and then its directory.
    assert len(synced) >= 4


def test_an_unplanned_directory_fails_as_a_campaign_error(tmp_path):
    with pytest.raises(Failure, match="Unreadable campaign file"):
        journal(tmp_path)


def test_a_corrupt_state_file_fails_as_a_campaign_error(tmp_path):
    planned(tmp_path)
    (tmp_path / campaign.STATE).write_bytes(b'{"half":')

    with pytest.raises(Failure, match="Unreadable campaign file state.json"):
        journal(tmp_path)


def test_the_journal_is_not_delivered_to_the_supervisor_pod():
    # The cluster ConfigMap has a shared 1 MiB ceiling that a Cloud Tasks
    # session has already reached once. This module is for the single-host
    # rig, and the supervisor has no use for it, so it must stay out of the
    # delivered closure rather than spending that ceiling.
    delivered = delivered_sources()

    # A positive control, so "excluded" cannot be read from an empty walk.
    assert "runtime.py" in delivered
    assert "campaign.py" not in delivered


def test_the_lock_is_held_across_the_read_and_the_write(tmp_path, monkeypatch):
    # Renaming a whole file publishes one writer's bytes at once; it does not
    # keep two writers from computing their next state from the same stale one.
    planned(tmp_path)
    book = journal(tmp_path)
    held = []
    real = campaign.fcntl.flock

    monkeypatch.setattr(
        campaign.fcntl, "flock", lambda fd, op: held.append(op) or real(fd, op)
    )
    work(book)

    assert campaign.fcntl.LOCK_EX in held
    assert held.count(campaign.fcntl.LOCK_EX) == held.count(campaign.fcntl.LOCK_UN)


def test_two_journals_do_not_hand_out_the_same_run(tmp_path):
    planned(tmp_path)
    first = journal(tmp_path)
    run = claimed(first)
    first.finish(run["order"], OWNER, campaign.OBSERVED, USED)

    assert claimed(journal(tmp_path))["order"] == 2


def test_a_campaign_directory_holds_only_what_it_needs(tmp_path):
    planned(tmp_path)
    work(journal(tmp_path))

    assert sorted(path.name for path in tmp_path.iterdir()) == [
        campaign.LOCK,
        campaign.MANIFEST,
        campaign.STATE,
    ]


def test_the_state_file_dates_its_records_for_a_reader(tmp_path):
    planned(tmp_path)
    book = journal(tmp_path)
    work(book)

    state = json.loads((tmp_path / campaign.STATE).read_bytes())

    assert state["runs"]["1"]["finished"] == "2026-09-22T00:00:00Z"
    assert state["runs"]["1"]["cellId"] == "k01"


def test_a_plan_that_names_a_cell_twice_is_refused(tmp_path):
    # A cell planned twice is a cell the campaign would spend twice.
    twice = runs(2)
    twice[1]["cellId"] = twice[0]["cellId"]

    with pytest.raises(Failure, match="each cell at most once"):
        campaign.plan_campaign(tmp_path, "cal1246d", twice, INPUTS, BUDGETS)


def _edit_plan(directory, order, cell):
    manifest = json.loads((directory / campaign.MANIFEST).read_bytes())
    manifest["plan"][order - 1]["cellId"] = cell
    (directory / campaign.MANIFEST).write_bytes(json.dumps(manifest).encode())


def test_a_manifest_edited_to_repeat_a_spent_cell_refuses_to_open(tmp_path):
    # The inputs still match, so only the plan itself can catch this: run 2
    # would otherwise be handed out under run 1's already-spent cell.
    planned(tmp_path)
    work(journal(tmp_path))
    _edit_plan(tmp_path, 2, "k01")

    with pytest.raises(Failure, match="differs from the one its state"):
        journal(tmp_path)


def test_a_manifest_edited_to_rename_a_cell_refuses_to_open(tmp_path):
    planned(tmp_path)
    _edit_plan(tmp_path, 2, "k99")

    with pytest.raises(Failure, match="differs from the one its state"):
        journal(tmp_path)


def test_planning_holds_the_campaign_lock_across_the_check_and_the_writes(
    tmp_path, monkeypatch
):
    # Otherwise two planners both pass the "already planned" check, and the
    # second rewrites a state the first had begun claiming from.
    events = []
    real = campaign.fcntl.flock

    def record(fd, operation):
        events.append(("flock", operation, (tmp_path / campaign.MANIFEST).exists()))
        return real(fd, operation)

    monkeypatch.setattr(campaign.fcntl, "flock", record)
    planned(tmp_path)

    assert events[0] == ("flock", campaign.fcntl.LOCK_EX, False)
    assert events[-1] == ("flock", campaign.fcntl.LOCK_UN, True)


def test_a_late_heartbeat_does_not_revive_an_expired_claim(tmp_path):
    clock = Clock()
    planned(tmp_path)
    book = journal(tmp_path, clock=clock)
    run = claimed(book)
    clock.now += campaign.HEARTBEAT_EXPIRY_SECONDS + 1

    with pytest.raises(Failure, match="outlived its supervision"):
        book.heartbeat(run["order"], OWNER)

    assert "outlived its supervision" in journal(tmp_path).stopped()["reason"]


def test_a_late_outcome_does_not_settle_an_expired_claim(tmp_path):
    clock = Clock()
    planned(tmp_path)
    book = journal(tmp_path, clock=clock)
    run = claimed(book)
    clock.now += campaign.HEARTBEAT_EXPIRY_SECONDS + 1

    with pytest.raises(Failure, match="outlived its supervision"):
        book.finish(run["order"], OWNER, campaign.OBSERVED, USED)

    assert book.counts()["runs"][campaign.OBSERVED] == 0


def test_counts_on_an_open_journal_notice_an_owner_that_died_since(tmp_path):
    # The journal was opened while the owner was alive; a report taken later
    # from the same object must not show a claimed run on a running campaign.
    owners = {"alive": True}
    planned(tmp_path)
    book = journal(tmp_path, alive=lambda owner: owners["alive"])
    claimed(book)
    owners["alive"] = False

    assert "lost its owner" in book.counts()["stopped"]["reason"]


def _rewrite_manifest(directory, edit):
    manifest = json.loads((directory / campaign.MANIFEST).read_bytes())
    edit(manifest["plan"])
    (directory / campaign.MANIFEST).write_bytes(json.dumps(manifest).encode())


def test_a_manifest_with_reordered_entries_refuses_to_open(tmp_path):
    # Claims read the plan by position: swapped entries would hand out order 1
    # under order 2's cell while recording order 1 as the one claimed.
    planned(tmp_path)
    _rewrite_manifest(tmp_path, lambda plan: plan.reverse())

    with pytest.raises(Failure, match="differs from the one its state"):
        journal(tmp_path)


@pytest.mark.parametrize(
    "edit",
    [
        lambda plan: plan[1]["reserves"].update(bytes=1),
        lambda plan: plan[1].update(arm="NAMED_HASH"),
    ],
    ids=["reservation", "arm"],
)
def test_a_manifest_with_a_changed_run_refuses_to_open(tmp_path, edit):
    planned(tmp_path)
    _rewrite_manifest(tmp_path, edit)

    with pytest.raises(Failure, match="differs from the one its state"):
        journal(tmp_path)


def test_a_state_file_without_its_plan_digest_fails_as_a_campaign_error(tmp_path):
    planned(tmp_path)
    state = json.loads((tmp_path / campaign.STATE).read_bytes())
    del state["planSha256"]
    (tmp_path / campaign.STATE).write_bytes(json.dumps(state).encode())

    with pytest.raises(Failure, match="differs from the one its state"):
        journal(tmp_path)


def test_a_state_file_with_swapped_run_records_refuses_to_open(tmp_path):
    # Swapping records leaves the manifest digest intact but would return a
    # finished run to PENDING under its order, and its cell would be spent again.
    planned(tmp_path)
    work(journal(tmp_path))
    state = json.loads((tmp_path / campaign.STATE).read_bytes())
    state["runs"]["1"], state["runs"]["2"] = state["runs"]["2"], state["runs"]["1"]
    (tmp_path / campaign.STATE).write_bytes(json.dumps(state).encode())

    with pytest.raises(Failure, match="no longer matches the plan"):
        journal(tmp_path)
