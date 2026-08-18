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

"""The sweep's `workflow_run` trigger names the E2E workflow by its display name.

That coupling has no error path: rename `name:` in e2e.yaml and the trigger
matches nothing, so the sweep silently stops running after E2E and quietly
falls back to a schedule GitHub is allowed to drop (issue #964). Nothing in
actionlint, the workflows themselves or the sweep script can see it, which is
what these tests are for.

The WIF binding is the same shape of coupling in the other direction: the
sweep authenticates through an `event_name`-keyed member, so the trigger only
works while OpenTofu grants that event.
"""

import re
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent.parent
E2E = REPO / ".github/workflows/e2e.yaml"
SWEEP = REPO / ".github/workflows/sweep-e2e.yaml"
E2E_SA = REPO / "opentofu/flink-gcp/e2e-sa.tf"


def workflow_name(path):
    match = re.search(r"^name:\s*(.+?)\s*$", path.read_text(), re.MULTILINE)
    assert match, f"{path.name} declares no name:"
    return match.group(1)


def triggered_workflows():
    match = re.search(
        r"^\s*workflow_run:\s*\n\s*workflows:\s*\[(.+?)\]",
        SWEEP.read_text(),
        re.MULTILINE,
    )
    assert match, "sweep-e2e.yaml declares no workflow_run.workflows"
    return [name.strip().strip("\"'") for name in match.group(1).split(",")]


def test_the_sweep_names_a_workflow_that_exists():
    assert workflow_name(E2E) in triggered_workflows()


def test_the_trigger_fires_on_every_terminal_state():
    # types: [completed] is what makes cancellation reach the sweep, and
    # cancellation is the case the sweep exists for. A narrower type list would
    # leave the killed run — the expensive one — uncollected.
    assert re.search(
        r"workflow_run:\s*\n.*\n\s*types:\s*\[completed\]", SWEEP.read_text()
    )


def sweep_command():
    # Joined rather than matched on one line: the command is a folded scalar, so
    # the flag and the expression that gates it sit on different lines.
    text = SWEEP.read_text()
    assert "just sweep-e2e" in text, "sweep-e2e.yaml does not run just sweep-e2e"
    tail = text[text.index("just sweep-e2e") :]
    return " ".join(line.strip() for line in tail.splitlines() if line.strip())


def test_the_post_e2e_sweep_drops_the_age_threshold():
    # Without --all the workflow_run trigger reclaims nothing: the instances it
    # exists to collect are minutes old, which is exactly what the threshold
    # skips (issue #966). The flag is the whole point of that trigger.
    assert "--all" in sweep_command()


def test_only_the_post_e2e_sweep_drops_it():
    # And the other direction, which is the dangerous one: an unconditional
    # --all would let the three-hourly schedule delete an instance a run is
    # still using. The flag has to be gated on the event.
    # Pinned as the association, not as an ordering: with the two merely both
    # present, the inverted expression `(...) && '' || '--all'` — which puts the
    # flag on the *schedule*, the one case that must never have it — reads as
    # correct.
    command = sweep_command()
    assert "&& '--all' || ''" in command
    # And the condition is those two cases and no other.
    assert "github.event_name == 'workflow_run' || inputs.all" in command


def test_the_manual_switch_reads_the_typed_input():
    # github.event.inputs.all is the string "false" on an unchecked box, and a
    # non-empty string is truthy in a GitHub expression, so that spelling would
    # delete everything on every dispatch. inputs.all is a real boolean.
    assert "github.event.inputs" not in sweep_command()


def test_the_wif_binding_admits_the_event_the_trigger_uses():
    # The member is keyed on repository_id:event_name:ref, so an event missing
    # from this set fails authentication rather than being ignored.
    events = re.search(r"for_each\s*=\s*toset\(\[(.+?)\]\)", E2E_SA.read_text())
    assert events, "e2e-sa.tf declares no event set"
    assert "workflow_run" in [
        event.strip().strip('"') for event in events.group(1).split(",")
    ]
