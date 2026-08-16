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
"""Tests for scripts/sweep-e2e.sh (issues #246, #224, #630).

This script deletes cloud resources, so the tests are about what it must
*not* delete as much as what it must: a foreign prefix, an id it cannot date,
and anything younger than the threshold all have to survive. The one failure
mode worse than deleting too much is reporting success while sweeping nothing,
which is what a guardrail looks like when it has quietly stopped working.

It sweeps Bigtable and Spanner instances and stops the fixed App Engine
fixture. The reason they share one script rather than separate just-recipe
lines is pinned here too: one cleanup failing must not skip the others.

gcloud is a stub on PATH. The script is copied into a synthetic tree so the
constants it reads out of each Java source can be controlled — plus one test
against the real tree, which is what notices if either source stops parsing.
"""

import shutil
import subprocess
import time

import pytest
from conftest import SCRIPTS

SOURCE = """\
package io.github.flink.gcp.connector.{package};

abstract class {klass} {{
    private static final String INSTANCE_PREFIX = "{prefix}";
    private static final Duration STALE_AFTER = Duration.ofHours({hours});
}}
"""

# One listing file per gcloud group, so a test can give the two services
# different instances; deletes are logged as "<group> <id>".
GCLOUD_STUB = """\
#!/usr/bin/env bash
set -eu
if [ "$1" = app ]; then
    if [ "$2" = versions ] && [ "$3" = describe ]; then
        cat "$GCLOUD_STUB_DIR/appengine-status.txt"
        exit 0
    fi
    if [ "$2" = instances ] && [ "$3" = list ]; then
        cat "$GCLOUD_STUB_DIR/appengine-instances.txt"
        exit 0
    fi
    if [ "$2" = versions ] && [ "$3" = stop ]; then
        echo "$*" >> "$GCLOUD_STUB_EVENT_LOG"
        echo "appengine default/$4" >> "$GCLOUD_STUB_LOG"
        case " ${GCLOUD_STUB_FAIL:-} " in
            *" $4 "*) exit 1 ;;
        esac
        echo STOPPED > "$GCLOUD_STUB_DIR/appengine-status.txt"
        : > "$GCLOUD_STUB_DIR/appengine-instances.txt"
        exit 0
    fi
    echo "unexpected gcloud invocation: $*" >&2
    exit 1
fi
if [ "$1" = bigtable ] && [ "$3" = tables ]; then
    case "$4" in
        list) cat "$GCLOUD_STUB_DIR/bigtable-tables.txt" ;;
        update)
            echo "$*" >> "$GCLOUD_STUB_EVENT_LOG"
            echo "$5" >> "$GCLOUD_STUB_PREP_LOG"
            case " ${GCLOUD_STUB_PREP_FAIL:-} " in
                *" $5 "*) exit 1 ;;
            esac
            ;;
    esac
    exit 0
fi
if [ -n "${GCLOUD_STUB_LIST_FAILS:-}" ] && [ "$3" = list ]; then
    echo "ERROR: (gcloud) not authenticated" >&2
    exit 1
fi
case "$3" in
    list) cat "$GCLOUD_STUB_DIR/$1-instances.txt" ;;
    delete)
        echo "$*" >> "$GCLOUD_STUB_EVENT_LOG"
        echo "$1 $4" >> "$GCLOUD_STUB_LOG"
        case " ${GCLOUD_STUB_FAIL:-} " in
            *" $4 "*) exit 1 ;;
        esac
        ;;
esac
"""

SERVICES = {
    "bigtable": ("flink-connector-gcp-bigtable", "AbstractBigtableRealGcpITCase"),
    "spanner": ("flink-connector-gcp-spanner", "AbstractSpannerRealGcpITCase"),
}


@pytest.fixture()
def sweep(tmp_path):
    """A copy of the script over a synthetic tree, with gcloud stubbed."""
    (tmp_path / "scripts").mkdir()
    script = tmp_path / "scripts" / "sweep-e2e.sh"
    shutil.copy(SCRIPTS / "sweep-e2e.sh", script)
    shutil.copy(SCRIPTS / "appengine-e2e-fixture.sh", tmp_path / "scripts")

    tofu = tmp_path / "opentofu" / "flink-gcp"
    tofu.mkdir(parents=True)
    (tofu / "appengine-e2e.tf").write_text(
        """\
locals {
  cloudtasks_appengine_e2e_service = "default"
  cloudtasks_appengine_e2e_version = "flink-e2e"
}
"""
    )

    sources = {}
    for group, (module, klass) in SERVICES.items():
        java = tmp_path / module / "src" / "test" / "java" / f"{klass}.java"
        java.parent.mkdir(parents=True)
        java.write_text(
            SOURCE.format(package=group, klass=klass, prefix="flink-it-", hours=2)
        )
        sources[group] = java

    stub_dir = tmp_path / "bin"
    stub_dir.mkdir()
    gcloud = stub_dir / "gcloud"
    gcloud.write_text(GCLOUD_STUB)
    gcloud.chmod(0o755)
    log = tmp_path / "deleted.txt"
    log.write_text("")
    prep_log = tmp_path / "prepared.txt"
    prep_log.write_text("")
    event_log = tmp_path / "events.txt"
    event_log.write_text("")

    def run(
        *args,
        instances=(),
        spanner_instances=(),
        bigtable_tables=(),
        fail=(),
        table_update_fails=(),
        list_fails=False,
        project="flink-gcp",
        spanner_project="flink-gcp",
        cloudtasks_project="flink-gcp",
        appengine_status="STOPPED",
        appengine_instances=(),
    ):
        listed = {"bigtable": instances, "spanner": spanner_instances}
        for group, ids in listed.items():
            (tmp_path / f"{group}-instances.txt").write_text(
                "".join(f"projects/p/instances/{i}\n" for i in ids)
            )
        (tmp_path / "bigtable-tables.txt").write_text(
            "".join(
                f"projects/p/instances/i/tables/{table}\n" for table in bigtable_tables
            )
        )
        (tmp_path / "appengine-status.txt").write_text(f"{appengine_status}\n")
        (tmp_path / "appengine-instances.txt").write_text(
            "".join(f"{instance}\n" for instance in appengine_instances)
        )
        env = {
            "PATH": f"{stub_dir}:/usr/bin:/bin",
            "GCLOUD_STUB_DIR": str(tmp_path),
            "GCLOUD_STUB_LOG": str(log),
            "GCLOUD_STUB_FAIL": " ".join(fail),
            "GCLOUD_STUB_PREP_LOG": str(prep_log),
            "GCLOUD_STUB_PREP_FAIL": " ".join(table_update_fails),
            "GCLOUD_STUB_EVENT_LOG": str(event_log),
            "APPENGINE_E2E_POLL_ATTEMPTS": "1",
            "APPENGINE_E2E_POLL_SECONDS": "0",
        }
        if project is not None:
            env["BIGTABLE_IT_PROJECT"] = project
        if spanner_project is not None:
            env["SPANNER_IT_PROJECT"] = spanner_project
        if cloudtasks_project is not None:
            env["CLOUDTASKS_IT_PROJECT"] = cloudtasks_project
        if list_fails:
            env["GCLOUD_STUB_LIST_FAILS"] = "1"
        result = subprocess.run(
            [str(script), *args], env=env, capture_output=True, text=True, check=False
        )
        result.deleted = log.read_text().splitlines()
        result.prepared = prep_log.read_text().splitlines()
        result.events = event_log.read_text().splitlines()
        return result

    run.sources = sources
    return run


def ago(hours):
    return int(time.time()) - int(hours * 3600)


def instance(hours_old, prefix="flink-it-", run_id="abcd1234"):
    return f"{prefix}{ago(hours_old)}-{run_id}"


# --- what must be deleted ---


def test_an_abandoned_instance_is_deleted(sweep):
    stale = instance(5)
    result = sweep(instances=[stale])
    assert result.returncode == 0, result.stderr
    assert result.deleted == [f"bigtable {stale}"]


def test_change_streams_are_disabled_before_a_bigtable_instance_is_deleted(sweep):
    stale = instance(5)
    result = sweep(instances=[stale], bigtable_tables=["plain", "change-stream"])

    assert result.returncode == 0, result.stderr
    assert result.prepared == ["plain", "change-stream"]
    assert result.deleted == [f"bigtable {stale}"]
    assert result.events == [
        (
            "bigtable instances tables update "
            f"plain --instance={stale} --project=flink-gcp "
            "--clear-change-stream-retention-period --quiet"
        ),
        (
            "bigtable instances tables update "
            f"change-stream --instance={stale} --project=flink-gcp "
            "--clear-change-stream-retention-period --quiet"
        ),
        f"bigtable instances delete {stale} --project=flink-gcp --quiet",
    ]


def test_a_change_stream_disable_failure_leaves_the_instance_for_retry(sweep):
    stale = instance(5)
    result = sweep(
        instances=[stale],
        spanner_instances=[stale],
        bigtable_tables=["change-stream"],
        table_update_fails=["change-stream"],
    )

    assert result.returncode == 1
    assert result.deleted == [f"spanner {stale}"]
    assert "failed to disable Change Streams" in result.stderr


def test_an_id_without_a_run_suffix_is_still_dated(sweep):
    # The Java tolerates a bare <prefix><epoch>; so does this.
    stale = f"flink-it-{ago(5)}"
    assert sweep(instances=[stale]).deleted == [f"bigtable {stale}"]


def test_all_billed_resource_types_are_swept_in_one_run(sweep):
    bigtable, spanner = instance(5), instance(6)
    result = sweep(
        instances=[bigtable],
        spanner_instances=[spanner],
        appengine_status="SERVING",
        appengine_instances=["instance-1"],
    )
    assert result.returncode == 0, result.stderr
    assert result.deleted == [
        f"bigtable {bigtable}",
        f"spanner {spanner}",
        "appengine default/flink-e2e",
    ]


# --- what must survive ---


@pytest.mark.parametrize(
    "survivor",
    [
        # Younger than the two-hour threshold: a run in progress.
        instance(0.5),
        instance(1.9),
        # Another project's instance that happens to share the zone.
        instance(99, prefix="production-"),
        # The one that matters: a foreign name whose first segment *is* a stale
        # epoch. Without the prefix guard this is deleted — every other
        # survivor here is caught by the date parse as well, so this is the
        # case that tells the two guards apart.
        "1700000000-legacy",
        # Dateable-looking but not: no epoch where one is expected.
        "flink-it-notanepoch-abcd1234",
        "flink-it--abcd1234",
        "flink-it-",
    ],
)
def test_what_the_sweep_must_not_touch(sweep, survivor):
    result = sweep(instances=[survivor], spanner_instances=[survivor])
    assert result.returncode == 0, result.stderr
    assert result.deleted == []
    # And quietly: dropping the explicit date check leaves `[ -lt ]` to reject
    # these itself, which works and prints "integer expression expected" for
    # every unrelated instance in the project — noise a daily job would carry
    # forever.
    assert result.stderr == ""


def test_a_running_instance_survives_beside_an_abandoned_one(sweep):
    stale, live = instance(5), instance(0.1)
    result = sweep(instances=[stale, live])
    assert result.deleted == [f"bigtable {stale}"]


# --- reporting ---


def test_dry_run_deletes_nothing_and_says_what_it_would(sweep):
    stale = instance(5)
    result = sweep(
        "--dry-run",
        instances=[stale],
        spanner_instances=[stale],
        appengine_status="SERVING",
        appengine_instances=["instance-1"],
    )
    assert result.returncode == 0
    assert result.deleted == []
    assert f"would delete bigtable {stale}" in result.stdout
    assert f"would delete spanner {stale}" in result.stdout
    assert "would stop App Engine fixture" in result.stdout


def test_a_failed_delete_is_reported(sweep):
    stale = instance(5)
    result = sweep(instances=[stale], fail=[stale])
    assert result.returncode == 1
    assert "could not be deleted" in result.stderr


def test_a_failed_delete_does_not_stop_the_other_service(sweep):
    # Why this is one script and not two lines of a just recipe: just stops at
    # the first failing line, so a Bigtable delete that fails would silently
    # skip the Spanner sweep — the guardrail failing quietly in the direction
    # that costs money.
    bigtable, spanner = instance(5), instance(6)
    result = sweep(instances=[bigtable], spanner_instances=[spanner], fail=[bigtable])
    assert result.returncode == 1
    assert result.deleted == [f"bigtable {bigtable}", f"spanner {spanner}"]


def test_a_failed_app_engine_stop_does_not_hide_the_instance_sweeps(sweep):
    bigtable, spanner = instance(5), instance(6)
    result = sweep(
        instances=[bigtable],
        spanner_instances=[spanner],
        appengine_status="SERVING",
        appengine_instances=["instance-1"],
        fail=["flink-e2e"],
    )

    assert result.returncode == 1
    assert result.deleted == [
        f"bigtable {bigtable}",
        f"spanner {spanner}",
        "appengine default/flink-e2e",
    ]


def test_an_infrastructure_error_outranks_a_failed_delete(sweep):
    # The worst status wins, so a run that could not even list one service does
    # not report the milder "a delete failed".
    stale = instance(5)
    result = sweep(instances=[stale], fail=[stale], spanner_project=None)
    assert result.returncode == 2


def test_a_listing_that_fails_is_not_an_empty_sweep(sweep):
    # The failure mode worth the most: `set -e` does not see a failing process
    # substitution, so piping the listing into the loop would report "0 stale
    # instances swept" and exit 0 on an unauthenticated gcloud.
    result = sweep(instances=[instance(5)], list_fails=True)
    assert result.returncode != 0
    assert "swept" not in result.stdout


# --- refusing to run on a tree it does not understand ---


@pytest.mark.parametrize(
    "missing", ["project", "spanner_project", "cloudtasks_project"]
)
def test_a_missing_project_is_an_infrastructure_error(sweep, missing):
    assert sweep(**{missing: None}).returncode == 2


@pytest.mark.parametrize("group", sorted(SERVICES))
@pytest.mark.parametrize("field", ["INSTANCE_PREFIX", "STALE_AFTER"])
def test_a_constant_it_cannot_read_is_an_infrastructure_error(sweep, group, field):
    # The prefix and the threshold have one home per service, in the Java. A
    # shape this cannot parse must stop the run: a sweep that matches nothing
    # looks exactly like a sweep with nothing to do.
    java = sweep.sources[group]
    text = java.read_text()
    java.write_text("\n".join(line for line in text.splitlines() if field not in line))
    result = sweep(instances=[instance(5)], spanner_instances=[instance(5)])
    assert result.returncode == 2
    # The other service is still swept: one unparsable source must not turn the
    # whole daily job into a no-op.
    assert result.deleted != []


@pytest.mark.parametrize("group", sorted(SERVICES))
def test_a_missing_source_file_is_an_infrastructure_error(sweep, group):
    sweep.sources[group].unlink()
    assert sweep(instances=[instance(5)]).returncode == 2


@pytest.mark.parametrize(
    "args",
    [
        ("--delete-everything",),
        # Not dropped on the floor: the argument a caller is most likely to
        # invent is a narrowing one, and ignoring it would widen a delete.
        ("--dry-run", "--only=flink-it-123"),
        ("--dry-run", "extra"),
    ],
)
def test_arguments_it_does_not_understand_are_rejected(sweep, args):
    result = sweep(*args, instances=[instance(5)])
    assert result.returncode == 2
    assert result.deleted == []


# --- against the real tree ---


def test_the_real_java_sources_still_parse(tmp_path):
    # The one real-repo assertion here, and the coupling most likely to break:
    # renaming the constants, or moving either class, must fail loudly rather
    # than turn the sweep into a no-op. Values are deliberately not asserted —
    # this notices a source that stopped parsing, not a threshold someone
    # changed.
    stub_dir = tmp_path / "bin"
    stub_dir.mkdir()
    gcloud = stub_dir / "gcloud"
    gcloud.write_text(GCLOUD_STUB)
    gcloud.chmod(0o755)
    for group in SERVICES:
        (tmp_path / f"{group}-instances.txt").write_text("")
    (tmp_path / "appengine-status.txt").write_text("STOPPED\n")
    (tmp_path / "appengine-instances.txt").write_text("")
    result = subprocess.run(
        [str(SCRIPTS / "sweep-e2e.sh"), "--dry-run"],
        env={
            "PATH": f"{stub_dir}:/usr/bin:/bin",
            "BIGTABLE_IT_PROJECT": "flink-gcp",
            "SPANNER_IT_PROJECT": "flink-gcp",
            "CLOUDTASKS_IT_PROJECT": "flink-gcp",
            "GCLOUD_STUB_DIR": str(tmp_path),
            "GCLOUD_STUB_LOG": str(tmp_path / "deleted.txt"),
            "GCLOUD_STUB_PREP_LOG": str(tmp_path / "prepared.txt"),
            "GCLOUD_STUB_EVENT_LOG": str(tmp_path / "events.txt"),
            "APPENGINE_E2E_POLL_ATTEMPTS": "1",
            "APPENGINE_E2E_POLL_SECONDS": "0",
        },
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    for group in SERVICES:
        assert f"{group} instances older than" in result.stdout
    assert "stopped with zero instances" in result.stdout
