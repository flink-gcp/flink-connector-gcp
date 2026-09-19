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
"""The rendered Cloud Tasks cell manifests satisfy the runtime's manifest contract."""

import json
import subprocess
from pathlib import Path

import flink_tier3 as rt
from flink_tier3.bundle import package_sources

ROOT = Path(__file__).resolve().parents[2]
RUN_ID = "contract-probe"
NONCE = "a" * 32
DIGEST = "sha256:" + "b" * 64


SESSIONS = sorted((ROOT / "kubernetes/lifecycle/sessions").glob("*.toml"))


def render(line, session_file=None):
    session = rt.load_session(
        session_file or ROOT / "kubernetes/lifecycle/sessions/example-wiring.toml"
    )
    image = rt.GAR + rt.FLINK_LINES[line][0] + "@" + DIGEST
    result = subprocess.run(
        [
            "cue",
            "export",
            "./lifecycle",
            "json:",
            "-",
            "--out",
            "json",
            "-e",
            "cellManifests",
            "-t",
            "run_id=" + RUN_ID,
            "-t",
            "nonce=" + NONCE,
            "-t",
            "expires_at=2026-09-20T00:00:00Z",
            "-t",
            "active_seconds=17820",
            "-t",
            "scenario=cloudtasks",
            "-t",
            "cells=" + json.dumps(session["cells"], separators=(",", ":")),
            "-t",
            "flink_version=" + line,
            "-t",
            "application_image=" + image,
            "-t",
            "target=" + rt.CLOUDTASKS_POLICY["target"],
        ],
        cwd=ROOT / "kubernetes",
        input=json.dumps({"packageSources": package_sources()}),
        capture_output=True,
        text=True,
        timeout=120,
        check=True,
    )
    return session, image, json.loads(result.stdout)


def approval(session, line, image, manifests):
    return rt.Approval(
        version=3,
        run_id=RUN_ID,
        nonce=NONCE,
        sha="c" * 40,
        started_at="2026-09-19T00:00:00Z",
        expires_at="2026-09-19T01:00:00Z",
        cleanup_at="2026-09-19T00:45:00Z",
        ceilings=rt.CEILINGS,
        namespaces={},
        images={"application": image},
        operator_uid="",
        baseline_uids=[],
        lock_owner={},
        runtime_sha256="d" * 64,
        application_sha256=rt.digest(manifests),
        scenario="cloudtasks",
        campaign=session["campaign"],
        flink_version=line,
        queue=rt.queue_name(RUN_ID),
        target=rt.CLOUDTASKS_POLICY["target"],
        cells=[
            {**cell, "manifest_sha256": rt.digest(manifest)}
            for cell, manifest in zip(session["cells"], manifests, strict=True)
        ],
        cloudtasks_ceilings=rt.CLOUDTASKS_CEILINGS,
        cloudtasks_pod_resources=rt.CLOUDTASKS_POD_RESOURCES,
    )


def test_rendered_cells_pass_the_runtime_manifest_contract_on_both_lines():
    for line in rt.FLINK_LINES:
        session, image, manifests = render(line)
        approved = approval(session, line, image, manifests)
        assert len(manifests) == len(session["cells"])
        for manifest, cell in zip(manifests, approved.cells, strict=True):
            assert (
                rt.validate_cell_manifest(manifest, cell, approved)
                == (rt.FLINK_LINES[line][0])
            )


def test_every_reviewed_session_renders_and_passes_the_contract():
    assert [path.name for path in SESSIONS] == [
        "calibration-1246-flink120.toml",
        "calibration-1246.toml",
        "example-wiring.toml",
    ]
    for path in SESSIONS:
        line = "1.20.4" if path.name.endswith("flink120.toml") else "2.2.1"
        session, image, manifests = render(line, path)
        approved = approval(session, line, image, manifests)
        for manifest, cell in zip(manifests, approved.cells, strict=True):
            rt.validate_cell_manifest(manifest, cell, approved)
