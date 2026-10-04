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
"""What every approval-bound service delivery shares; this does not authenticate."""

import os
import subprocess
from pathlib import Path

from . import repository
from .bundle import source_digest
from .common import Failure, json_bytes, timestamp, utc
from .model import Approval
from .policy import MIB


def check_approval(approval, prepared_at, scenario, service):
    """The separately supplied approval, at a whole second inside its window."""
    value = approval.to_dict() if isinstance(approval, Approval) else approval
    if not isinstance(value, dict):
        raise Failure("Approval must be a JSON object")
    when = timestamp(prepared_at)
    result = Approval.from_dict(value, when)
    if result.scenario != scenario:
        raise Failure(f"Execution bundles require a {service} approval")
    if when != int(when) or when < result.schedule.started:
        raise Failure(
            "Bundle preparation requires a whole second within the run window"
        )
    if result.runtime_sha256 != source_digest():
        raise Failure("Bundle source differs from approval")
    return result, when


def check_revision(revision):
    """Require the approved checkout and unchanged CUE inputs."""

    def git(*args):
        try:
            result = subprocess.run(
                ["git", "--no-optional-locks", *args],
                cwd=repository.ROOT,
                env={
                    key: value
                    for key, value in os.environ.items()
                    if not key.startswith("GIT_")
                },
                capture_output=True,
                text=True,
                timeout=60,
                check=False,
            )
        except (OSError, subprocess.TimeoutExpired) as error:
            raise Failure(
                "Cannot verify the approved repository revision: " + str(error)
            ) from error
        if result.returncode:
            raise Failure(
                "Cannot verify the approved repository revision: "
                + result.stderr[-1000:].strip()
            )
        return result.stdout

    if git("rev-parse", "HEAD").strip() != revision:
        raise Failure("Repository revision differs from approval")
    if git("diff", "--name-only", "HEAD", "--", "kubernetes"):
        raise Failure("Approved Kubernetes inputs have local changes")
    # Include ignored files: Git's ordinary status hides them, but CUE may load
    # a CUE file, and dispatch reads a reviewed trial file by name.
    others = git("ls-files", "--others", "-z", "--", "kubernetes").split("\0")
    if any(
        path.endswith((".cue", ".toml")) or "cue.mod" in Path(path).parts
        for path in others
    ):
        raise Failure("Approved Kubernetes inputs contain untracked CUE or TOML files")


def bind(kind, approved, bundle, when):
    """Embed the approval in a re-rendered, already compared bundle."""
    # These two fields vary after approval; every other rendered field is retained.
    bundle["delivery"]["config"]["data"]["approval.json"] = json_bytes(
        approved.to_dict()
    ).decode()
    bundle["delivery"]["supervisor"]["spec"]["activeDeadlineSeconds"] = (
        approved.schedule.active_seconds(when)
    )
    data = bundle["delivery"]["config"]["data"]
    if (
        sum(len(key.encode()) + len(value.encode()) for key, value in data.items())
        > MIB
    ):
        raise Failure("Approved ConfigMap exceeds the Kubernetes data size limit")
    return {
        "kind": kind,
        "version": 1,
        "admission_enabled": False,
        "prepared_at": utc(when),
        "approval": approved.to_dict(),
        **bundle,
    }


def validate(prepare, bundle, approval):
    """Re-render against the caller's approval; embedded approval is not authority."""
    if not isinstance(bundle, dict) or not isinstance(bundle.get("prepared_at"), str):
        raise Failure("Bundle must contain its preparation time")
    expected = prepare(approval, prepared_at=bundle["prepared_at"])
    if json_bytes(bundle) != json_bytes(expected):
        raise Failure(
            "Bundle differs from the separately supplied approval and rendering"
        )
    return expected
