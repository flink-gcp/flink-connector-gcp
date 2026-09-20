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
"""Exercise the built artifact and installed command outside the checkout."""

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest
from flink_tier3.bundle import package_sources, source_digest

ROOT = Path(__file__).resolve().parents[3]


def run(*args, cwd, env=None):
    return subprocess.run(
        args, cwd=cwd, env=env, capture_output=True, text=True, timeout=120, check=True
    )


def test_wheel_installs_cli_policy_and_complete_source_bundle(tmp_path):
    distribution = tmp_path / "dist"
    run(
        "uv",
        "build",
        "--package",
        "flink-tier3",
        "--out-dir",
        str(distribution),
        cwd=ROOT,
    )
    [wheel] = distribution.glob("*.whl")
    assert len(list(distribution.glob("*.tar.gz"))) == 1
    installed = tmp_path / "installed"
    run(
        "uv",
        "pip",
        "install",
        "--python",
        sys.executable,
        "--no-deps",
        "--target",
        str(installed),
        str(wheel),
        cwd=tmp_path,
    )
    # Reuse the suite's SDKs, but load the application only from the installed wheel.
    environment = {
        **os.environ,
        "PYTHONPATH": str(installed),
        "PYTHONDONTWRITEBYTECODE": "1",
    }
    command = installed / "bin/flink-tier3"
    for arguments in (
        [],
        ["lifecycle"],
        ["bootstrap"],
        ["schemas"],
        ["supervisor"],
        ["render"],
        ["analyze"],
        ["bigquery"],
    ):
        result = run(str(command), *arguments, "--help", cwd=tmp_path, env=environment)
        assert (
            "usage: flink-tier3" + (" " + arguments[0] if arguments else "")
            in result.stdout
        )
    observed = run(
        sys.executable,
        "-c",
        """
import json
from pathlib import Path
import flink_tier3
from flink_tier3.bundle import package_sources, source_digest
from flink_tier3.policy import CEILINGS
print(json.dumps({"file": str(Path(flink_tier3.__file__).resolve()),
                  "sources": package_sources(), "digest": source_digest(),
                  "pods": CEILINGS["pods"]}))
""",
        cwd=tmp_path,
        env=environment,
    )
    payload = json.loads(observed.stdout)
    assert Path(payload["file"]).is_relative_to(installed)
    assert payload["sources"] == package_sources()
    assert payload["digest"] == source_digest()
    assert payload["pods"] == 4


def test_repository_argument_routes_bootstrap_without_changing_relative_paths(
    monkeypatch, tmp_path
):
    from flink_tier3 import bootstrap, cli, lifecycle, schemas, workflow

    checkout = tmp_path / "checkout"
    (checkout / "kubernetes/cue.mod").mkdir(parents=True)
    (checkout / "kubernetes/cue.mod/module.cue").touch()
    (checkout / "opentofu").mkdir()
    for module in (bootstrap, lifecycle, schemas, workflow):
        monkeypatch.setattr(module, "ROOT", module.ROOT)
    monkeypatch.chdir(tmp_path)
    seen = []

    def authenticate(cluster):
        seen.append((cluster.root_path, cluster.kubeconfig))

    monkeypatch.setattr(bootstrap.Cluster, "authenticate", authenticate)
    assert (
        cli.main(
            [
                "--repository",
                str(checkout),
                "bootstrap",
                "--kubeconfig",
                "config",
                "auth",
            ]
        )
        == 0
    )
    assert seen == [(checkout / "opentofu/tier3-bootstrap", tmp_path / "config")]


def test_repository_rejected_before_cloud_access(monkeypatch, tmp_path, capsys):
    from flink_tier3 import cli, lifecycle

    monkeypatch.setattr(lifecycle.rt, "Storage", lambda: pytest.fail("No cloud access"))
    with pytest.raises(SystemExit) as error:
        cli.main(
            [
                "--repository",
                str(tmp_path),
                "lifecycle",
                "lock",
                "release",
                "--file",
                "owner.json",
            ]
        )
    assert error.value.code == 2
    assert "--repository" in capsys.readouterr().err


def test_source_identity_preserves_file_bytes(tmp_path):
    source = tmp_path / "runtime.py"
    source.write_bytes(b"# Reviewed source.\n")
    original = source_digest(tmp_path)
    source.write_bytes(b"# Reviewed source.\r\n")
    assert source_digest(tmp_path) != original
    assert package_sources(tmp_path)["runtime.py"].encode() == source.read_bytes()
