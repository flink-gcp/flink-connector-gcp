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
from flink_tier3 import bundle
from flink_tier3.bundle import (
    delivered_sources,
    delivery_digest,
    package_sources,
    source_digest,
)
from flink_tier3.common import Failure

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
        ["bigquery-bundle"],
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
    assert payload["pods"] == 5


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


def minimal_package(directory):
    """The smallest delivery the supervisor entrypoint can be identified by.

    The entrypoints carry real imports, including a deferred one, so the walk
    under test is exercised rather than a package with no edges at all.
    """
    (directory / "__init__.py").write_bytes(b"from flink_tier3 import common\n")
    (directory / "__main__.py").write_bytes(b"from .cli import main\n")
    (directory / "cli.py").write_bytes(b"from importlib import import_module\n")
    (directory / "common.py").write_bytes(b"# Reviewed source.\n")
    (directory / "helper.py").write_bytes(b"# Reviewed source.\n")
    (directory / "runtime.py").write_bytes(
        b"def main():\n    from .helper import used  # noqa: F401\n"
    )
    (directory / "policy.toml").write_bytes(b"# Reviewed policy.\n")
    return directory / "runtime.py"


def test_source_identity_preserves_file_bytes(tmp_path):
    source = minimal_package(tmp_path)
    original = source_digest(tmp_path)
    source.write_bytes(b"# Reviewed source.\r\n")
    assert source_digest(tmp_path) != original
    assert package_sources(tmp_path)["runtime.py"].encode() == source.read_bytes()


def test_delivery_is_closed_under_imports_and_refuses_a_truncated_package(tmp_path):
    minimal_package(tmp_path)
    (tmp_path / "unused.py").write_bytes(b"# Imported by nothing.\n")
    assert set(delivered_sources(tmp_path)) == {
        "__init__.py",
        "__main__.py",
        "cli.py",
        "common.py",
        "helper.py",
        "runtime.py",
        "policy.toml",
    }
    # Walking a delivery reproduces itself, which is what lets the supervisor
    # verify its mount against a digest the runner computed before the run.
    delivery = tmp_path / "delivered"
    delivery.mkdir()
    for name, content in delivered_sources(tmp_path).items():
        (delivery / name).write_text(content)
    assert delivered_sources(delivery) == delivered_sources(tmp_path)
    assert delivery_digest(delivery) == delivery_digest(tmp_path)
    (delivery / "policy.toml").unlink()
    with pytest.raises(Failure, match="missing policy.toml"):
        delivered_sources(delivery)


def test_delivery_follows_a_module_to_the_package_data_it_names(tmp_path):
    minimal_package(tmp_path)
    (tmp_path / "rules.toml").write_bytes(b"# Reviewed rules.\n")
    (tmp_path / "reader.py").write_bytes(b'RULES = "rules.toml"\n')
    assert "rules.toml" not in delivered_sources(tmp_path)
    runtime = tmp_path / "runtime.py"
    runtime.write_bytes(runtime.read_bytes() + b"from . import reader  # noqa: F401\n")
    # Both actors would agree on a delivery whose data stayed behind, so the
    # digest cannot catch this one; following the literal is what does.
    assert "rules.toml" in delivered_sources(tmp_path)
    assert "reader.py" in delivered_sources(tmp_path)


def test_delivery_follows_the_declared_command_dispatch(tmp_path):
    from flink_tier3 import cli

    minimal_package(tmp_path)
    # `cli` reaches the Pod's module only through a computed name, so the
    # declared edge is the only thing that keeps it in the delivery.
    assert "runtime.py" in delivered_sources(tmp_path)
    assert bundle.POD_COMMANDS == ("supervisor",)
    assert bundle.DISPATCHED["supervisor"] == "runtime"
    # The CLI resolves the command through this same table, not a second copy.
    assert cli.DISPATCHED is bundle.DISPATCHED
    # And the command seeded here is the one the manifest actually runs.
    manifest = (ROOT / "kubernetes/lifecycle/delivery.cue").read_text()
    for command in bundle.POD_COMMANDS:
        assert f'"flink_tier3", "{command}"' in manifest
