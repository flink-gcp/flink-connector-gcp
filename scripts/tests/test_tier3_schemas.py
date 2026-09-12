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

"""Synthetic chart tests for the Tier-3 schema generator."""

import hashlib
import importlib.util
import io
import json
import subprocess
import sys
import tarfile
from pathlib import Path

import pytest
import yaml

SPEC = importlib.util.spec_from_file_location(
    "tier3_schemas", Path(__file__).parents[1] / "tier3-schemas.py"
)
SCHEMAS = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(SCHEMAS)


def chart_bytes(
    *, version="1.15.0", app_version="1.15.0", identity=None, link=False, missing=False
):
    files = {
        "flink-kubernetes-operator/Chart.yaml": {
            "version": version,
            "appVersion": app_version,
        }
    }
    for plural, kind in SCHEMAS.CRDS.items():
        if missing and plural == "flinkdeployments":
            continue
        name = f"{plural}.flink.apache.org"
        files[f"flink-kubernetes-operator/crds/{name}-v1.yml"] = {
            "apiVersion": "apiextensions.k8s.io/v1",
            "kind": "CustomResourceDefinition",
            "metadata": {"name": name},
            "spec": {
                "group": "flink.apache.org",
                "names": {"kind": kind},
                "versions": [
                    {
                        "name": "v1beta1",
                        "schema": {
                            "openAPIV3Schema": {
                                "type": "object",
                                "x-kubernetes-validations": [
                                    {"rule": "self == oldSelf"}
                                ],
                                "properties": {
                                    "spec": {
                                        "x-kubernetes-preserve-unknown-fields": True
                                    }
                                },
                            }
                        },
                    }
                ],
            },
        }
    if identity:
        location, value = identity
        target = files[
            "flink-kubernetes-operator/crds/flinkdeployments.flink.apache.org-v1.yml"
        ]
        for key in location[:-1]:
            target = target[key]
        target[location[-1]] = value
    output = io.BytesIO()
    with tarfile.open(fileobj=output, mode="w:gz") as archive:
        for name, document in files.items():
            entry = tarfile.TarInfo(name)
            payload = yaml.safe_dump(document).encode()
            entry.size = len(payload)
            if link and "flinkdeployments.flink.apache.org-v1.yml" in name:
                target = tarfile.TarInfo("flink-kubernetes-operator/payload.yml")
                target.size = len(payload)
                archive.addfile(target, io.BytesIO(payload))
                entry.type = tarfile.SYMTYPE
                entry.size = 0
                entry.linkname = "../payload.yml"
            archive.addfile(entry, io.BytesIO(payload))
    return output.getvalue()


def pin(data):
    return {"version": "1.15.0", "sha512": hashlib.sha512(data).hexdigest()}


def test_complete_crd_payload_is_preserved():
    data = chart_bytes()
    documents = SCHEMAS.read_chart(data, pin(data))
    assert set(documents) == {f"{name}.flink.apache.org" for name in SCHEMAS.CRDS}
    for document in documents.values():
        schema = document["spec"]["versions"][0]["schema"]["openAPIV3Schema"]
        assert schema["x-kubernetes-validations"] == [{"rule": "self == oldSelf"}]
        assert (
            schema["properties"]["spec"]["x-kubernetes-preserve-unknown-fields"] is True
        )


def test_hash_mismatch_is_rejected_before_parsing():
    with pytest.raises(ValueError, match="SHA-512"):
        SCHEMAS.read_chart(b"not a tar file", {"sha512": "0" * 128})


@pytest.mark.parametrize(
    "changes",
    [
        {"version": "1.14.0"},
        {"app_version": "1.14.0"},
        {"missing": True},
    ],
)
def test_mismatched_or_incomplete_chart_is_rejected(changes):
    data = chart_bytes(**changes)
    with pytest.raises((ValueError, KeyError)):
        SCHEMAS.read_chart(data, pin(data))


def test_symlink_to_valid_crd_is_rejected():
    data = chart_bytes(link=True)
    name = "flinkdeployments.flink.apache.org"
    with tarfile.open(fileobj=io.BytesIO(data), mode="r:gz") as chart:
        document = yaml.safe_load(
            chart.extractfile(f"flink-kubernetes-operator/crds/{name}-v1.yml")
        )
        assert document["metadata"]["name"] == name
    with pytest.raises(ValueError, match="Expected an ordinary CRD file"):
        SCHEMAS.read_chart(data, pin(data))


@pytest.mark.parametrize(
    "location,value",
    [
        (("apiVersion",), "apiextensions.k8s.io/v1beta1"),
        (("kind",), "Unrelated"),
        (("metadata", "name"), "renamed.flink.apache.org"),
        (("spec", "group"), "example.org"),
        (("spec", "names", "kind"), "Unrelated"),
    ],
)
def test_each_crd_identity_field_is_checked(location, value):
    data = chart_bytes(identity=(location, value))
    with pytest.raises(ValueError, match="Unexpected CRD identity"):
        SCHEMAS.read_chart(data, pin(data))


@pytest.mark.parametrize("length", [7, 8, 9])
def test_download_limit_is_distinct_from_a_hash_error(monkeypatch, length):
    monkeypatch.setattr(SCHEMAS, "MAX_CHART_BYTES", 8)
    monkeypatch.setattr(
        SCHEMAS.urllib.request, "urlopen", lambda *a, **kw: io.BytesIO(b"a" * length)
    )
    if length <= 8:
        assert SCHEMAS.download_chart("https://example.org/chart") == b"a" * length
    else:
        with pytest.raises(ValueError, match="exceeds the 8-byte limit"):
            SCHEMAS.download_chart("https://example.org/chart")


@pytest.fixture
def cli(tmp_path, monkeypatch):
    chart = tmp_path / "chart.tgz"
    data = chart_bytes()
    chart.write_bytes(data)
    module = tmp_path / "kubernetes"
    module.mkdir()
    (module / "upstream.toml").write_text(
        "[operator]\n"
        + "\n".join(f"{key} = {json.dumps(value)}" for key, value in pin(data).items())
    )
    monkeypatch.setattr(SCHEMAS, "ROOT", tmp_path)
    expected = {
        Path("v1beta1/resource.cue"): b"package v1beta1\n#Resource: {}\n",
    }

    def generate(documents, output, cue, upstream):
        for path, content in expected.items():
            file = output / path
            file.parent.mkdir(parents=True, exist_ok=True)
            file.write_bytes(content)
        return "# generated\n"

    monkeypatch.setattr(SCHEMAS, "generate", generate)
    actual = tmp_path / SCHEMAS.GENERATED
    generate({}, actual, "unused", {})

    def invoke(mode):
        monkeypatch.setattr(
            sys, "argv", ["tier3-schemas.py", mode, "--chart", str(chart)]
        )
        SCHEMAS.main()

    invoke("refresh")
    return invoke, actual, expected


def test_check_accepts_matching_output_without_writing(cli):
    invoke, actual, expected = cli
    before = {path: (actual / path).stat().st_mtime_ns for path in expected}
    invoke("check")
    assert SCHEMAS.generated_files(actual) == expected
    assert {path: (actual / path).stat().st_mtime_ns for path in expected} == before


@pytest.mark.parametrize("change", ["changed", "missing", "stale"])
def test_check_rejects_each_difference_without_repairing(cli, change, capsys):
    invoke, actual, expected = cli
    file = actual / next(iter(expected))
    if change == "changed":
        file.write_text("package changed\n")
    elif change == "missing":
        file.unlink()
    else:
        (actual / "stale.cue").write_text("package stale\n")
    before = SCHEMAS.generated_files(actual)
    with pytest.raises(SystemExit) as error:
        invoke("check")
    assert error.value.code == 1
    assert "Generated schemas differ" in capsys.readouterr().err
    assert SCHEMAS.generated_files(actual) == before


def test_refresh_replaces_generated_files_and_preserves_other_sources(cli):
    invoke, actual, expected = cli
    (actual / next(iter(expected))).write_text("package changed\n")
    (actual / "stale.cue").write_text("package stale\n")
    note = actual / "README.md"
    note.write_text("Keep this note.\n")
    handwritten = SCHEMAS.ROOT / "kubernetes/common.cue"
    handwritten.write_text("package tier3\n")
    invoke("refresh")
    assert SCHEMAS.generated_files(actual) == expected
    assert note.read_text() == "Keep this note.\n"
    assert handwritten.read_text() == "package tier3\n"


def test_generation_failure_does_not_touch_existing_sources(cli, monkeypatch):
    invoke, actual, expected = cli

    def fail(*args):
        raise subprocess.CalledProcessError(1, "cue")

    monkeypatch.setattr(SCHEMAS, "generate", fail)
    with pytest.raises(subprocess.CalledProcessError):
        invoke("refresh")
    assert SCHEMAS.generated_files(actual) == expected


@pytest.mark.parametrize("stage", ["get", "fmt"])
def test_each_cue_failure_is_propagated(tmp_path, monkeypatch, stage):
    def run(command, *, check, cwd=None):
        if command[1] == stage:
            if check:
                raise subprocess.CalledProcessError(7, command)
            return subprocess.CompletedProcess(command, 7)
        if command[1] == "get":
            directory = cwd / "v1beta1"
            directory.mkdir()
            (directory / "resource.cue").write_text("package v1beta1\n")
        return subprocess.CompletedProcess(command, 0)

    monkeypatch.setattr(SCHEMAS.subprocess, "run", run)
    data = chart_bytes()
    upstream = pin(data)
    documents = SCHEMAS.read_chart(data, upstream)
    with pytest.raises(subprocess.CalledProcessError) as error:
        SCHEMAS.generate(documents, tmp_path / "generated", "cue", upstream)
    assert error.value.returncode == 7
    assert error.value.cmd[1] == stage


@pytest.mark.parametrize("change", ["changed", "missing", "stale"])
def test_crd_payload_changes_are_detected_and_refreshable(cli, change):
    invoke, _, _ = cli
    directory = SCHEMAS.ROOT / SCHEMAS.CRD_GENERATED
    original = {p.name: p.read_bytes() for p in directory.glob("*.yaml")}
    file = directory / next(iter(original))
    if change == "changed":
        file.write_text("kind: Changed\n")
    elif change == "missing":
        file.unlink()
    else:
        (directory / "stale.yaml").write_text("kind: Stale\n")
    changed = {p.name: p.read_bytes() for p in directory.glob("*.yaml")}
    with pytest.raises(SystemExit):
        invoke("check")
    assert {p.name: p.read_bytes() for p in directory.glob("*.yaml")} == changed
    invoke("refresh")
    assert {p.name: p.read_bytes() for p in directory.glob("*.yaml")} == original
