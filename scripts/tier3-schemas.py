# /// script
# requires-python = ">=3.11"
# dependencies = ["pyyaml==6.0.3"]
# ///
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

"""Regenerate the pinned Operator CRD data and CUE validation packages.

Both outputs come from one checksum-verified chart. This command performs no
Kubernetes or GCP operations. The check mode writes only to a temporary directory.
"""

import argparse
import hashlib
import io
import json
import subprocess
import tarfile
import tempfile
import tomllib
import urllib.request
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parent.parent
GENERATED = Path("kubernetes/gen/flink")
CRDS = {
    "flinkbluegreendeployments": "FlinkBlueGreenDeployment",
    "flinkdeployments": "FlinkDeployment",
    "flinksessionjobs": "FlinkSessionJob",
    "flinkstatesnapshots": "FlinkStateSnapshot",
}
MAX_CHART_BYTES = 2 * 1024 * 1024


def download_chart(url):
    """Bound downloads without presenting truncation as a checksum mismatch."""
    with urllib.request.urlopen(url, timeout=60) as response:
        data = response.read(MAX_CHART_BYTES + 1)
    if len(data) > MAX_CHART_BYTES:
        raise ValueError(f"Operator chart exceeds the {MAX_CHART_BYTES}-byte limit")
    return data


def read_chart(data, pin):
    """Validate the downloaded bytes and return the complete CRD documents."""
    if hashlib.sha512(data).hexdigest() != pin["sha512"]:
        raise ValueError(
            "Operator chart SHA-512 does not match kubernetes/upstream.toml"
        )
    result = {}
    with tarfile.open(fileobj=io.BytesIO(data), mode="r:gz") as chart:
        version = yaml.safe_load(
            chart.extractfile("flink-kubernetes-operator/Chart.yaml")
        )
        if (
            version["version"] != pin["version"]
            or version["appVersion"] != pin["version"]
        ):
            raise ValueError("Operator chart version does not match its pin")
        for plural, kind in CRDS.items():
            name = f"{plural}.flink.apache.org"
            path = f"flink-kubernetes-operator/crds/{name}-v1.yml"
            member = chart.getmember(path)
            if not member.isfile():
                raise ValueError(f"Expected an ordinary CRD file: {path}")
            source = chart.extractfile(member).read().decode()
            document = yaml.safe_load(source)
            if (
                document.get("apiVersion") != "apiextensions.k8s.io/v1"
                or document.get("kind") != "CustomResourceDefinition"
                or document["metadata"]["name"] != name
                or document["spec"]["group"] != "flink.apache.org"
                or document["spec"]["names"]["kind"] != kind
            ):
                raise ValueError(f"Unexpected CRD identity: {path}")
            result[name] = document
    return result


def generate(documents, output, cue, pin):
    """Generate in isolation so stale version packages cannot survive a refresh."""
    output.mkdir(parents=True)
    payload = output.parent / "crds.json"
    # cue get crd accepts separate CRD documents rather than a JSON array.
    sources = []
    for name, document in sorted(documents.items()):
        source = output.parent / f"{name}.json"
        source.write_text(json.dumps(document))
        sources.append(str(source))
    subprocess.run(
        [cue, "get", "crd", "--group", "flink.apache.org", *sources],
        cwd=output,
        check=True,
    )
    data_dir = output / "crds"
    data_dir.mkdir()
    payload.write_text(json.dumps({"objects": documents}))
    subprocess.run(
        [
            cue,
            "import",
            "json:",
            str(payload),
            "-p",
            "crds",
            "-o",
            str(data_dir / "crds.cue"),
        ],
        check=True,
    )
    header = (
        "// Generated from the Apache Flink Kubernetes Operator "
        + pin["version"]
        + " chart.\n"
        "// Regenerate with just tier3-schemas refresh; do not edit by hand.\n"
        "// Licensed to the Apache Software Foundation (ASF) under one\n"
        "// or more contributor license agreements. See the NOTICE file\n"
        "// distributed with this work for additional information\n"
        "// regarding copyright ownership. The ASF licenses this file\n"
        "// to you under the Apache License, Version 2.0 (the\n"
        '// "License"); you may not use this file except in compliance\n'
        "// with the License. You may obtain a copy of the License at\n"
        "//\n"
        "//     http://www.apache.org/licenses/LICENSE-2.0\n"
        "//\n"
        "// Unless required by applicable law or agreed to in writing, software\n"
        '// distributed under the License is distributed on an "AS IS" BASIS,\n'
        "// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.\n"
        "// See the License for the specific language governing permissions and\n"
        "// limitations under the License.\n\n"
    )
    for file in output.rglob("*.cue"):
        file.write_text(header + file.read_text())
    subprocess.run([cue, "fmt", "./..."], cwd=output, check=True)


def generated_files(directory):
    return {p.relative_to(directory): p.read_bytes() for p in directory.rglob("*.cue")}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("check", "refresh"))
    parser.add_argument(
        "--chart",
        type=Path,
        help="Use an already downloaded chart (checksum still checked)",
    )
    parser.add_argument("--cue", default="cue")
    args = parser.parse_args()
    pin = tomllib.loads((ROOT / "kubernetes/upstream.toml").read_text())["operator"]
    if args.chart:
        data = args.chart.read_bytes()
    else:
        data = download_chart(pin["url"])
    documents = read_chart(data, pin)
    with tempfile.TemporaryDirectory(prefix="tier3-schemas-") as temporary:
        output = Path(temporary) / "flink"
        generate(documents, output, args.cue, pin)
        expected = generated_files(output)
        actual = generated_files(ROOT / GENERATED)
        if args.mode == "check":
            differences = sorted(
                path
                for path in expected.keys() | actual.keys()
                if expected.get(path) != actual.get(path)
            )
            if differences:
                parser.exit(
                    1,
                    "Generated CUE differs; run just tier3-schemas refresh:\n"
                    + "\n".join(map(str, differences))
                    + "\n",
                )
            print("Operator CRD data and validation packages match the pinned chart.")
        else:
            for path in actual.keys() - expected.keys():
                (ROOT / GENERATED / path).unlink()
            for path, content in expected.items():
                destination = ROOT / GENERATED / path
                destination.parent.mkdir(parents=True, exist_ok=True)
                destination.write_bytes(content)
            print(
                f"Regenerated {len(expected)} CUE files from Operator {pin['version']}."
            )


if __name__ == "__main__":
    main()
