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
"""Identity and delivery of the installed Tier-3 package's source and policy."""

import ast
import hashlib
import json
from importlib.resources import files
from importlib.util import resolve_name

from .common import Failure

# The delivered ConfigMap has one consumer: the supervisor Pod, started with the
# fixed command `python3 -m flink_tier3 supervisor`. Delivering the whole package
# instead carried every module any scenario adds into all of them, and a Cloud
# Tasks session's manifest reached the Kubernetes ConfigMap data ceiling.
ENTRYPOINTS = ("__main__", "__init__", "cli")
# `cli` dispatches by name through `import_module`, which no import walk can
# follow, so `cli` reads this table rather than keeping its own copy.
DISPATCHED = {
    "supervisor": "runtime",
    "bigquery": "bigquery.oracle",
    "bigquery-bundle": "bigquery.bundle",
    "vm-analyze": "cloudtasks.vmanalyze",
}
# The Pod's command is fixed by the manifest. Only its module seeds the walk;
# naming a command the Pod never runs would carry that command's modules into
# every delivery, which is what this selection exists to stop.
POD_COMMANDS = ("supervisor",)
DATA_FILES = ("policy.toml",)


def package_sources(directory=None):
    directory = files("flink_tier3") if directory is None else directory

    def walk(directory, prefix=""):
        for path in sorted(directory.iterdir(), key=lambda path: path.name):
            name = prefix + path.name
            if path.is_dir():
                yield from walk(path, name + "/")
            elif path.is_file() and path.name.endswith((".py", ".toml")):
                yield name, path.read_bytes().decode("utf-8")

    return dict(walk(directory))


def _imported(source, module="__init__", modules=None):
    parent = module.rpartition(".")[0]
    package = "flink_tier3" + ("." + parent if parent else "")

    def local(name):
        return name == "flink_tier3" or name.startswith("flink_tier3.")

    def suffix(name):
        return (
            name.removeprefix("flink_tier3.") if name != "flink_tier3" else "__init__"
        )

    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.ImportFrom):
            name = "." * node.level + (node.module or "")
            name = resolve_name(name, package) if node.level else name
            if local(name):
                yield suffix(name)
                # Bare relative imports name modules in this package. Keep
                # their edges even when a truncated delivery lacks the file.
                # Items from an explicit module may instead be exported symbols.
                for alias in node.names:
                    child = suffix(name + "." + alias.name)
                    if node.module is None or modules is None or child in modules:
                        yield child
        elif isinstance(node, ast.Import):
            for alias in node.names:
                if local(alias.name):
                    yield suffix(alias.name)
        # Package data is opened by name, so a literal naming a file beside the
        # modules is the only edge to it. A delivery whose data stayed behind is
        # the one incompleteness both actors would compute the same digest over.
        elif (
            isinstance(node, ast.Constant)
            and isinstance(node.value, str)
            and node.value.endswith(".toml")
        ):
            yield (parent.replace(".", "/") + "/" if parent else "") + node.value


def delivered_sources(directory=None):
    """The modules the supervisor entrypoint can import, and its policy data.

    The set is closed under imports, so walking it from a delivery reproduces
    exactly the set the runner computed from a complete installation.
    """
    available = package_sources(directory)
    modules = {
        name.removesuffix(".py").replace("/", "."): name
        for name in available
        if name.endswith(".py")
    }
    modules.update(
        {
            name.removesuffix("/__init__.py").replace("/", "."): name
            for name in available
            if name.endswith("/__init__.py")
        }
    )
    reachable, data = set(), set(DATA_FILES)
    pending = [*ENTRYPOINTS, *(DISPATCHED.get(c, c) for c in POD_COMMANDS)]
    while pending:
        name = pending.pop()
        if name.endswith(".toml"):
            if name in available:
                data.add(name)
            continue
        if name in reachable:
            continue
        if name not in modules:
            raise Failure(
                "Delivered package is missing " + name.replace(".", "/") + ".py"
            )
        reachable.add(name)
        path = modules[name]
        # Importing a submodule executes every package initializer above it.
        parts = path.split("/")[:-1]
        for index in range(1, len(parts) + 1):
            initializer = "/".join(parts[:index]) + "/__init__.py"
            if initializer not in available:
                raise Failure("Delivered package is missing " + initializer)
            pending.append(".".join(parts[:index]) + ".__init__")
        module = path.removesuffix(".py").replace("/", ".")
        pending.extend(_imported(available[path], module, modules))
    names = {modules[name] for name in reachable} | data
    missing = sorted(names - set(available))
    if missing:
        raise Failure("Delivered package is missing " + ", ".join(missing))
    return {name: available[name] for name in sorted(names)}


def _digest(files):
    hashes = {
        "flink_tier3/" + name: hashlib.sha256(content.encode()).hexdigest()
        for name, content in files.items()
    }
    return hashlib.sha256(
        json.dumps(hashes, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def source_digest(directory=None):
    """The complete installed package, which the runner verifies against."""
    return _digest(package_sources(directory))


def delivery_digest(directory=None):
    """The subset the supervisor mounts, which is all it can verify itself."""
    return _digest(delivered_sources(directory))
