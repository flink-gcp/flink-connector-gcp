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
"""Fault injection for lifecycle admission, settlement, ownership and release."""

import base64
import copy
import hashlib
import io
import json
import subprocess
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

import google_crc32c
import pytest
import requests
import urllib3
import yaml
from flink_tier3 import lifecycle as cli
from google.cloud import storage

rt = cli.rt
ROOT = Path(__file__).resolve().parents[3]


class Clock:
    def __init__(self):
        self.now = rt.timestamp("2026-09-15T00:00:00Z")
        self.sleeps = []

    def __call__(self):
        return self.now

    def sleep(self, seconds):
        assert 0 < seconds <= 45
        self.sleeps.append(seconds)
        self.now += seconds


class Store:
    """In-memory storage: JSON documents in ``data``, raw bytes in ``blobs``.

    Both tables share one generation counter and one namespace per bucket, as
    the service does; ``blobs`` carries the gzip parts and receipts the
    measurement application writes, ``data`` the lifecycle's JSON records.
    """

    def __init__(self):
        self.data = {}
        self.blobs = {}
        self.serial = 0
        self.fail_evidence = False
        self.before_write = None
        self.conflicts = 0
        self.created = "2026-09-15T00:00:00Z"

    def _generation(self, name, bucket):
        if (bucket, name) in self.data:
            return self.data[bucket, name][1]
        if (bucket, name) in self.blobs:
            return self.blobs[bucket, name][1]
        return "0"

    def _bytes(self, name, bucket):
        if (bucket, name) in self.blobs:
            return self.blobs[bucket, name][0]
        if (bucket, name) in self.data:
            return rt.json_bytes(self.data[bucket, name][0])
        return None

    def read(self, name, bucket=rt.EVIDENCE):
        if (bucket, name) in self.blobs:
            data, generation = self.blobs[bucket, name]
            return json.loads(data), generation
        return copy.deepcopy(self.data.get((bucket, name), (None, "0")))

    def write(self, name, data, generation="0", bucket=rt.EVIDENCE):
        if self.before_write:
            callback, self.before_write = self.before_write, None
            callback()
        if self.fail_evidence and name.startswith("runs/"):
            raise rt.Failure("Evidence unavailable")
        if str(generation) != self._generation(name, bucket):
            self.conflicts += 1
            raise rt.ApiError(412, "POST", name)
        self.serial += 1
        self.blobs.pop((bucket, name), None)
        self.data[bucket, name] = copy.deepcopy(data), str(self.serial)
        return str(self.serial)

    def write_bytes(self, name, data, bucket=rt.EVIDENCE, generation="0"):
        if str(generation) != self._generation(name, bucket):
            raise rt.ApiError(412, "POST", name)
        self.serial += 1
        self.data.pop((bucket, name), None)
        self.blobs[bucket, name] = bytes(data), str(self.serial)
        return str(self.serial)

    def delete(self, name, generation, bucket=rt.EVIDENCE):
        if str(generation) != self._generation(name, bucket):
            raise rt.ApiError(412, "DELETE", name)
        self.data.pop((bucket, name), None)
        self.blobs.pop((bucket, name), None)

    def objects(self, prefix, bucket=rt.EVIDENCE, maximum=20000):
        result = [
            {
                "name": name,
                "generation": generation,
                "size": str(len(rt.json_bytes(data))),
                "created": self.created,
            }
            for (b, name), (data, generation) in self.data.items()
            if b == bucket and name.startswith(prefix)
        ] + [
            {
                "name": name,
                "generation": generation,
                "size": str(len(data)),
                "created": self.created,
            }
            for (b, name), (data, generation) in self.blobs.items()
            if b == bucket and name.startswith(prefix)
        ]
        if len(result) > maximum:
            raise rt.Failure("Object inventory exceeds ceiling")
        return result

    def prefixes(self, prefix, bucket=rt.EVIDENCE, maximum=20000):
        names = [b_name for b, b_name in self.data if b == bucket] + [
            b_name for b, b_name in self.blobs if b == bucket
        ]
        result = set()
        for name in names:
            if name.startswith(prefix) and "/" in name[len(prefix) :]:
                result.add(prefix + name[len(prefix) :].split("/", 1)[0] + "/")
        if len(result) > maximum:
            raise rt.Failure("Prefix inventory exceeds ceiling")
        return result

    def metadata(self, name, bucket=rt.EVIDENCE):
        data = self._bytes(name, bucket)
        if data is None:
            return None
        return {
            "size": str(len(data)),
            "crc32c": base64.b64encode(
                google_crc32c.value(data).to_bytes(4, "big")
            ).decode(),
            "md5": base64.b64encode(hashlib.md5(data).digest()).decode(),
            "generation": self._generation(name, bucket),
            "created": self.created,
        }

    def open(self, name, generation, bucket=rt.EVIDENCE, chunk_size=None):
        data = self._bytes(name, bucket)
        if data is None:
            raise rt.ApiError(404, "GET", name)
        if str(generation) != self._generation(name, bucket):
            raise rt.ApiError(412, "GET", name)
        return io.BytesIO(data)

    def rewrite(
        self,
        source_bucket,
        source_name,
        source_generation,
        destination_name,
        destination_bucket=rt.EVIDENCE,
    ):
        data = self._bytes(source_name, source_bucket)
        if data is None:
            raise rt.ApiError(404, "POST", destination_name)
        if str(source_generation) != self._generation(source_name, source_bucket):
            raise rt.ApiError(412, "POST", destination_name)
        if self._generation(destination_name, destination_bucket) != "0":
            return self.metadata(destination_name, destination_bucket)
        return self.write_bytes(destination_name, data, destination_bucket)


def obj(kind, name, namespace=rt.SMOKE, owner=None):
    result = {
        "apiVersion": "v1",
        "kind": kind,
        "metadata": {
            "namespace": namespace,
            "name": name,
            "uid": name + "-uid",
            "resourceVersion": "1",
        },
        "spec": {},
        "status": {},
    }
    if owner:
        result["metadata"]["ownerReferences"] = [{"uid": owner}]
    return result


class Kube:
    def __init__(self):
        self.data = {}
        self.calls = []
        self.normal_cleanup = True
        self.replace_before_delete = None
        self.fail_create_response = False

    def put(self, value):
        self.data[
            value["kind"], value["metadata"]["namespace"], value["metadata"]["name"]
        ] = copy.deepcopy(value)
        return copy.deepcopy(value)

    def namespace(self, namespace):
        return {"metadata": {"uid": namespace + "-uid"}}

    def get(self, kind, namespace, name):
        return copy.deepcopy(self.data.get((kind, namespace, name)))

    def inventory(self, application=None):
        from flink_tier3.policy import inventory_namespaces

        return copy.deepcopy(
            [
                value
                for value in self.data.values()
                if value["kind"] != "ResourceQuota"
                and (
                    application is None
                    or value["metadata"]["namespace"]
                    in inventory_namespaces(application)
                )
            ]
        )

    def items(self, kind, namespace):
        return [
            item
            for item in self.inventory()
            if item["kind"] == kind and item["metadata"]["namespace"] == namespace
        ]

    def patch(self, value, changes, subresource=""):
        assert not subresource
        current = self.get(
            value["kind"], value["metadata"]["namespace"], value["metadata"]["name"]
        )
        assert current["metadata"]["uid"] == value["metadata"]["uid"]
        self.calls.append(("patch", value["kind"], value["metadata"]["uid"], changes))
        for change in changes:
            if change["path"] == "/spec/hard":
                current["spec"]["hard"] = change["value"]
                current["status"]["hard"] = change["value"]
                current["status"]["used"] = {"pods": "0"}
            elif change["path"] == "/metadata/finalizers":
                current["metadata"]["finalizers"] = change["value"]
                self.data.pop(
                    (
                        current["kind"],
                        current["metadata"]["namespace"],
                        current["metadata"]["name"],
                    )
                )
                return current
            else:
                raise AssertionError(change)
        return self.put(current)

    def delete(self, value, force=False):
        assert not force, "API disappearance is not process termination"
        key = value["kind"], value["metadata"]["namespace"], value["metadata"]["name"]
        if self.replace_before_delete:
            self.data[key]["metadata"]["uid"] = "replacement"
            self.replace_before_delete = False
        if key not in self.data:
            return False
        if self.data[key]["metadata"]["uid"] != value["metadata"]["uid"]:
            raise rt.ApiError(409, "DELETE", key[-1])
        self.calls.append(("delete", value["kind"], value["metadata"]["uid"]))
        if value["kind"] == "FlinkDeployment" and not self.normal_cleanup:
            return True
        owned = rt.ownership(self.inventory(), {value["metadata"]["uid"]})
        self.data = {
            key: item
            for key, item in self.data.items()
            if item["metadata"]["uid"] not in owned
        }
        return True

    def create(self, value, dry_run=False):
        self.calls.append(("create", value["kind"], dry_run))
        if dry_run:
            return value
        value = copy.deepcopy(value)
        value["metadata"]["uid"] = value["metadata"]["name"] + "-uid"
        value["metadata"]["resourceVersion"] = "1"
        self.put(value)
        if self.fail_create_response:
            raise rt.Failure("Lost response after creation")
        return value

    def path(self, kind, namespace, name):
        return f"/{namespace}/{kind}/{name}"

    def request(self, method, path, body=None, **_kwargs):
        assert path.endswith("/scale")
        current = self.get("Deployment", rt.SYSTEM, rt.OPERATOR)
        spec = (
            {"replicas": current["spec"]["replicas"]}
            if current["spec"]["replicas"]
            else {}
        )
        if method == "GET":
            return {
                "apiVersion": "autoscaling/v1",
                "kind": "Scale",
                "metadata": current["metadata"],
                "spec": spec,
            }
        assert method == "PATCH"
        assert body[:2] == [
            {
                "op": "test",
                "path": "/metadata/uid",
                "value": current["metadata"]["uid"],
            },
            {
                "op": "test",
                "path": "/metadata/resourceVersion",
                "value": current["metadata"]["resourceVersion"],
            },
        ]
        assert body[-1]["path"] == "/spec/replicas"
        assert body[-1]["op"] in ("add", "replace")
        if body[-1]["op"] == "replace" and "replicas" not in spec:
            raise rt.ApiError(422, method, path)
        replicas = body[-1]["value"]
        self.calls.append(("scale", replicas))
        current["spec"]["replicas"] = replicas
        current["status"]["replicas"] = replicas
        current["status"]["readyReplicas"] = replicas
        return self.put(current)

    def logs(self, _pod, _since=None):
        return b"final log\n"


@pytest.fixture
def env():
    clock, store, kube = Clock(), Store(), Kube()
    approval = {
        "version": 1,
        "run_id": "test-1310",
        "nonce": "a" * 32,
        "sha": "b" * 40,
        "started_at": rt.utc(clock()),
        "expires_at": rt.utc(clock() + 3600),
        "cleanup_at": rt.utc(clock() + 2700),
        "ceilings": copy.deepcopy(rt.CEILINGS),
        "namespaces": {},
        "baseline_uids": [],
        "runtime_sha256": "c" * 64,
        "delivery_sha256": "f" * 64,
        "application_sha256": "d" * 64,
        "images": {
            key: rt.GAR + package + "@sha256:" + "e" * 64
            for key, package in (
                ("smoke", "smoke"),
                ("operator", "operator"),
                ("supervisor", "lifecycle-tools"),
            )
        },
        "lock_owner": {
            "nonce": "a" * 32,
            "kind": "run",
            "run_id": "test-1310",
            "github_run_id": "123",
            "attempt": "1",
            "sha": "b" * 40,
        },
    }
    for ns in (rt.SMOKE, rt.SYSTEM):
        quota = obj("ResourceQuota", "tier3-idle", ns)
        quota["metadata"]["uid"] = ns + "-quota"
        quota["spec"]["hard"] = {"pods": "0", "persistentvolumeclaims": "0"}
        quota["status"] = {
            "hard": copy.deepcopy(quota["spec"]["hard"]),
            "used": {"pods": "0"},
        }
        kube.put(quota)
        approval["namespaces"][ns] = {
            "uid": ns + "-uid",
            "quota_uid": quota["metadata"]["uid"],
            "hard": copy.deepcopy(quota["spec"]["hard"]),
        }
    operator = obj("Deployment", rt.OPERATOR, rt.SYSTEM)
    operator["spec"]["replicas"] = 0
    operator["status"]["replicas"] = 0
    kube.put(operator)
    approval["operator_uid"] = operator["metadata"]["uid"]
    approval["baseline_uids"] = [operator["metadata"]["uid"]]
    store.write(rt.ENVIRONMENT, approval["lock_owner"])
    store.write(
        "_control/runs/test-1310.json",
        {"nonce": approval["nonce"], "phase": "approved", "roots": {}, "observed": {}},
    )
    fixture = kube, store, approval, clock
    app(fixture)
    return fixture


def lifecycle(env, cls=rt.Supervisor):
    kube, store, approval, clock = env
    return cls(rt.Environment(kube, store, approval, clock, clock.sleep))


def app(env):
    _kube, _store, approval, _clock = env
    value = obj("FlinkDeployment", approval["run_id"])
    value["metadata"]["annotations"] = {rt.NONCE: approval["nonce"]}
    value["metadata"]["finalizers"] = ["flink.apache.org"]
    value["spec"] = {"image": approval["images"]["smoke"]}
    approval["application_sha256"] = rt.digest(value)
    return value


@pytest.mark.parametrize("value", ["../bad", "UPPER", "a/b", "a" * 41, "bad-"])
def test_rejects_run_names_before_mutation(env, value):
    env[2]["run_id"] = value
    with pytest.raises(rt.Failure, match="identity"):
        lifecycle(env)
    assert env[0].calls == []


@pytest.mark.parametrize(
    "change",
    [
        lambda a: a["ceilings"].update(pods=6),
        lambda a: a.update(expires_at="2028-01-01T00:00:00+00:00"),
        lambda a: a["namespaces"][rt.SMOKE]["hard"].update(pods="1"),
        lambda a: a["images"].update(smoke="evil@sha256:" + "a" * 64),
    ],
)
def test_rejects_unapproved_limits_and_targets(env, change):
    change(env[2])
    with pytest.raises(rt.Failure):
        lifecycle(env)
    assert env[0].calls == []


def test_lock_has_no_age_based_takeover(env):
    _kube, store, approval, _clock = env
    with pytest.raises(rt.Failure):
        rt.EnvironmentLock(store).acquire({"nonce": "different"})
    assert store.read(rt.ENVIRONMENT)[0] == approval["lock_owner"]


def test_lock_release_cannot_delete_a_replacement(env):
    _kube, store, approval, _clock = env
    store.before_write = None
    owner = approval["lock_owner"]
    _, generation = store.read(rt.ENVIRONMENT)
    store.write(rt.ENVIRONMENT, {"nonce": "replacement"}, generation)
    with pytest.raises(rt.Failure, match="owner changed"):
        rt.EnvironmentLock(store).release(owner)


def test_concurrent_records_preserve_both_roots(env):
    runner = lifecycle(env)
    other = rt.Records(env[1], rt.Approval.from_dict(env[2]))
    env[1].before_write = lambda: other.remember_root("second", {"uid": "two"})
    runner.env.records.remember_root("first", {"uid": "one"})
    assert runner.env.refresh().to_dict()["roots"] == {
        "first": {"uid": "one"},
        "second": {"uid": "two"},
    }


def test_namespace_replacement_blocks_mutations(env):
    env[2]["namespaces"][rt.SMOKE]["uid"] = "replaced"
    runner = lifecycle(env)
    with pytest.raises(rt.Failure, match="Namespace identity"):
        runner.cleanup.quota(rt.SMOKE, "run")
    assert env[0].calls == []


def test_cancellation_blocks_every_admission(env):
    runner = lifecycle(env, cli.runner_api.Runner)
    runner.env.records.request_stop()
    with pytest.raises(rt.Failure, match="stopped"):
        runner.create_application(app(env))
    assert env[0].calls == []


def test_lost_create_response_records_verified_uid(env):
    value = app(env)
    runner = lifecycle(env, cli.runner_api.Runner)
    env[0].fail_create_response = True
    with pytest.raises(rt.Failure, match="Lost response"):
        runner.create_application(value)
    assert (
        runner.env.refresh().to_dict()["roots"]["application"]["uid"]
        == value["metadata"]["uid"]
    )


def test_same_name_different_nonce_is_not_adopted(env):
    value = app(env)
    runner = lifecycle(env, cli.runner_api.Runner)
    runner.env.records.intend("application")
    other = copy.deepcopy(value)
    other["metadata"]["annotations"][rt.NONCE] = "f" * 32
    env[0].put(other)
    with pytest.raises(rt.Failure, match="nonce differs"):
        runner.adopt_application(value)
    assert runner.env.roots == {}


@pytest.mark.parametrize("evidence_failure", [False, True])
def test_cleanup_deletes_application_before_operator_and_restores_quotas(
    env, evidence_failure
):
    runner = lifecycle(env)
    value = env[0].put(app(env))
    runner.env.remember("application", value)
    runner.cleanup.scale_operator(1)
    runner.cleanup.quota(rt.SMOKE, "run")
    runner.cleanup.quota(rt.SYSTEM, "run")
    env[1].fail_evidence = evidence_failure
    runner.cleanup.run("finished", True)
    calls = env[0].calls
    deleted = next(
        i for i, c in enumerate(calls) if c[:2] == ("delete", "FlinkDeployment")
    )
    stopped = calls.index(("scale", 0))
    assert deleted < stopped
    assert runner.env.refresh().to_dict()["phase"] == "cleaned"
    assert runner.env.refresh().to_dict()["success"] is not evidence_failure
    rt.verify_idle(rt.Environment(*env[:3]))
    assert env[1].read(rt.ENVIRONMENT)[0] is not None


def test_operator_scale_handles_omitted_zero_and_preserves_conditional_writes(env):
    kube = env[0]
    runner = lifecycle(env, cli.runner_api.Runner)
    scale_path = kube.path("Deployment", rt.SYSTEM, rt.OPERATOR) + "/scale"
    assert kube.request("GET", scale_path)["spec"] == {}
    runner.cleanup.scale_operator(0)
    assert kube.calls == []
    runner.cleanup.scale_operator(1)
    assert kube.get("Deployment", rt.SYSTEM, rt.OPERATOR)["spec"]["replicas"] == 1
    runner.cleanup.scale_operator(0)
    assert kube.get("Deployment", rt.SYSTEM, rt.OPERATOR)["spec"]["replicas"] == 0
    runner.cleanup.scale_operator(0)
    assert kube.calls == [("scale", 1), ("scale", 0)]


def test_forced_cleanup_only_deletes_uid_descendants_after_normal_window(env):
    runner = lifecycle(env)
    kube, _store, _approval, clock = env
    value = kube.put(app(env))
    runner.env.remember("application", value)
    kube.put(obj("Deployment", "jm", owner=value["metadata"]["uid"]))
    kube.put(obj("Pod", "tm", owner="jm-uid"))
    unrelated = obj("ConfigMap", "unrelated")
    kube.put(unrelated)
    kube.normal_cleanup = False
    started = clock()
    runner.cleanup.run("operator failure")
    assert clock() - started >= 600
    assert kube.get("ConfigMap", rt.SMOKE, "unrelated") == unrelated
    assert not kube.get("Pod", rt.SMOKE, "tm")
    assert not kube.get("FlinkDeployment", rt.SMOKE, "test-1310")


def test_uid_replacement_is_not_force_deleted(env):
    runner = lifecycle(env)
    value = env[0].put(app(env))
    runner.env.remember("application", value)
    env[0].replace_before_delete = True
    with pytest.raises(rt.Failure):
        runner.cleanup.run("replacement")
    assert (
        env[0].get("FlinkDeployment", rt.SMOKE, "test-1310")["metadata"]["uid"]
        == "replacement"
    )
    assert ("scale", 0) not in env[0].calls


def test_orphaned_ha_metadata_blocks_idle_without_becoming_owned(env):
    runner = lifecycle(env)
    value = app(env)
    runner.env.remember("application", env[0].put(value))
    ha = obj("ConfigMap", "test-1310-cluster-config-map")
    ha["metadata"]["labels"] = {
        "app": "test-1310",
        "type": "flink-native-kubernetes",
    }
    env[0].put(ha)
    runner.cleanup.run("normal")
    assert ha["metadata"]["uid"] not in runner.env.observed
    with pytest.raises(rt.Failure, match="non-baseline"):
        rt.verify_idle(rt.Environment(*env[:3]))


def test_terminated_supervisor_can_be_recovered_idempotently(env):
    runner = lifecycle(env, cli.runner_api.Runner)
    job = env[0].put(obj("Job", "supervisor", rt.SYSTEM))
    runner.env.remember("supervisor", job)
    pod = obj("Pod", "supervisor-pod", rt.SYSTEM, job["metadata"]["uid"])
    pod["status"]["containerStatuses"] = [{"state": {"terminated": {"exitCode": 1}}}]
    env[0].put(pod)
    current_job = env[0].get("Job", rt.SYSTEM, "supervisor")
    if current_job:
        current_job["status"]["succeeded"] = 1
        env[0].put(current_job)
    runner.settle(request_stop=True)
    assert runner.env.refresh().to_dict()["idle"]
    runner.settle(request_stop=True)
    assert runner.env.refresh().final_log_attempted
    assert env[1].read(rt.ENVIRONMENT)[0] is not None


@pytest.mark.parametrize("pending", ["operator", "quota-hard", "quota-used"])
@pytest.mark.parametrize("converges", [False, True])
def test_recovery_waits_for_controller_status_with_a_deadline(env, pending, converges):
    kube, store, approval, clock = env
    runner = lifecycle(env, cli.runner_api.Runner)
    seed_record(env, phase="cleaned", state_clean=True)
    started = clock()
    get = kube.get

    def delayed(kind, namespace, name):
        value = get(kind, namespace, name)
        if not converges or clock() < started + 30:
            if pending == "operator" and kind == "Deployment":
                value["status"]["replicas"] = 1
            elif kind == "ResourceQuota":
                if pending == "quota-hard":
                    value["status"]["hard"]["pods"] = "2"
                elif pending == "quota-used":
                    value["status"]["used"]["pods"] = "1"
        return value

    kube.get = delayed
    if converges:
        runner.settle(request_stop=True)
        assert runner.env.refresh().to_dict()["idle"]
        assert clock() - started == 30
    else:
        with pytest.raises(rt.Failure, match="wait expired"):
            runner.settle(request_stop=True)
        assert not runner.env.refresh().to_dict().get("idle")
        assert clock() - started == 180
    assert store.read(rt.ENVIRONMENT)[0] == approval["lock_owner"]


@pytest.mark.parametrize("pending", [False, True])
def test_recovery_does_not_retry_identity_failure_as_controller_lag(env, pending):
    runner = lifecycle(env, cli.runner_api.Runner)
    seed_record(env, phase="cleaned", state_clean=True)
    operator = env[0].get("Deployment", rt.SYSTEM, rt.OPERATOR)
    if pending:
        operator["status"]["replicas"] = 1
        quota = env[0].get("ResourceQuota", rt.SMOKE, "tier3-idle")
        quota["metadata"]["uid"] = "replacement"
        env[0].put(quota)
    else:
        operator["metadata"]["uid"] = "replacement"
    env[0].put(operator)
    with pytest.raises(rt.Failure):
        runner.settle(request_stop=True)
    assert not env[3].sleeps
    assert env[1].read(rt.ENVIRONMENT)[0] is not None


def test_final_receipt_requires_three_empty_plans(env):
    runner = lifecycle(env, cli.runner_api.Runner)
    seed_record(env, success=True)
    with pytest.raises(rt.Failure, match="three"):
        runner.finalize(
            {"nonce": env[2]["nonce"], "roots": cli.wf.ROOTS[:2], "empty": True}
        )
    assert env[1].read(rt.ENVIRONMENT)[0] is not None
    assert runner.finalize(
        {"nonce": env[2]["nonce"], "roots": cli.wf.ROOTS, "empty": True}
    )
    assert env[1].read(rt.ENVIRONMENT)[0] is None
    assert env[1].read("runs/test-1310/result.json")[0]["idle"]


def test_failed_receipt_write_retains_lock_after_paid_workloads_stop(env):
    runner = lifecycle(env, cli.runner_api.Runner)
    env[1].fail_evidence = True
    with pytest.raises(rt.Failure, match="Evidence unavailable"):
        runner.finalize(
            {"nonce": env[2]["nonce"], "roots": cli.wf.ROOTS, "empty": True}
        )
    assert env[1].read(rt.ENVIRONMENT)[0] is not None
    rt.verify_idle(rt.Environment(*env[:3]))


@pytest.mark.parametrize(
    "change",
    [
        lambda r: r.update(path=".github/workflows/evil.yaml"),
        lambda r: r.update(event="pull_request"),
        lambda r: r.update(head_branch="branch"),
        lambda r: r.update(head_sha="0" * 40),
        lambda r: r.update(status="in_progress"),
    ],
)
def test_recovery_checks_origin_not_only_recovery_workflow_identity(
    env, monkeypatch, change
):
    owner = env[2]["lock_owner"]
    source = {
        "repository": {"full_name": rt.REPOSITORY},
        "path": ".github/workflows/tier3-run.yaml",
        "event": "workflow_dispatch",
        "head_branch": "main",
        "head_sha": owner["sha"],
        "run_attempt": 1,
        "status": "completed",
    }
    change(source)
    monkeypatch.setattr(cli.wf, "github_run", lambda _id: source)
    with pytest.raises(rt.Failure):
        cli.wf.verify_source(owner, completed=True)


def test_expiry_stops_before_opening_quotas(env):
    runner = lifecycle(env, cli.runner_api.Runner)
    env[3].now = rt.timestamp(env[2]["cleanup_at"])
    with pytest.raises(rt.Failure, match="admission window"):
        runner.create_application(app(env))
    assert env[0].calls == []


def test_rejected_supervisor_creation_can_return_to_idle(env, monkeypatch):
    runner = lifecycle(env, cli.runner_api.Runner)
    job = obj("Job", "supervisor", rt.SYSTEM)
    monkeypatch.setattr(
        env[0],
        "create",
        lambda *_a, **_k: (_ for _ in ()).throw(rt.ApiError(403, "POST", "jobs")),
    )
    with pytest.raises(rt.ApiError):
        runner.create_root("supervisor", job)
    current_job = env[0].get("Job", rt.SYSTEM, "supervisor")
    if current_job:
        current_job["status"]["succeeded"] = 1
        env[0].put(current_job)
    runner.settle(request_stop=True)
    assert runner.env.refresh().to_dict()["idle"]


@pytest.mark.parametrize("limit", ["bytes", "objects"])
def test_state_limit_triggers_stop_and_generation_checked_cleanup(
    env, monkeypatch, limit
):
    supervisor, pod = prepared_supervisor(env)
    runner = lifecycle(env, cli.runner_api.Runner)
    runner.env.records.set_phase(rt.Phase.READY)
    runner.cleanup.quota(rt.SYSTEM, "run")
    runner.cleanup.scale_operator(1)
    runner.cleanup.quota(rt.SMOKE, "run")
    runner.create_application(app(env))
    application = runner.env.root("application")
    runner.env.records.set_phase(rt.Phase.RUNNING)
    count = rt.CEILINGS["state_objects"] + 1 if limit == "objects" else 1
    for index in range(count):
        env[1].write(
            f"runs/test-1310/state-{index}", {"checkpoint": 1}, bucket=rt.STATE
        )
    env[1].write("runs/unrelated/state", {}, bucket=rt.STATE)
    objects = env[1].objects

    def inventory(prefix, bucket=rt.EVIDENCE, maximum=20000):
        result = objects(prefix, bucket, maximum)
        if limit == "bytes" and bucket == rt.STATE and result:
            result[0]["size"] = str(rt.CEILINGS["state_bytes"] + 1)
        return result

    monkeypatch.setattr(env[1], "objects", inventory)
    started = env[3]()
    supervisor.supervise(pod["metadata"]["uid"])
    control = supervisor.env.refresh()
    assert control.phase == rt.Phase.CLEANED and not control.success
    assert control.state_clean
    assert (
        "State byte ceiling" in control.reason
        if limit == "bytes"
        else "Object inventory exceeds" in control.reason
    )
    assert env[3]() == started, "The first observation must stop the run"
    assert not objects("runs/test-1310/", rt.STATE)
    assert objects("runs/unrelated/", rt.STATE)
    assert ("delete", "FlinkDeployment", application["metadata"]["uid"]) in env[0].calls
    assert env[0].get("Deployment", rt.SYSTEM, rt.OPERATOR)["spec"]["replicas"] == 0
    for ns in (rt.SMOKE, rt.SYSTEM):
        assert (
            env[0].get("ResourceQuota", ns, "tier3-idle")["spec"]["hard"]
            == env[2]["namespaces"][ns]["hard"]
        )
    assert env[1].read(rt.ENVIRONMENT)[0] == env[2]["lock_owner"]


def test_inventory_observations_survive_parent_disappearance(env):
    runner = lifecycle(env)
    value = env[0].put(app(env))
    runner.env.remember("application", value)
    child = env[0].put(obj("Deployment", "jm", owner=value["metadata"]["uid"]))
    pod = env[0].put(obj("Pod", "tm", owner=child["metadata"]["uid"]))
    runner.cleanup.inventory()
    env[0].data.pop(("FlinkDeployment", rt.SMOKE, "test-1310"))
    fresh = lifecycle(env)
    fresh.env.refresh().to_dict()
    fresh.cleanup.run("orphaned parents")
    assert not env[0].get("Pod", rt.SMOKE, pod["metadata"]["name"])


def test_estimated_cost_and_the_rates_age(env):
    # The supervisor shape carries the headroom that keeps it off a busy
    # node; an hour of smoke is estimated well under a dollar.
    assert rt.estimated_cost(3600) == Decimal("0.813125")
    # The review date is the estimate's basis, not an admission deadline.
    env[3].now = rt.timestamp(rt.PRICING_REVIEWED) + 31 * 86400
    later = dict(
        env[2],
        started_at=rt.utc(env[3].now),
        expires_at=rt.utc(env[3].now + 3600),
        cleanup_at=rt.utc(env[3].now + 2700),
    )
    rt.validate_approval(later, env[3]())


def test_admission_quotas_cover_the_pods_each_namespace_will_hold(env):
    """The quota that reaches the API server, not the helper that derives it.

    Deriving the numbers is only half of it: a quota written from the wrong
    shapes, or from a hand-written copy of them, still admits nothing. The
    roles below are the Pods each namespace holds under a run admission.
    """
    runner = lifecycle(env)
    runner.cleanup.quota(rt.SYSTEM, "run")
    runner.cleanup.quota(rt.SMOKE, "run")
    # The third `tier3-system` entry is the slot a replacement occupies while
    # the Pod it replaces terminates; nothing runs in it.
    held = {
        rt.SYSTEM: ("supervisor", "operator", "operator"),
        rt.SMOKE: ("smoke", "smoke"),
    }
    # The per-namespace quotas and the flat Pod ceiling are one allowance seen
    # from two sides; drifting apart would admit a Pod the audit then stops.
    assert sum(len(roles) for roles in held.values()) == rt.CEILINGS["pods"]
    for namespace, roles in held.items():
        hard = env[0].get("ResourceQuota", namespace, "tier3-idle")["spec"]["hard"]
        assert hard["pods"] == str(len(roles)), namespace
        for key in ("cpu", "memory", "ephemeral-storage"):
            needed = sum(rt.quantity(rt.POD_RESOURCES[role][key]) for role in roles)
            for category in ("requests.", "limits."):
                # Equal, not merely sufficient: the quota is the enforcement
                # of the approved shapes, so slack in it is slack nobody
                # approved.
                assert rt.quantity(hard[category + key]) == needed, (
                    namespace,
                    category + key,
                )


def test_pod_capacity_and_effective_resources_match_approval(env):
    container = {
        "name": "supervisor",
        "image": env[2]["images"]["supervisor"],
        "resources": {
            k: rt.POD_RESOURCES["supervisor"] for k in ("requests", "limits")
        },
    }
    pod = {
        "spec": {
            "containers": [container],
            "affinity": {
                "nodeAffinity": {
                    "requiredDuringSchedulingIgnoredDuringExecution": {
                        "nodeSelectorTerms": [
                            {
                                "matchExpressions": [
                                    {
                                        "key": "cloud.google.com/gke-spot",
                                        "operator": "NotIn",
                                        "values": ["true"],
                                    }
                                ]
                            }
                        ]
                    }
                }
            },
        }
    }
    rt.verify_pod(pod, "supervisor", container["image"])
    pod["spec"]["affinity"]["nodeAffinity"][
        "requiredDuringSchedulingIgnoredDuringExecution"
    ]["nodeSelectorTerms"].append({})
    with pytest.raises(rt.Failure, match="every term"):
        rt.verify_pod(pod, "supervisor", container["image"])


# The two terms the audit log recorded on the pilot's supervisor Pod
# (tier3-system/lifecycle-bq1312-alo-10-a1-8dcb6, 2026-09-23T15:41:51Z): the
# delivery's own, and the one Autopilot adds for the safe-to-evict annotation.
PILOT_SUPERVISOR_TERMS = [
    {
        "matchExpressions": [
            {
                "key": "cloud.google.com/gke-spot",
                "operator": "NotIn",
                "values": ["true"],
            }
        ]
    },
    {
        "matchExpressions": [
            {
                "key": "cloud.google.com/extended-duration-pods",
                "operator": "In",
                "values": ["1", "X"],
            }
        ]
    },
]


def audited_supervisor(terms):
    container = {
        "image": rt.GAR + "lifecycle-tools@sha256:" + "a" * 64,
        "resources": {
            "requests": dict(rt.POD_RESOURCES["supervisor"]),
            "limits": dict(rt.POD_RESOURCES["supervisor"]),
        },
    }
    pod = {
        "spec": {
            "containers": [container],
            "affinity": {
                "nodeAffinity": {
                    "requiredDuringSchedulingIgnoredDuringExecution": {
                        "nodeSelectorTerms": copy.deepcopy(terms)
                    }
                }
            },
        }
    }
    return pod, container["image"]


def test_the_supervisor_audit_accepts_the_pod_autopilot_admitted():
    pod, image = audited_supervisor(PILOT_SUPERVISOR_TERMS)
    rt.verify_pod(pod, "supervisor", image)


@pytest.mark.parametrize(
    "term",
    [
        # Another label, or the same label used to widen rather than narrow.
        {
            "matchExpressions": [
                {"key": "kubernetes.io/arch", "operator": "In", "values": ["amd64"]}
            ]
        },
        {
            "matchExpressions": [
                {
                    "key": "cloud.google.com/extended-duration-pods",
                    "operator": "NotIn",
                    "values": ["1"],
                }
            ]
        },
        {
            "matchExpressions": [
                {
                    "key": "cloud.google.com/extended-duration-pods",
                    "operator": "In",
                    "values": [],
                }
            ]
        },
        # Bundled with something else, the term is no longer Autopilot's alone.
        {
            "matchExpressions": [
                {
                    "key": "cloud.google.com/extended-duration-pods",
                    "operator": "In",
                    "values": ["1"],
                },
                {
                    "key": "cloud.google.com/gke-spot",
                    "operator": "In",
                    "values": ["true"],
                },
            ]
        },
        {"matchExpressions": []},
        {},
    ],
)
def test_the_supervisor_audit_refuses_any_other_term_that_may_select_spot(term):
    pod, image = audited_supervisor([PILOT_SUPERVISOR_TERMS[0], term])
    with pytest.raises(rt.Failure, match="every term"):
        rt.verify_pod(pod, "supervisor", image)


def test_the_audit_accepts_autopilots_term_as_an_alternative_on_its_own():
    """Each term is an alternative, so this one is accepted without the other.

    That it selects no Spot node is GKE's documented premise — an extended run
    time is refused to Spot Pods — not something a fabricated Pod can show.
    """
    pod, image = audited_supervisor([PILOT_SUPERVISOR_TERMS[1]])
    rt.verify_pod(pod, "supervisor", image)


def rig_args(**overrides):
    value = {
        "approve": "phrase",
        "sha": "a" * 40,
        "rig_sha": None,
        "run_id": "rig-1487",
    }
    value.update(overrides)
    return SimpleNamespace(**value)


class EmptyStore:
    def objects(self, prefix):
        return []


def test_dispatch_runs_the_main_commit_unless_a_rig_commit_is_named(monkeypatch):
    monkeypatch.setenv("GITHUB_REF", "refs/heads/main")
    monkeypatch.setenv("GITHUB_SHA", "a" * 40)
    cli.refuse_before_admission(rig_args(), EmptyStore(), "phrase")
    with pytest.raises(rt.Failure, match="exact rig commit"):
        cli.refuse_before_admission(rig_args(sha="b" * 40), EmptyStore(), "phrase")
    # A named rig commit must be the approved one, not main's.
    cli.refuse_before_admission(
        rig_args(sha="b" * 40, rig_sha="b" * 40), EmptyStore(), "phrase"
    )
    with pytest.raises(rt.Failure, match="exact rig commit"):
        cli.refuse_before_admission(
            rig_args(sha="a" * 40, rig_sha="b" * 40), EmptyStore(), "phrase"
        )
    monkeypatch.setenv("GITHUB_REF", "refs/heads/feature")
    with pytest.raises(rt.Failure, match="exact rig commit"):
        cli.refuse_before_admission(
            rig_args(sha="b" * 40, rig_sha="b" * 40), EmptyStore(), "phrase"
        )


def test_the_lock_owner_names_a_rig_commit_the_approval_then_binds(env):
    workflow = {"kind": "run", "nonce": "a" * 32, "sha": "b" * 40}
    assert "rig_sha" not in cli.rig_owner(workflow, rig_args(sha="b" * 40))
    owner = cli.rig_owner(workflow, rig_args(sha="c" * 40))
    assert owner["sha"] == "b" * 40 and owner["rig_sha"] == "c" * 40
    approval = copy.deepcopy(env[2])
    approval["sha"] = "c" * 40
    approval["lock_owner"] = dict(approval["lock_owner"], rig_sha="c" * 40)
    rt.validate_approval(approval)
    approval["lock_owner"]["rig_sha"] = "d" * 40
    with pytest.raises(rt.Failure, match="lock identity"):
        rt.validate_approval(approval)


class SdkAdapter(requests.adapters.BaseAdapter):
    def __init__(self, replies):
        self.replies = iter(replies)
        self.calls = []

    def send(self, request, **kwargs):
        self.calls.append((request, kwargs))
        reply = next(self.replies)
        status, payload = reply[:2]
        response = requests.Response()
        response.status_code = status
        response.url = request.url
        response.request = request
        response.headers["Content-Type"] = "application/json"
        if len(reply) == 3:
            response.headers.update(reply[2])
        if status == 302:
            response.headers["Location"] = "https://foreign.invalid/redirect"
        response.raw = urllib3.HTTPResponse(
            body=io.BytesIO(
                payload if isinstance(payload, bytes) else rt.json_bytes(payload)
            ),
            status=status,
            preload_content=False,
        )
        return response

    def close(self):
        pass


def sdk_store(replies):
    from google.oauth2.credentials import Credentials

    token = rt.GoogleToken(Credentials("fixture-token"))
    session = rt.authorized_session(token)
    adapter = SdkAdapter(replies)
    session.mount("https://", adapter)
    client = storage.Client(
        project="fixture-project",
        credentials=token.credentials,
        _http=session,
        client_options={"api_endpoint": "https://storage.googleapis.com"},
    )
    return rt.Storage(client), adapter


def test_gcs_insert_and_delete_use_observed_generations():
    store, adapter = sdk_store([(200, {"generation": "42"}), (204, b"")])
    assert store.write(rt.ENVIRONMENT, {"holder": "one"}, "17") == "42"
    store.delete(rt.ENVIRONMENT, "42")
    assert "ifGenerationMatch=17" in adapter.calls[0][0].url
    assert "ifGenerationMatch=42" in adapter.calls[1][0].url
    assert adapter.calls[0][0].headers["authorization"] == "Bearer fixture-token"
    assert all(options["timeout"] == rt.HTTP_TIMEOUT for _, options in adapter.calls)
    with pytest.raises(rt.Failure, match="observed"):
        store.delete(rt.ENVIRONMENT, "0")
    assert len(adapter.calls) == 2


@pytest.mark.parametrize("status", [401, 412, 503])
def test_storage_sdk_does_not_retry_or_delete_failed_upload(status):
    store, adapter = sdk_store([(status, {"error": {"message": "fixture failure"}})])
    with pytest.raises(rt.ApiError) as error:
        store.write("control", {}, "0")
    assert error.value.status == status
    assert len(adapter.calls) == 1
    assert adapter.calls[0][0].method == "POST"


def test_storage_sdk_download_pins_metadata_generation():
    store, adapter = sdk_store([(200, {"generation": "17", "size": "2"}), (200, b"{}")])
    assert store.read("control") == ({}, "17")
    assert "generation=17" in adapter.calls[1][0].url
    assert "ifGenerationMatch=17" in adapter.calls[1][0].url


@pytest.mark.parametrize("status", [404, 412])
def test_storage_read_restarts_when_observed_generation_is_replaced(status):
    store, adapter = sdk_store(
        [
            (200, {"generation": "17", "size": "2"}),
            (status, {"error": {"message": "generation replaced"}}),
            (200, {"generation": "18", "size": "2"}),
            (200, b"{}"),
        ]
    )
    assert store.read("control") == ({}, "18")
    assert len(adapter.calls) == 4
    assert "ifGenerationMatch=17" in adapter.calls[1][0].url
    assert "ifGenerationMatch=18" in adapter.calls[3][0].url
    assert "generation=" not in adapter.calls[2][0].url


@pytest.mark.parametrize("status", [404, 412])
def test_storage_read_conflict_retries_are_bounded(status):
    store, adapter = sdk_store(
        [
            reply
            for _ in range(5)
            for reply in (
                (200, {"generation": "17", "size": "2"}),
                (status, {"error": {"message": "generation replaced"}}),
            )
        ]
    )
    with pytest.raises(rt.Failure, match="Concurrent storage reads did not settle"):
        store.read("control")
    assert len(adapter.calls) == 10


@pytest.mark.parametrize("status", [401, 403, 503])
def test_storage_read_does_not_retry_other_download_failures(status):
    store, adapter = sdk_store(
        [
            (200, {"generation": "17", "size": "2"}),
            (status, {"error": {"message": "failure"}}),
        ]
    )
    with pytest.raises(rt.ApiError) as error:
        store.read("control")
    assert error.value.status == status
    assert len(adapter.calls) == 2


def test_storage_sdk_list_keeps_pagination_and_fails_at_count_cap():
    item = {"name": "runs/one", "generation": "17", "size": "2"}
    store, adapter = sdk_store(
        [
            (200, {"items": [item], "nextPageToken": "next"}),
            (200, {"items": [{**item, "name": "runs/two"}]}),
        ]
    )
    with pytest.raises(rt.Failure, match="count ceiling"):
        store.objects("runs/", maximum=1)
    assert len(adapter.calls) == 2
    assert "pageToken=next" in adapter.calls[1][0].url


def test_storage_sdk_disables_proxy_and_redirect(monkeypatch):
    monkeypatch.setenv("HTTPS_PROXY", "http://invalid.example:9")
    store, adapter = sdk_store([(302, {"redirect": "fixture"})])
    with pytest.raises(rt.Failure):
        store.read("control")
    assert len(adapter.calls) == 1
    assert not adapter.calls[0][1]["proxies"]


def test_google_token_uses_sdk_refresh_and_reuses_valid_credentials():
    from google.oauth2.credentials import Credentials

    credentials = Credentials("fixture-token")
    token = rt.GoogleToken(credentials)
    assert token() == "fixture-token"
    assert token() == "fixture-token"


def test_kubernetes_destructive_requests_pin_uid_and_resource_version():
    calls = []

    class Http:
        def json(self, *args, **kwargs):
            calls.append((args, kwargs))
            return {}

    kube = rt.Kubernetes("https://fixture.example", Http())
    resource = obj("Pod", "test")
    kube.delete(resource)
    kube.patch(
        resource, [{"op": "replace", "path": "/metadata/finalizers", "value": []}]
    )
    assert calls[0][0][2]["preconditions"] == {"uid": "test-uid"}
    assert calls[1][0][2][:2] == [
        {"op": "test", "path": "/metadata/uid", "value": "test-uid"},
        {"op": "test", "path": "/metadata/resourceVersion", "value": "1"},
    ]


def test_image_retention_margin_is_checked_against_absolute_expiry(env):
    class Http:
        status_code = 200

        def get(self, *_args, **_kwargs):
            return self

        def raise_for_status(self):
            pass

        def json(self):
            return {"createTime": "2026-09-10T00:00:00Z"}

    receipts = cli.wf.image_receipts(Http(), env[2]["images"], "2026-09-15T00:00:00Z")
    assert len(receipts) == 3
    with pytest.raises(rt.Failure, match="24-hour"):
        cli.wf.image_receipts(Http(), env[2]["images"], "2026-09-16T00:00:00Z")


def prepared_supervisor(env):
    runner = lifecycle(env)
    job = env[0].put(obj("Job", "supervisor", rt.SYSTEM))
    runner.env.remember("supervisor", job)
    return runner, supervisor_pod(env, job)


def supervisor_pod(env, job):
    """A Running supervisor Pod owned by the Job, shaped like the delivery."""
    pod = obj("Pod", "supervisor-pod", rt.SYSTEM, job["metadata"]["uid"])
    pod["spec"] = {
        "containers": [
            {
                "name": "supervisor",
                "image": env[2]["images"]["supervisor"],
                "resources": {
                    k: copy.deepcopy(rt.POD_RESOURCES["supervisor"])
                    for k in ("requests", "limits")
                },
            }
        ],
        "affinity": {
            "nodeAffinity": {
                "requiredDuringSchedulingIgnoredDuringExecution": {
                    "nodeSelectorTerms": [
                        {
                            "matchExpressions": [
                                {
                                    "key": "cloud.google.com/gke-spot",
                                    "operator": "NotIn",
                                    "values": ["true"],
                                }
                            ]
                        }
                    ]
                }
            }
        },
    }
    pod["status"]["phase"] = "Running"
    env[0].put(pod)
    return pod


@pytest.mark.parametrize(
    "failure",
    [
        None,
        "evidence",
        "cancellation",
        "job-failure",
        "missing-lineage",
        "fresh-lineage",
    ],
)
def test_supervisor_state_machine_returns_to_cleanup_on_each_outcome(
    env, monkeypatch, failure
):
    runner, pod = prepared_supervisor(env)
    application = app(env)
    create, request = env[0].create, env[0].request

    def created(value, dry_run=False):
        actual = create(value, dry_run)
        if value["kind"] == "FlinkDeployment" and not dry_run:
            if failure == "cancellation":
                runner.env.records.request_stop()
            actual["status"]["jobStatus"] = {
                "state": "FAILED" if failure == "job-failure" else "FINISHED",
                "jobId": "f" * 32,
            }
            env[0].put(actual)
            env[0].put(
                obj("Service", "test-1310-rest", owner=actual["metadata"]["uid"])
            )
            task = obj("Pod", "task", owner=actual["metadata"]["uid"])
            task["spec"] = {
                "containers": [
                    {
                        "name": "flink-main-container",
                        "image": env[2]["images"]["smoke"],
                        "resources": {
                            k: rt.POD_RESOURCES["smoke"] for k in ("requests", "limits")
                        },
                    }
                ],
                "nodeSelector": {"cloud.google.com/gke-spot": "true"},
            }
            task["status"]["phase"] = "Running"
            env[0].put(task)
        return actual

    def requested(method, path, *args, **kwargs):
        if path.endswith("/checkpoints"):
            return {"counts": {"completed": 1}}
        result = request(method, path, *args, **kwargs)
        return result

    def logs(item, _since=None):
        if item["metadata"]["namespace"] != rt.SMOKE or failure == "missing-lineage":
            return b"ordinary log\n"
        return progress().encode()

    if failure == "fresh-lineage":
        runner.env.records.record_lineage("22222222-2222-4222-8222-222222222222")
    monkeypatch.setattr(env[0], "logs", logs)
    monkeypatch.setattr(env[0], "create", created)
    monkeypatch.setattr(env[0], "request", requested)
    env[1].fail_evidence = failure == "evidence"
    admission = lifecycle(env, cli.runner_api.Runner)
    admission.env.records.set_phase(rt.Phase.READY)
    admission.cleanup.quota(rt.SYSTEM, "run")
    admission.cleanup.scale_operator(1)
    admission.cleanup.quota(rt.SMOKE, "run")
    admission.create_application(application)
    admission.env.records.set_phase(rt.Phase.RUNNING)
    runner.supervise(pod["metadata"]["uid"])
    control = runner.env.refresh().to_dict()
    assert control["phase"] == "cleaned"
    assert control["success"] == (failure is None)
    assert env[0].get("Deployment", rt.SYSTEM, rt.OPERATOR)["spec"]["replicas"] == 0
    assert (
        env[0].get("ResourceQuota", rt.SMOKE, "tier3-idle")["spec"]["hard"]["pods"]
        == "0"
    )
    assert env[1].read(rt.ENVIRONMENT)[0] is not None
    assert env[0].get("Pod", rt.SYSTEM, pod["metadata"]["name"]), (
        "The external finalizer must observe supervisor termination"
    )


def test_workflows_hold_the_lock_around_infrastructure_and_preserve_main_boundary():
    import yaml

    workflows = Path(__file__).parents[3] / ".github/workflows"
    for mode in ("plan", "apply"):
        source = yaml.safe_load((workflows / f"tofu-{mode}.yaml").read_text())
        steps = source["jobs"][mode]["steps"]
        acquire = next(
            i for i, step in enumerate(steps) if step.get("id") == "environment-lock"
        )
        init = next(
            i for i, step in enumerate(steps) if step.get("name") == "tofu init"
        )
        release = next(
            i for i, step in enumerate(steps) if "lock release" in step.get("run", "")
        )
        assert acquire < init < release
        assert "!cancelled()" in steps[release]["if"]
        assert source["jobs"][mode]["permissions"]["actions"] == "read"
    for name in ("run", "recover"):
        source = yaml.safe_load((workflows / f"tier3-{name}.yaml").read_text())
        job = source["jobs"]["lifecycle"]
        assert job["if"] == "github.ref == 'refs/heads/main'"
        steps = job["steps"]
        accounts = [
            step["with"]["service_account"]
            for step in steps
            if "service_account" in step.get("with", {})
        ]
        assert accounts == [
            "tier3-runner@flink-gcp.iam.gserviceaccount.com",
            "opentofu-plan@flink-gcp.iam.gserviceaccount.com",
            "tier3-runner@flink-gcp.iam.gserviceaccount.com",
        ]
        refs = [
            (index, step["with"]["ref"])
            for index, step in enumerate(steps)
            if step.get("with", {}).get("ref")
        ]
        if name == "recover":
            # Recovery never executes anything but main.
            assert refs == [], "Never execute a workflow_run head in recovery"
            continue
        # A run may check out a chosen rig commit, and only after main's own
        # workflow has verified that the commit heads a branch of this
        # repository, which excludes a fork's pull request commit.
        assert [ref for _, ref in refs] == ["${{ inputs.rig_sha || github.sha }}"]
        [(checkout, _)] = refs
        verify = [
            index
            for index, step in enumerate(steps)
            if "branches-where-head" in step.get("run", "")
            and step.get("if") == "${{ inputs.rig_sha != '' }}"
        ]
        assert verify and verify[0] < checkout, "Verify the rig before checking it out"
        assert '[[ "$RIG_SHA" =~ ^[0-9a-f]{40}$ ]]' in steps[verify[0]]["run"]
        assert '[ "$RIG_SHA" != "$REVIEWED_SHA" ]' in steps[verify[0]]["run"]


def test_completed_later_attempt_can_recover_an_older_retained_lock(env, monkeypatch):
    owner = env[2]["lock_owner"]
    source = {
        "repository": {"full_name": rt.REPOSITORY},
        "path": ".github/workflows/tier3-run.yaml",
        "event": "workflow_dispatch",
        "head_branch": "main",
        "head_sha": owner["sha"],
        "run_attempt": 2,
        "status": "completed",
    }
    monkeypatch.setattr(cli.wf, "github_run", lambda _id: source)
    cli.wf.verify_source(owner, completed=True)
    with pytest.raises(rt.Failure):
        cli.wf.verify_source(owner)


def test_reusing_a_run_id_fails_before_acquiring_a_lock(env, monkeypatch):
    from types import SimpleNamespace

    monkeypatch.setenv("GITHUB_REF", "refs/heads/main")
    monkeypatch.setenv("GITHUB_SHA", env[2]["sha"])
    env[1].write("runs/test-1310/approval.json", {"old": True})
    args = SimpleNamespace(approve=cli.APPROVAL, sha=env[2]["sha"], run_id="test-1310")
    with pytest.raises(rt.Failure, match="already has immutable"):
        cli.start(args, env[1])
    assert env[0].calls == []


def test_native_list_items_receive_requested_type_metadata():
    class Http:
        def json(self, *_args, **_kwargs):
            return {
                "apiVersion": "apps/v1",
                "kind": "DeploymentList",
                "items": [
                    {
                        "metadata": {"name": "operator", "uid": "id"},
                        "spec": {"replicas": 0},
                        "status": {},
                    }
                ],
            }

    kube = rt.Kubernetes("https://fixture.example", Http())
    [deployment] = kube.items("Deployment", rt.SYSTEM)
    assert deployment["kind"] == "Deployment"
    assert deployment["apiVersion"] == "apps/v1"


@pytest.mark.parametrize("lost_response", [False, True])
def test_api_defaulted_supervisor_creation_reaches_idle(
    env, monkeypatch, lost_response
):
    runner = lifecycle(env, cli.runner_api.Runner)
    job = obj("Job", "supervisor", rt.SYSTEM)
    job["spec"] = {
        "template": {
            "spec": {
                "containers": [
                    {
                        "name": "supervisor",
                        "image": env[2]["images"]["supervisor"],
                        "command": ["python3", "-m", "flink_tier3", "supervisor"],
                    }
                ],
                "volumes": [{"name": "source", "configMap": {"name": "source"}}],
            }
        }
    }
    original_create = env[0].create

    def defaulted(manifest, dry_run=False):
        value = original_create(manifest, dry_run)
        container = value["spec"]["template"]["spec"]["containers"][0]
        container.update(
            imagePullPolicy="IfNotPresent",
            terminationMessagePath="/dev/termination-log",
            terminationMessagePolicy="File",
        )
        value["spec"]["template"]["spec"]["volumes"][0]["configMap"]["defaultMode"] = (
            420
        )
        env[0].put(value)
        if lost_response:
            raise rt.Failure("Lost response after defaulting")
        return value

    monkeypatch.setattr(env[0], "create", defaulted)
    if lost_response:
        with pytest.raises(rt.Failure, match="Lost response"):
            runner.create_root("supervisor", job)
    else:
        runner.create_root("supervisor", job)
    pod = obj("Pod", "supervisor-pod", rt.SYSTEM, "supervisor-uid")
    pod["status"]["containerStatuses"] = [{"state": {"terminated": {"exitCode": 0}}}]
    env[0].put(pod)
    current_job = env[0].get("Job", rt.SYSTEM, "supervisor")
    if current_job:
        current_job["status"]["succeeded"] = 1
        env[0].put(current_job)
    runner.settle(request_stop=True)
    assert runner.env.refresh().to_dict()["idle"]
    assert runner.env.refresh().final_log_attempted


@pytest.mark.parametrize("change", ["image", "count", "resources"])
def test_default_aware_lists_preserve_submitted_container_contract(change):
    expected = [{"image": "digest", "resources": {"cpu": "1"}}]
    actual = copy.deepcopy(expected)
    assert rt.contains(actual, expected)
    if change == "image":
        actual[0]["image"] = "other"
    elif change == "count":
        actual.append(copy.deepcopy(expected[0]))
    else:
        actual[0]["resources"]["cpu"] = "2"
    assert not rt.contains(actual, expected)


def test_recorded_uid_cannot_be_transferred_to_identical_replacement(env):
    runner = lifecycle(env)
    original = obj("Job", "supervisor", rt.SYSTEM)
    runner.env.remember("supervisor", original)
    replacement = copy.deepcopy(original)
    replacement["metadata"]["uid"] = "other-uid"
    with pytest.raises(rt.Failure, match="cannot be replaced"):
        runner.env.remember("supervisor", replacement)
    assert runner.env.roots["supervisor"]["uid"] == "supervisor-uid"


@pytest.mark.parametrize("historical_status", ["completed", "in_progress"])
def test_recovery_retry_checks_the_previous_attempt_ended(
    env, monkeypatch, historical_status
):
    monkeypatch.setenv("GITHUB_RUN_ID", "456")
    monkeypatch.setenv("GITHUB_RUN_ATTEMPT", "2")
    env[1].write(
        "_control/recovery.json",
        {"github_run_id": "456", "attempt": "1", "nonce": env[2]["nonce"]},
    )
    calls = []

    def lookup(run_id, attempt=None):
        calls.append((run_id, attempt))
        return {"status": historical_status if attempt == "1" else "in_progress"}

    monkeypatch.setattr(cli.wf, "github_run", lookup)
    if historical_status == "completed":
        cli.claim_recovery(env[1], env[2]["lock_owner"])
        assert env[1].read("_control/recovery.json")[0]["attempt"] == "2"
    else:
        with pytest.raises(rt.Failure, match="active"):
            cli.claim_recovery(env[1], env[2]["lock_owner"])
    assert calls == [("456", None), ("456", "1")]


def progress(
    run_id="test-1310",
    phase="initial",
    lineage="11111111-1111-4111-8111-111111111111",
    restored="false",
):
    return f"2026-09-15T00:00:00Z 1> event=smoke-progress run_id={run_id} phase={phase} lineage={lineage} restored={restored} processed=100 sequence=99\n"


def test_native_ha_metadata_is_observed_without_claiming_uid_ownership(env):
    runner = lifecycle(env)
    application = env[0].put(app(env))
    runner.env.remember("application", application)
    ha = obj("ConfigMap", "test-1310-cluster-config-map")
    ha["metadata"]["labels"] = {"app": "test-1310", "type": "flink-native-kubernetes"}
    env[0].put(ha)
    items, _pods = runner.cleanup.audit()
    assert ha in items
    assert ha["metadata"]["uid"] not in runner.env.observed
    assert rt.ha_metadata(ha, "test-1310")
    assert not rt.ha_metadata(ha, "other-run")
    ha["metadata"]["labels"]["type"] = "other-type"
    assert not rt.ha_metadata(ha, "test-1310")


def test_progress_accepts_restoration_with_the_same_lineage(env):
    runner = lifecycle(env)
    runner.inspect_progress(progress())
    runner.inspect_progress(progress(restored="true"))
    assert (
        runner.env.refresh().to_dict()["lineage"]
        == "11111111-1111-4111-8111-111111111111"
    )


@pytest.mark.parametrize(
    "fields",
    [
        {"lineage": "22222222-2222-4222-8222-222222222222"},
        {"lineage": "not-a-uuid"},
        {"run_id": "other-run"},
        {"phase": "upgrade"},
    ],
)
def test_progress_rejects_changed_or_invalid_identity(env, fields):
    runner = lifecycle(env)
    runner.inspect_progress(progress())
    with pytest.raises(rt.Failure):
        runner.inspect_progress(progress(**fields))
    assert (
        runner.env.refresh().to_dict()["lineage"]
        == "11111111-1111-4111-8111-111111111111"
    )


def test_completed_ci_triggers_recovery_without_replacing_another_source():
    workflow = yaml.safe_load(
        (ROOT / ".github/workflows/tier3-recover.yaml").read_text()
    )
    assert "CI" in workflow[True]["workflow_run"]["workflows"]
    assert workflow[True]["workflow_run"]["types"] == ["completed"]
    group = workflow["concurrency"]["group"]
    assert "github.event.workflow_run.id" in group and "inputs.source_id" in group
    assert workflow["concurrency"]["cancel-in-progress"] is False


@pytest.mark.parametrize("automatic", [False, True])
def test_unrelated_completion_does_not_recover_current_lock(
    env, monkeypatch, automatic
):
    from types import SimpleNamespace

    monkeypatch.setenv(
        "GITHUB_EVENT_NAME", "workflow_run" if automatic else "workflow_dispatch"
    )
    args = SimpleNamespace(source_id="another-execution")
    if automatic:
        cli.recover(args, env[1])
    else:
        with pytest.raises(rt.Failure, match="does not hold"):
            cli.recover(args, env[1])
    assert env[0].calls == []
    assert env[1].read(rt.ENVIRONMENT)[0] == env[2]["lock_owner"]


def test_a_later_read_may_carry_a_startup_burst_and_a_full_one_still_stops(
    env, monkeypatch
):
    """The 2026-09-23 BigQuery pilot's JobManager printed 365 bytes before its
    first read and its whole startup output before the next one."""
    supervisor, pod = prepared_supervisor(env)
    limits, replies = [], iter([b"x" * 365, b"x" * 70000, b"x" * rt.MIB])
    monkeypatch.setattr(
        env[0], "logs", lambda _pod, since=None: limits.append(since) or next(replies)
    )
    supervisor.telemetry([], [pod])
    supervisor.telemetry([], [pod])
    assert supervisor.log_bytes == 70365
    with pytest.raises(rt.Failure, match="Log collection ceiling"):
        supervisor.telemetry([], [pod])
    assert limits[0] is None and limits[1] is not None and limits[2] is not None
    assert any(
        b"x" * 70000 in rt.json_bytes(value)
        for (bucket, path), (value, _gen) in env[1].data.items()
        if "/supervisor/" in path
    )


def test_log_request_keeps_initial_history_and_bounds_both_reads():
    calls = []

    class Http:
        def request(self, *args, **kwargs):
            calls.append((args, kwargs))
            return b"log"

    pod = obj("Pod", "supervisor", rt.SYSTEM)
    pod["spec"]["containers"] = [{"name": "supervisor"}]
    kube = rt.Kubernetes("https://fixture.example", Http())
    kube.logs(pod)
    kube.logs(pod, "2026-09-15T00:00:00Z")
    assert calls[0][1]["limit"] == rt.MIB
    assert "limitBytes=1048576" in calls[0][0][1] and "sinceTime" not in calls[0][0][1]
    assert calls[1][1]["limit"] == rt.MIB and "sinceTime=" in calls[1][0][1]


def test_plan_timeout_preserves_partial_output_and_exact_lock(
    env, monkeypatch, tmp_path, capsys
):
    from types import SimpleNamespace

    cli.wf.save(tmp_path / "owner.json", env[2]["lock_owner"])

    def timeout(*args, **kwargs):
        assert 0 < kwargs["timeout"] <= rt.Schedule.plan_budget_seconds
        raise subprocess.TimeoutExpired(
            args[0], 180, output=b"partial init", stderr=b"provider slow"
        )

    monkeypatch.setattr(subprocess, "run", timeout)
    with pytest.raises(
        rt.Failure,
        match=f"flink-gcp init exhausted the {rt.Schedule.plan_budget_seconds}-second plan budget",
    ):
        cli.wf.plans(
            SimpleNamespace(directory=tmp_path, kubeconfig=tmp_path / "kubeconfig"),
            env[1],
        )
    assert (tmp_path / "flink-gcp-init.log").read_text() == "partial initprovider slow"
    assert "partial initprovider slow" in capsys.readouterr().err
    assert not (tmp_path / "plans.json").exists()
    assert env[1].read(rt.ENVIRONMENT)[0] == env[2]["lock_owner"]


def test_successful_supervisor_without_cleaned_record_enters_recovery_promptly(env):
    runner = lifecycle(env, cli.runner_api.Runner)
    job = obj("Job", "supervisor", rt.SYSTEM)
    job["status"]["succeeded"] = 1
    runner.env.remember("supervisor", env[0].put(job))
    before = env[3]()
    runner.settle()
    assert env[3]() == before
    assert runner.env.refresh().phase == rt.Phase.CLEANED


def supervisor_records(env, event=None):
    """The supervisor's emitted records, payloads included."""
    return [
        value
        for (bucket, name), (value, _generation) in env[1].data.items()
        if bucket == rt.EVIDENCE
        and name.startswith("runs/test-1310/supervisor/")
        and (event is None or value["event"] == event)
    ]


def log_failure(status, path="pods/supervisor-pod/log"):
    def logs(*_args, **_kwargs):
        raise rt.ApiError(status, "GET", path)

    return logs


def pod_reader(env, monkeypatch, answer):
    """Answer only the re-read of `supervisor-pod`; delegate everything else.

    Patching `get` wholesale would let a future audit or root re-read inside
    the poll take this stub's answer and leave the test green while covering
    a different call.
    """
    real, seen = env[0].get, []

    def get(kind, namespace, name, *args, **kwargs):
        if (kind, namespace, name) != ("Pod", rt.SYSTEM, "supervisor-pod"):
            return real(kind, namespace, name, *args, **kwargs)
        seen.append((kind, namespace, name))
        return answer()

    monkeypatch.setattr(env[0], "get", get)
    return seen


def second_pod(env, pod, name="supervisor-pod-2"):
    other = copy.deepcopy(pod)
    other["metadata"]["name"] = name
    other["metadata"]["uid"] = name + "-uid"
    env[0].put(other)
    return other


def test_a_pod_replaced_mid_poll_retires_its_log_instead_of_stopping(env, monkeypatch):
    """A preempted Operator Pod must not end a session that is going fine.

    The inventory lists the Pod, the service replaces it, and the log read
    then answers 404. Measured on 2026-09-20: the Operator was preempted, the
    supervisor read the retired Pod's log, and a session stopped after three
    of its ten cells. The Pod being gone is what makes the 404 benign, so the
    recovery belongs to every scenario rather than to the exercise alone.
    """
    supervisor, pod = prepared_supervisor(env)
    monkeypatch.setattr(env[0], "logs", log_failure(404))
    # The service already replaced it; only the stale listing still has it.
    env[0].data.pop(("Pod", rt.SYSTEM, pod["metadata"]["name"]))
    supervisor.telemetry([], [pod])
    retired = supervisor_records(env, "retired-pod-log-unavailable")
    # The namespace is how an operator reading the evidence learns which Pod
    # went away, so the record has to carry it.
    assert [record["payload"] for record in retired] == [
        {"uid": pod["metadata"]["uid"], "namespace": rt.SYSTEM}
    ]


@pytest.mark.parametrize(
    ("name", "replace"),
    [
        ("gone", lambda pod: None),
        ("replaced", lambda pod: {"metadata": dict(pod["metadata"], uid="other-uid")}),
        (
            "terminating",
            lambda pod: {
                "metadata": dict(pod["metadata"], deletionTimestamp="2026-09-20T00:00Z")
            },
        ),
    ],
)
def test_every_way_a_pod_leaves_retires_its_log(env, monkeypatch, name, replace):
    """Gone, replaced under the same name, or on its way out: all benign."""
    supervisor, pod = prepared_supervisor(env)
    monkeypatch.setattr(env[0], "logs", log_failure(404))
    seen = pod_reader(env, monkeypatch, lambda: replace(pod))
    supervisor.telemetry([], [pod])
    assert seen == [("Pod", rt.SYSTEM, "supervisor-pod")], name
    assert supervisor_records(env, "retired-pod-log-unavailable"), name


def test_a_pod_replaced_mid_poll_keeps_collecting_the_pods_behind_it(env, monkeypatch):
    """Retiring one Pod's log must not abandon the rest of the poll.

    A real poll carries several Pods. Leaving the loop here would silently
    drop every later Pod's log for the rest of the session.
    """
    supervisor, pod = prepared_supervisor(env)
    other = second_pod(env, pod)

    def logs(target, _since=None):
        if target["metadata"]["uid"] == pod["metadata"]["uid"]:
            raise rt.ApiError(404, "GET", "pods/supervisor-pod/log")
        return b"final log\n"

    monkeypatch.setattr(env[0], "logs", logs)
    env[0].data.pop(("Pod", rt.SYSTEM, pod["metadata"]["name"]))
    supervisor.telemetry([], [pod, other])
    # The retired read is charged to nobody and leaves no resume window; the
    # healthy Pod behind it is still collected.
    assert supervisor.log_bytes == len(b"final log\n")
    assert list(supervisor.log_since) == [other["metadata"]["uid"]]


def test_a_tolerated_outage_keeps_collecting_the_pods_behind_it(env, monkeypatch):
    """Same obligation on the tolerance path as on the retire path."""
    supervisor, pod = prepared_supervisor(env)
    other = second_pod(env, pod)

    def logs(target, _since=None):
        if target["metadata"]["uid"] == pod["metadata"]["uid"]:
            raise rt.ApiError(404, "GET", "pods/supervisor-pod/log")
        return b"final log\n"

    monkeypatch.setattr(env[0], "logs", logs)
    pod_reader(
        env,
        monkeypatch,
        lambda: (_ for _ in ()).throw(rt.ApiError(503, "GET", "pods/supervisor-pod")),
    )
    supervisor.telemetry([], [pod, other])
    assert supervisor.pod_read_failures == {pod["metadata"]["uid"]: 1}
    assert list(supervisor.log_since) == [other["metadata"]["uid"]]


def test_an_unresolved_pod_re_read_is_tolerated_then_fatal(env, monkeypatch):
    """The re-read is the guard, so its own outage is not a liveness proof.

    A 404 on the log plus a 503 on the Pod says nothing about the Pod. Calling
    that "still alive" would end a session for the transport rather than for
    anything observed, which is the failure this change exists to remove.
    """
    supervisor, pod = prepared_supervisor(env)
    monkeypatch.setattr(env[0], "logs", log_failure(404))
    pod_reader(
        env,
        monkeypatch,
        lambda: (_ for _ in ()).throw(rt.ApiError(503, "GET", "pods/supervisor-pod")),
    )
    for _ in range(2):
        supervisor.telemetry([], [pod])
    unresolved = supervisor_records(env, "pod-presence-unavailable")
    assert [record["payload"] for record in unresolved] == [
        {"uid": pod["metadata"]["uid"]}
    ] * 2
    # Tolerated, not ignored: an outage that does not pass stops the run.
    with pytest.raises(rt.ApiError) as error:
        supervisor.telemetry([], [pod])
    assert error.value.status == 503


def test_the_re_read_tolerance_counts_consecutive_outages(env, monkeypatch):
    """Two outages hours apart are not the same thing as an outage.

    Without a reset, a session that survived a blip in its first minutes
    would treat an unrelated one in its last as the third strike and stop.
    """
    supervisor, pod = prepared_supervisor(env)
    monkeypatch.setattr(env[0], "logs", log_failure(404))
    # The Pod really is gone; the outage decides whether the re-read says so.
    env[0].data.pop(("Pod", rt.SYSTEM, pod["metadata"]["name"]))
    outage = [True]

    def answer():
        if outage[0]:
            raise rt.ApiError(503, "GET", "pods/supervisor-pod")

    pod_reader(env, monkeypatch, answer)
    for _ in range(2):
        supervisor.telemetry([], [pod])
    assert supervisor.pod_read_failures == {pod["metadata"]["uid"]: 2}
    outage[0] = False
    supervisor.telemetry([], [pod])
    assert supervisor.pod_read_failures == {}
    # A later, separate outage gets the whole allowance again rather than
    # inheriting the earlier one's third strike.
    outage[0] = True
    for _ in range(2):
        supervisor.telemetry([], [pod])
    with pytest.raises(rt.ApiError):
        supervisor.telemetry([], [pod])


@pytest.mark.parametrize("status", [429, 500, 502, 504, "transport"])
def test_every_answerless_re_read_is_tolerated_not_only_a_503(env, monkeypatch, status):
    """A read is answerless or it is not; 503 is not the only way to say so."""
    from flink_tier3.common import TransportError

    unresolved = (
        TransportError("GET pods/supervisor-pod: connection reset")
        if status == "transport"
        else rt.ApiError(status, "GET", "pods/supervisor-pod")
    )
    supervisor, pod = prepared_supervisor(env)
    monkeypatch.setattr(env[0], "logs", log_failure(404))
    pod_reader(env, monkeypatch, lambda: (_ for _ in ()).throw(unresolved))
    supervisor.telemetry([], [pod])
    assert supervisor.pod_read_failures == {pod["metadata"]["uid"]: 1}
    assert supervisor_records(env, "pod-presence-unavailable")


def test_one_pod_cannot_spend_or_replenish_another_pods_allowance(env, monkeypatch):
    """The allowance belongs to a Pod, not to the poll.

    A shared counter lets a healthy Pod reset an unresolved one's budget on
    every poll, so the unresolved Pod never reaches the third strike and its
    logs are never collected; and it lets a Pod that has left the inventory
    spend the allowance of one that has not.
    """
    supervisor, pod = prepared_supervisor(env)
    other = second_pod(env, pod)
    monkeypatch.setattr(env[0], "logs", log_failure(404))

    def answer(kind, namespace, name, *args, **kwargs):
        if name == other["metadata"]["name"]:
            # This one answers: it is terminating, so its log is retired.
            return {"metadata": dict(other["metadata"], deletionTimestamp="2026-09Z")}
        raise rt.ApiError(503, "GET", "pods/supervisor-pod")

    monkeypatch.setattr(env[0], "get", answer)
    for _ in range(2):
        supervisor.telemetry([], [pod, other])
    assert supervisor.pod_read_failures == {pod["metadata"]["uid"]: 2}
    # The Pod that keeps answering has not bought the other one a third poll.
    with pytest.raises(rt.ApiError) as error:
        supervisor.telemetry([], [pod, other])
    assert error.value.status == 503


def test_a_log_that_reads_again_clears_that_pods_allowance(env, monkeypatch):
    """A Pod whose log comes back is a Pod whose presence is settled.

    Without clearing, a Pod that survived two blips early would be stopped by
    a single unrelated one later, however many healthy polls came between.
    """
    supervisor, pod = prepared_supervisor(env)
    failing = [True]

    def logs(_pod, _since=None):
        if failing[0]:
            raise rt.ApiError(404, "GET", "pods/supervisor-pod/log")
        return b"back\n"

    monkeypatch.setattr(env[0], "logs", logs)
    pod_reader(
        env,
        monkeypatch,
        lambda: (_ for _ in ()).throw(rt.ApiError(503, "GET", "pods/supervisor-pod")),
    )
    for _ in range(2):
        supervisor.telemetry([], [pod])
    assert supervisor.pod_read_failures == {pod["metadata"]["uid"]: 2}
    failing[0] = False
    supervisor.telemetry([], [pod])
    assert supervisor.pod_read_failures == {}
    # The allowance is whole again rather than one short of stopping.
    failing[0] = True
    supervisor.telemetry([], [pod])
    assert supervisor.pod_read_failures == {pod["metadata"]["uid"]: 1}


def test_a_pod_that_leaves_the_inventory_takes_its_allowance_with_it(env, monkeypatch):
    """Otherwise a count nobody is spending stops an unrelated Pod later."""
    supervisor, pod = prepared_supervisor(env)
    monkeypatch.setattr(env[0], "logs", log_failure(404))
    pod_reader(
        env,
        monkeypatch,
        lambda: (_ for _ in ()).throw(rt.ApiError(503, "GET", "pods/supervisor-pod")),
    )
    for _ in range(2):
        supervisor.telemetry([], [pod])
    assert supervisor.pod_read_failures == {pod["metadata"]["uid"]: 2}
    # The next inventory does not list it; the run continues without it.
    supervisor.telemetry([], [])
    assert supervisor.pod_read_failures == {}


def test_a_pod_re_read_refused_outright_is_never_tolerated(env, monkeypatch):
    """403 is an answer about us, not about the Pod; it was never transient."""
    supervisor, pod = prepared_supervisor(env)
    monkeypatch.setattr(env[0], "logs", log_failure(404))
    pod_reader(
        env,
        monkeypatch,
        lambda: (_ for _ in ()).throw(rt.ApiError(403, "GET", "pods/supervisor-pod")),
    )
    with pytest.raises(rt.ApiError) as error:
        supervisor.telemetry([], [pod])
    assert error.value.status == 403
    assert supervisor.pod_read_failures == {}


def test_a_404_for_a_pod_that_is_still_there_is_still_fatal(env, monkeypatch):
    """Only a Pod that actually went away explains a missing log."""
    supervisor, pod = prepared_supervisor(env)
    monkeypatch.setattr(env[0], "logs", log_failure(404))
    with pytest.raises(rt.ApiError) as error:
        supervisor.telemetry([], [pod])
    assert error.value.status == 404


def test_a_log_read_that_failed_for_another_reason_is_still_fatal(env, monkeypatch):
    """The recovery is for a missing Pod, not for every failed log read."""
    supervisor, pod = prepared_supervisor(env)
    monkeypatch.setattr(env[0], "logs", log_failure(500))
    env[0].data.pop(("Pod", rt.SYSTEM, pod["metadata"]["name"]))
    with pytest.raises(rt.ApiError) as error:
        supervisor.telemetry([], [pod])
    assert error.value.status == 500
    assert not supervisor_records(env, "retired-pod-log-unavailable")


def test_the_pod_ceiling_admits_the_replacement_slot_and_refuses_one_more(env):
    """Five Pods is the four that run plus one replacement, and no more."""
    runner, pod = prepared_supervisor(env)
    for n in range(2, 6):
        second_pod(env, pod, f"supervisor-pod-{n}")
    _items, pods = runner.cleanup.audit()
    assert len(pods) == 5
    second_pod(env, pod, "supervisor-pod-6")
    with pytest.raises(rt.Failure, match="count ceiling"):
        runner.cleanup.audit()


def test_initial_log_over_its_ceiling_is_not_silently_accepted(env, monkeypatch):
    supervisor, pod = prepared_supervisor(env)
    monkeypatch.setattr(env[0], "logs", lambda *_args: b"x" * rt.MIB)
    with pytest.raises(rt.Failure, match="Log collection ceiling"):
        supervisor.telemetry([], [pod])
    assert pod["metadata"]["uid"] not in supervisor.log_since


def test_plan_timeout_workflow_output_is_bounded(env, monkeypatch, tmp_path, capsys):
    from types import SimpleNamespace

    cli.wf.save(tmp_path / "owner.json", env[2]["lock_owner"])
    output = "START" + "x" * 70000 + "END"

    def timeout(*args, **kwargs):
        assert 0 < kwargs["timeout"] <= rt.Schedule.plan_budget_seconds
        raise subprocess.TimeoutExpired(args[0], 180, output=output.encode())

    monkeypatch.setattr(subprocess, "run", timeout)
    with pytest.raises(rt.Failure, match="inspect the output above"):
        cli.wf.plans(
            SimpleNamespace(directory=tmp_path, kubeconfig=tmp_path / "kubeconfig"),
            env[1],
        )
    stderr = capsys.readouterr().err
    assert len(stderr.encode()) == 65537
    assert stderr.endswith("END\n") and "START" not in stderr
    assert (tmp_path / "flink-gcp-init.log").read_text() == output


class SdkPool:
    def __init__(self, replies):
        self.replies = iter(replies)
        self.calls = []

    def request(self, *args, **kwargs):
        self.calls.append((args, kwargs))
        status, body = next(self.replies)
        return urllib3.HTTPResponse(
            body=io.BytesIO(body if isinstance(body, bytes) else rt.json_bytes(body)),
            status=status,
            headers={"Content-Type": "application/json"},
            preload_content=False,
        )


def sdk_kube(replies, token=lambda: "fixture-token"):
    transport = rt.KubernetesTransport("https://fixture.example", token)
    pool = SdkPool(replies)
    transport.client.rest_client.pool_manager = pool
    return rt.Kubernetes("https://fixture.example", transport), pool


def test_kubernetes_sdk_preserves_delete_and_patch_preconditions():
    kube, pool = sdk_kube([(200, {}), (200, {})])
    pod = obj("Pod", "owned")
    kube.delete(pod)
    kube.patch(pod, [{"op": "replace", "path": "/metadata/finalizers", "value": []}])
    delete, patch = [json.loads(kwargs["body"]) for _, kwargs in pool.calls]
    assert delete["preconditions"] == {"uid": pod["metadata"]["uid"]}
    assert patch[:2] == [
        {"op": "test", "path": "/metadata/uid", "value": pod["metadata"]["uid"]},
        {"op": "test", "path": "/metadata/resourceVersion", "value": "1"},
    ]
    assert pool.calls[1][1]["headers"]["Content-Type"] == "application/json-patch+json"
    assert all(
        kwargs["timeout"].read_timeout == rt.HTTP_TIMEOUT for _, kwargs in pool.calls
    )


@pytest.mark.parametrize("status", [404, 409, 403])
def test_kubernetes_sdk_errors_are_mapped_without_retries(status):
    kube, pool = sdk_kube([(status, {"message": "fixture failure"})])
    if status == 404:
        assert kube.get("Pod", rt.SYSTEM, "absent") is None
    else:
        with pytest.raises(rt.ApiError) as error:
            kube.get("Pod", rt.SYSTEM, "absent")
        assert error.value.status == status
    assert len(pool.calls) == 1


def test_a_transient_read_is_retried_and_a_mutation_is_not():
    """Pilot bq1312-alo-10-a6 lost a run to one 503 on a namespace read."""
    kube, pool = sdk_kube([(503, {}), (502, {}), (200, {"metadata": {"uid": "u"}})])
    slept = []
    kube.sleep = slept.append
    assert kube.get("Pod", rt.SYSTEM, "owned") == {"metadata": {"uid": "u"}}
    assert len(pool.calls) == 3 and slept == [1, 2]
    kube, pool = sdk_kube([(503, {})] * 4)
    kube.sleep = slept.append
    with pytest.raises(rt.ApiError, match="503"):
        kube.get("Pod", rt.SYSTEM, "owned")
    assert len(pool.calls) == 4
    kube, pool = sdk_kube([(503, {})])
    kube.sleep = lambda _delay: pytest.fail("A mutation was retried")
    with pytest.raises(rt.ApiError, match="503"):
        kube.delete(obj("Pod", "owned"))
    assert len(pool.calls) == 1


def test_a_proxied_flink_rest_read_is_not_retried():
    """Its 503 is the job's answer, which recovery windows classify themselves."""
    kube, pool = sdk_kube([(503, {})])
    kube.sleep = lambda _delay: pytest.fail("A proxied read was retried")
    with pytest.raises(rt.ApiError, match="503"):
        kube.request(
            "GET",
            "/api/v1/namespaces/tier3-bigquery/services/app-rest:8081/proxy/jobs/j/checkpoints",
        )
    assert len(pool.calls) == 1


def test_kubernetes_sdk_refreshes_token_and_disables_proxy_retries(monkeypatch):
    monkeypatch.setenv("HTTPS_PROXY", "http://invalid.example:9")
    tokens = iter(["old-token", "new-token"])
    kube, pool = sdk_kube([(200, {}), (200, {})], lambda: next(tokens))
    kube.get("Pod", rt.SYSTEM, "one")
    kube.get("Pod", rt.SYSTEM, "two")
    assert [kwargs["headers"]["authorization"] for _, kwargs in pool.calls] == [
        "Bearer old-token",
        "Bearer new-token",
    ]
    config = kube.http.client.configuration
    assert config.proxy is None
    assert config.retries.total == 0 and config.retries.redirect == 0


def test_google_token_wif_requests_email_identity_for_kubernetes(monkeypatch, tmp_path):
    subject = tmp_path / "subject-token"
    subject.write_text("fixture-subject-token")
    impersonation_url = (
        "https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/"
        "runner@fixture-project.iam.gserviceaccount.com:generateAccessToken"
    )
    credentials = tmp_path / "credentials.json"
    credentials.write_text(
        json.dumps(
            {
                "type": "external_account",
                "audience": (
                    "//iam.googleapis.com/projects/123/locations/global/"
                    "workloadIdentityPools/fixture/providers/fixture"
                ),
                "subject_token_type": "urn:ietf:params:oauth:token-type:jwt",
                "token_url": "https://sts.googleapis.com/v1/token",
                "credential_source": {"file": str(subject)},
                "service_account_impersonation_url": impersonation_url,
            }
        )
    )
    monkeypatch.setenv("GOOGLE_APPLICATION_CREDENTIALS", str(credentials))
    monkeypatch.setenv("GOOGLE_CLOUD_PROJECT", "fixture-project")
    token = rt.GoogleToken()
    adapter = SdkAdapter(
        [
            (200, {"access_token": "federated-token", "expires_in": 3600}),
            (
                200,
                {"accessToken": "runner-token", "expireTime": "2099-01-01T00:00:00Z"},
            ),
        ]
    )
    token.request.func.session.mount("https://", adapter)
    kube, pool = sdk_kube([(200, {"spec": {"hard": {"pods": "0"}}})], token)
    assert kube.get("ResourceQuota", rt.SMOKE, "tier3-idle")["spec"]["hard"] == {
        "pods": "0"
    }
    assert len(adapter.calls) == 2
    impersonation = adapter.calls[1][0]
    assert impersonation.url == impersonation_url
    assert set(json.loads(impersonation.body)["scope"]) == {
        "https://www.googleapis.com/auth/cloud-platform",
        "https://www.googleapis.com/auth/userinfo.email",
    }
    assert pool.calls[0][1]["headers"]["authorization"] == "Bearer runner-token"


def test_google_token_delegates_expired_credential_refresh_to_sdk():
    from google.auth.credentials import Credentials

    class Expired(Credentials):
        def __init__(self):
            super().__init__()
            self.refreshes = 0

        def refresh(self, request):
            self.refreshes += 1
            self.token = "refreshed-token"

    credentials = Expired()
    token = rt.GoogleToken(credentials)
    assert token() == "refreshed-token"
    assert token() == "refreshed-token"
    assert credentials.refreshes == 1


def test_storage_sdk_missing_object_and_delete_are_idempotent():
    store, adapter = sdk_store(
        [
            (404, {"error": {"message": "absent"}}),
            (404, {"error": {"message": "absent"}}),
        ]
    )
    assert store.read("absent") == (None, "0")
    store.delete("absent", "17")
    assert len(adapter.calls) == 2
    assert "ifGenerationMatch=17" in adapter.calls[1][0].url


def test_storage_resumable_upload_cannot_trigger_unconditional_checksum_delete():
    store, adapter = sdk_store(
        [
            (
                200,
                {},
                {"Location": "https://storage.googleapis.com/upload/resumable-session"},
            ),
            (200, {"generation": "42", "crc32c": "AAAAAA=="}),
        ]
    )
    assert store.write("large-evidence", {"payload": "x" * (8 * rt.MIB)}, "0") == "42"
    assert [request.method for request, _ in adapter.calls] == ["POST", "PUT"]
    assert "uploadType=resumable" in adapter.calls[0][0].url
    assert "ifGenerationMatch=0" in adapter.calls[0][0].url
    assert len(adapter.calls[1][0].body) > 8 * rt.MIB


def test_google_token_refresh_transport_bounds_timeout_and_disables_proxy(monkeypatch):
    from google.auth.credentials import Credentials

    class Expired(Credentials):
        def refresh(self, request):
            response = request(url="https://oauth2.googleapis.com/token", method="POST")
            self.token = json.loads(response.data)["access_token"]

    monkeypatch.setenv("HTTPS_PROXY", "http://invalid.example:9")
    token = rt.GoogleToken(Expired())
    adapter = SdkAdapter([(200, {"access_token": "refreshed-token"})])
    token.request.func.session.mount("https://", adapter)
    assert token() == "refreshed-token"
    assert token() == "refreshed-token"
    assert len(adapter.calls) == 1
    assert adapter.calls[0][1]["timeout"] == rt.HTTP_TIMEOUT
    assert not adapter.calls[0][1]["proxies"]


def test_google_token_refresh_does_not_forward_redirect():
    from google.auth.credentials import Credentials

    class Expired(Credentials):
        def refresh(self, request):
            request(url="https://oauth2.googleapis.com/token", method="POST")

    token = rt.GoogleToken(Expired())
    adapter = SdkAdapter([(302, {})])
    token.request.func.session.mount("https://", adapter)
    with pytest.raises(rt.Failure, match="credential refresh failed"):
        token()
    assert len(adapter.calls) == 1


def test_approval_is_an_independent_frozen_snapshot(env):
    from dataclasses import FrozenInstanceError

    value = rt.Approval.from_dict(env[2], env[3]())
    with pytest.raises(FrozenInstanceError):
        value.run_id = "replacement"
    env[2]["namespaces"][rt.SMOKE]["uid"] = "replacement"
    assert value.namespaces[rt.SMOKE]["uid"] == rt.SMOKE + "-uid"


def test_schedule_serial_wait_budget_fits_workflow_timeout(env):
    import re

    import yaml

    schedule = rt.Approval.from_dict(env[2]).schedule
    workflow = yaml.safe_load((ROOT / ".github/workflows/tier3-run.yaml").read_text())
    timeout = workflow["jobs"]["lifecycle"]["timeout-minutes"]
    # The workflow selects a longer timeout for Cloud Tasks sessions and for
    # BigQuery trials; a timeout shorter than a window kills the job mid-run.
    match = re.fullmatch(
        r"\$\{\{ inputs\.scenario == 'cloudtasks' && (\d+) "
        r"\|\| inputs\.scenario == 'bigquery-recovery' && (\d+) \|\| (\d+) \}\}",
        str(timeout),
    )
    assert match, timeout
    session_minutes, trial_minutes, smoke_minutes = (int(m) for m in match.groups())
    assert schedule.serial_budget <= smoke_minutes * 60
    longest = rt.Schedule.for_window(
        env[3](), env[3]() + rt.CLOUDTASKS_CEILINGS["session_seconds"]
    )
    assert longest.serial_budget <= session_minutes * 60
    from flink_tier3.policy import BIGQUERY_CEILINGS

    trial = rt.Schedule.for_window(env[3](), env[3]() + BIGQUERY_CEILINGS["seconds"])
    assert trial.serial_budget <= trial_minutes * 60
    assert schedule.cleanup_at == schedule.expires_at - 900
    assert schedule.active_seconds(env[3]()) == 3420
    late = schedule.cleanup_window(schedule.expires_at + 60)
    assert late.cleanup_end == schedule.expires_at + 240
    assert late.force_at <= schedule.expires_at + 60


def test_the_workflow_offers_exactly_what_dispatch_accepts():
    """Its inputs are a second copy of these values; drift would strand one."""
    import yaml
    from flink_tier3 import bigquery_plan

    workflow = yaml.safe_load((ROOT / ".github/workflows/tier3-run.yaml").read_text())
    inputs = workflow[True]["workflow_dispatch"]["inputs"]
    assert inputs["trial"]["options"] == ["none", *bigquery_plan.TRIALS]
    assert inputs["trial"]["default"] == "none"
    assert inputs["approval"]["description"] == (
        f"Type {cli.APPROVAL}, or {cli.CLOUDTASKS_APPROVAL}, or "
        f"{cli.BIGQUERY_APPROVAL}; spend is approved beforehand from the estimate"
    )


def test_record_phase_transitions_and_wire_round_trip():
    record = rt.RunRecord("nonce")
    for phase in (
        rt.Phase.READY,
        rt.Phase.RUNNING,
        rt.Phase.CLEANING,
        rt.Phase.CLEANED,
    ):
        record.set_phase(phase)
        assert rt.RunRecord.from_dict(record.to_dict()) == record
    with pytest.raises(rt.Failure, match="transition"):
        record.set_phase(rt.Phase.RUNNING)
    record.set_phase(rt.Phase.CLEANED)


def seed_record(env, **fields):
    store = env[1]
    path = "_control/runs/" + env[2]["run_id"] + ".json"
    value, generation = store.read(path)
    store.write(path, {**value, **fields}, generation)


def test_concurrent_cleanup_preserves_uid_graph_and_settles_record_cas(env):
    first = lifecycle(env)
    second = lifecycle(env, cli.runner_api.Runner)
    application = env[0].put(app(env))
    first.env.remember("application", application)
    env[0].put(obj("Deployment", "jm", owner=application["metadata"]["uid"]))
    env[0].put(obj("Pod", "tm", owner="jm-uid"))
    first.cleanup.scale_operator(1)
    first.cleanup.quota(rt.SMOKE, "run")
    first.cleanup.quota(rt.SYSTEM, "run")
    first.cleanup.inventory()
    seed_record(env, phase="running")
    env[0].calls.clear()
    # The runner completes cleanup between the supervisor's read and CAS.
    env[1].before_write = lambda: second.cleanup.run("recovery", False)
    first.cleanup.run("supervisor", False)
    control = first.env.refresh()
    assert env[1].conflicts == 1
    assert control.phase == rt.Phase.CLEANED and control.state_clean
    assert not control.evidence_failed
    assert set(control.observed) == {"test-1310-uid", "jm-uid", "tm-uid"}
    assert all(call[1] == 0 for call in env[0].calls if call[0] == "scale")
    assert all(
        call[2] in control.observed for call in env[0].calls if call[0] == "delete"
    )
    assert all(
        call[3][0]["value"] == env[2]["namespaces"][rt.SMOKE]["hard"]
        for call in env[0].calls
        if call[:2] == ("patch", "ResourceQuota")
    )
    rt.verify_idle(first.env)
    assert env[1].read(rt.ENVIRONMENT)[0] == env[2]["lock_owner"]


@pytest.mark.parametrize("target", ["quota", "operator"])
def test_cleanup_rechecks_lock_after_reading_foundation_version(
    env, monkeypatch, target
):
    actor = lifecycle(env)
    actor.cleanup.scale_operator(1)
    actor.cleanup.quota(rt.SMOKE, "run")
    env[0].calls.clear()
    original = env[0].get
    replaced = False

    def get(kind, namespace, name):
        nonlocal replaced
        value = original(kind, namespace, name)
        if not replaced and kind == (
            "ResourceQuota" if target == "quota" else "Deployment"
        ):
            replaced = True
            _, generation = env[1].read(rt.ENVIRONMENT)
            env[1].write(rt.ENVIRONMENT, {"nonce": "new-owner"}, generation)
        return value

    monkeypatch.setattr(env[0], "get", get)
    with pytest.raises(rt.Failure, match="lock owner changed"):
        if target == "quota":
            actor.cleanup.quota(rt.SMOKE, None)
        else:
            actor.cleanup.scale_operator(0)
    assert env[0].calls == []


def test_concurrent_quota_restore_retries_from_a_fresh_snapshot(env, monkeypatch):
    first, second = lifecycle(env), lifecycle(env)
    first.cleanup.quota(rt.SMOKE, "run")
    original = env[0].patch
    conflicts = []

    def patch(value, changes, **kwargs):
        if not conflicts:
            conflicts.append(True)
            second.cleanup.quota(rt.SMOKE, None)
            raise rt.ApiError(409, "PATCH", "quota")
        return original(value, changes, **kwargs)

    monkeypatch.setattr(env[0], "patch", patch)
    first.cleanup.quota(rt.SMOKE, None)
    assert conflicts == [True]
    assert (
        env[0].get("ResourceQuota", rt.SMOKE, "tier3-idle")["spec"]["hard"]
        == env[2]["namespaces"][rt.SMOKE]["hard"]
    )


def test_supervisor_waits_for_runner_admission_and_only_writes_toward_idle(
    env, monkeypatch
):
    supervisor, pod = prepared_supervisor(env)
    sleep = env[3].sleep
    observations = []

    def admit(seconds):
        observations.append(list(env[0].calls))
        assert not any(call[0] in ("create", "scale", "patch") for call in env[0].calls)
        sleep(seconds)
        env[0].calls.clear()
        # Admission arrives from the distinct runner actor after observation.
        admission = lifecycle(env, cli.runner_api.Runner)
        admission.env.records.set_phase(rt.Phase.READY)
        admission.create_application(app(env))
        admission.env.records.set_phase(rt.Phase.RUNNING)
        admission.env.records.request_stop()

    monkeypatch.setattr(supervisor.env, "sleep", admit)
    supervisor.supervise(pod["metadata"]["uid"])
    assert observations == [[]]
    assert supervisor.env.refresh().phase == rt.Phase.CLEANED
    assert not hasattr(supervisor, "start") and not hasattr(
        supervisor, "create_application"
    )


def test_final_log_failure_remains_recorded_after_settlement_retry(env):
    supervisor, _pod = prepared_supervisor(env)
    job = env[0].get("Job", rt.SYSTEM, "supervisor")
    job["status"]["succeeded"] = 1
    env[0].put(job)
    env[1].fail_evidence = True
    runner = lifecycle(env, cli.runner_api.Runner)
    runner.settle(request_stop=True)
    env[1].fail_evidence = False
    lifecycle(env, cli.runner_api.Runner).settle(request_stop=True)
    assert supervisor.env.refresh().final_log_attempted
    assert supervisor.env.refresh().evidence_failed
    assert env[1].read(rt.ENVIRONMENT)[0] is not None


@pytest.mark.parametrize("lost_response", [False, True])
def test_runner_admits_after_supervisor_readiness_and_settles_once(
    env, monkeypatch, lost_response
):
    _supervisor, pod = prepared_supervisor(env)
    env[0].data.pop(("Pod", rt.SYSTEM, pod["metadata"]["name"]))
    env[0].data.pop(("Job", rt.SYSTEM, "supervisor"))
    seed_record(env, roots={})
    runner = lifecycle(env, cli.runner_api.Runner)
    create = env[0].create

    def created(value, dry_run=False):
        actual = create(value, dry_run)
        if not dry_run and value["kind"] == "Job":
            env[0].put(pod)
            rt.Records(env[1], rt.Approval.from_dict(env[2])).heartbeat()
        if not dry_run and value["kind"] == "FlinkDeployment" and lost_response:
            raise rt.Failure("Lost application response")
        return actual

    monkeypatch.setattr(env[0], "create", created)
    args = (
        obj("ConfigMap", "source", rt.SYSTEM),
        obj("Job", "supervisor", rt.SYSTEM),
        app(env),
    )
    if lost_response:
        with pytest.raises(rt.Failure, match="Lost application response"):
            runner.start(*args)
    else:
        runner.start(*args)
        assert runner.env.refresh().phase == rt.Phase.RUNNING
    assert runner.env.refresh().roots["application"]["uid"] == "test-1310-uid"
    job = env[0].get("Job", rt.SYSTEM, "supervisor")
    job["status"]["succeeded"] = 1
    env[0].put(job)
    runner.settle(request_stop=True)
    assert runner.env.refresh().idle
    assert runner.env.refresh().final_log_attempted
    assert (
        sum(
            call[0] == "delete" and call[1] == "FlinkDeployment"
            for call in env[0].calls
        )
        == 1
    )
    rt.verify_idle(runner.env)


def test_runner_never_admits_application_without_a_ready_supervisor(env):
    runner = lifecycle(env, cli.runner_api.Runner)
    with pytest.raises(rt.Failure, match="wait expired"):
        runner.start(
            obj("ConfigMap", "source", rt.SYSTEM),
            obj("Job", "supervisor", rt.SYSTEM),
            app(env),
        )
    assert not any(call[:2] == ("create", "FlinkDeployment") for call in env[0].calls)
    assert ("scale", 1) not in env[0].calls
    runner.settle(request_stop=True)
    rt.verify_idle(runner.env)


def test_missing_supervisor_job_marks_final_log_evidence_incomplete(env):
    _supervisor, _pod = prepared_supervisor(env)
    env[0].delete(env[0].get("Job", rt.SYSTEM, "supervisor"))
    seed_record(env, phase="cleaned", state_clean=True, success=True)
    runner = lifecycle(env, cli.runner_api.Runner)
    runner.settle()
    assert runner.env.refresh().idle
    assert runner.env.refresh().final_log_attempted
    assert runner.env.refresh().evidence_failed
    assert env[1].read(rt.ENVIRONMENT)[0] is not None


def test_successful_plans_feed_cli_finish_and_release_lock(env, monkeypatch, tmp_path):
    from types import SimpleNamespace

    runner = lifecycle(env, cli.runner_api.Runner)
    seed_record(env, phase="cleaned", state_clean=True, success=True)
    cli.wf.save(tmp_path / "owner.json", env[2]["lock_owner"])
    cli.wf.save(tmp_path / "approval.json", env[2])
    commands = []

    def run(args, **kwargs):
        commands.append(args)
        return SimpleNamespace(returncode=0, stdout="No changes.\n", stderr="")

    class Cluster:
        root_path = tmp_path

        def __init__(self, *args):
            pass

        def authenticate(self):
            pass

        def preflight(self):
            pass

    monkeypatch.setattr(subprocess, "run", run)
    monkeypatch.setattr(cli.bootstrap, "Cluster", Cluster)
    monkeypatch.setattr(cli.bootstrap, "prepare_operator_chart", lambda _path: None)
    monkeypatch.setattr(cli.bootstrap, "provider_environment", lambda _path: {})
    monkeypatch.setattr(cli.wf, "external", lambda _path: env[0])
    args = SimpleNamespace(directory=tmp_path, kubeconfig=tmp_path / "kubeconfig")
    cli.wf.plans(args, env[1])
    cli.finish(args, env[1])
    assert len(commands) == 6
    assert env[1].read(rt.ENVIRONMENT)[0] is None
    result, _ = env[1].read("runs/test-1310/result.json")
    assert result["idle"] and result["success"]
    assert env[1].read(runner.env.records.path)[0] is None


def ready_admission(env, monkeypatch, *, operator_ready=True):
    supervisor, pod = prepared_supervisor(env)
    env[0].data.pop(("Pod", rt.SYSTEM, pod["metadata"]["name"]))
    env[0].data.pop(("Job", rt.SYSTEM, "supervisor"))
    seed_record(env, roots={})
    create, request = env[0].create, env[0].request

    def created(value, dry_run=False):
        result = create(value, dry_run)
        if value["kind"] == "Job" and not dry_run:
            env[0].put(pod)
            supervisor.env.records.heartbeat()
        return result

    def requested(method, path, *args, **kwargs):
        result = request(method, path, *args, **kwargs)
        if not operator_ready:
            operator = env[0].get("Deployment", rt.SYSTEM, rt.OPERATOR)
            operator["status"]["readyReplicas"] = 0
            env[0].put(operator)
        return result

    monkeypatch.setattr(env[0], "create", created)
    monkeypatch.setattr(env[0], "request", requested)
    return supervisor, pod, lifecycle(env, cli.runner_api.Runner)


def test_runner_operator_readiness_timeout_never_creates_application(env, monkeypatch):
    _supervisor, _pod, runner = ready_admission(env, monkeypatch, operator_ready=False)
    before = env[3]()
    with pytest.raises(rt.Failure, match="wait expired"):
        runner.start(
            obj("ConfigMap", "source", rt.SYSTEM),
            obj("Job", "supervisor", rt.SYSTEM),
            app(env),
        )
    assert env[3]() - before == 600
    assert ("scale", 1) in env[0].calls
    assert not any(call[:2] == ("create", "FlinkDeployment") for call in env[0].calls)
    runner.settle(request_stop=True)
    rt.verify_idle(runner.env)


@pytest.mark.parametrize("stage", ["quota", "scale", "application"])
@pytest.mark.parametrize("conflict", [False, True])
def test_supervisor_stop_cannot_clean_ahead_of_inflight_admission(
    env, monkeypatch, stage, conflict
):
    supervisor, pod, runner = ready_admission(env, monkeypatch)
    create, patch, request, delete = (
        env[0].create,
        env[0].patch,
        env[0].request,
        env[0].delete,
    )
    interrupted = False

    def stop():
        nonlocal interrupted
        interrupted = True
        supervisor.env.stopping = True
        with pytest.raises(rt.Failure, match="Admission unfinished"):
            supervisor.supervise(pod["metadata"]["uid"])
        assert supervisor.env.refresh().phase == rt.Phase.READY
        assert not any(
            call[0] == "delete" or call == ("scale", 0) for call in env[0].calls
        )
        if conflict:
            raise rt.ApiError(409, "PATCH", "fixture")

    def created(value, dry_run=False):
        if (
            stage == "application"
            and value["kind"] == "FlinkDeployment"
            and not dry_run
        ):
            stop()
        return create(value, dry_run)

    def patched(value, changes, **kwargs):
        if (
            stage == "quota"
            and not interrupted
            and value["metadata"]["namespace"] == rt.SMOKE
        ):
            stop()
        return patch(value, changes, **kwargs)

    def requested(method, path, body=None, **kwargs):
        if (
            stage == "scale"
            and not interrupted
            and method == "PATCH"
            and body[-1]["value"] == 1
        ):
            stop()
        return request(method, path, body, **kwargs)

    def deleted(value, **kwargs):
        if value["kind"] == "FlinkDeployment":
            assert (
                env[0].get("Deployment", rt.SYSTEM, rt.OPERATOR)["spec"]["replicas"]
                == 1
            )
        return delete(value, **kwargs)

    monkeypatch.setattr(env[0], "create", created)
    monkeypatch.setattr(env[0], "patch", patched)
    monkeypatch.setattr(env[0], "request", requested)
    monkeypatch.setattr(env[0], "delete", deleted)
    with pytest.raises(rt.Failure):
        runner.start(
            obj("ConfigMap", "source", rt.SYSTEM),
            obj("Job", "supervisor", rt.SYSTEM),
            app(env),
        )
    assert interrupted
    env[1].write("runs/test-1310/application.json", app(env))
    runner.settle(request_stop=True)
    rt.verify_idle(runner.env)
    assert runner.env.refresh().idle


def test_supervisor_waits_for_uid_publication_before_application_audit(
    env, monkeypatch
):
    supervisor, pod = prepared_supervisor(env)
    admission = lifecycle(env, cli.runner_api.Runner)
    admission.env.records.set_phase(rt.Phase.READY)
    admission.cleanup.scale_operator(1)
    application = env[0].create(app(env))
    waited = []

    def publish(seconds):
        waited.append(seconds)
        admission.env.remember("application", application)
        admission.env.records.set_phase(rt.Phase.RUNNING)
        admission.env.records.request_stop()
        env[3].sleep(seconds)

    monkeypatch.setattr(supervisor.env, "sleep", publish)
    supervisor.supervise(pod["metadata"]["uid"])
    assert waited == [rt.POLL]
    assert supervisor.env.refresh().phase == rt.Phase.CLEANED
    assert ("delete", "FlinkDeployment", application["metadata"]["uid"]) in env[0].calls


def test_reviewed_policy_preserves_the_fixed_approval_contract():
    assert rt.CEILINGS == {
        "seconds": 3600,
        "cleanup_seconds": 900,
        "pods": 5,
        "pvcs": 0,
        "state_bytes": 1073741824,
        "state_objects": 10000,
        "log_bytes": 104857600,
        "evidence_bytes": 104857600,
    }
    assert rt.COST_RATES == {
        "cpu": Decimal("0.10"),
        "memory": Decimal("0.02"),
        "ephemeral-storage": Decimal("0.001"),
    }
    assert rt.POD_RESOURCES == {
        "smoke": {"cpu": "1", "memory": "2Gi", "ephemeral-storage": "1Gi"},
        "operator": {"cpu": "1", "memory": "2Gi", "ephemeral-storage": "1Gi"},
        "supervisor": {"cpu": "1", "memory": "2Gi", "ephemeral-storage": "128Mi"},
    }
    assert cli.APPROVAL == "APPROVE ONE SMOKE RUN: 5 PODS, 60 MINUTES"


def test_supervisor_source_identity_covers_every_delivered_module_and_policy(tmp_path):
    import shutil
    import sys

    source = Path(rt.__file__).parent
    shutil.copytree(
        source,
        tmp_path / "flink_tier3",
        ignore=shutil.ignore_patterns("__pycache__"),
    )
    original = rt.delivery_digest(tmp_path / "flink_tier3")
    cli.wf.save(tmp_path / "approval.json", {"delivery_sha256": original})
    package = tmp_path / "flink_tier3"
    delivered = rt.delivered_sources(package)
    # The loop is driven by the function under test, so a bug that shrinks
    # the delivery would otherwise shrink this test's coverage with it.
    assert {
        "runtime.py",
        "supervisor.py",
        "cloudtasks.py",
        "model.py",
        "environment.py",
        "policy.toml",
    } <= set(delivered)
    paths = [package / name for name in sorted(delivered)]
    for path in paths:
        contents = path.read_bytes()
        path.write_bytes(contents + b"\n# Changed after approval.\n")
        assert rt.delivery_digest(package) != original, path
        result = subprocess.run(
            [
                sys.executable,
                "-m",
                "flink_tier3",
                "supervisor",
                "--directory",
                str(tmp_path),
            ],
            cwd=tmp_path,
            capture_output=True,
            text=True,
            check=False,
        )
        assert result.returncode != 0
        assert "Supervisor source differs from approval" in result.stderr, (
            path,
            result.stderr,
        )
        path.write_bytes(contents)
    assert rt.delivery_digest(package) == original
    # The digest follows what the entrypoint can import, so a module no
    # delivered module imports is neither covered nor delivered — and becomes
    # both the moment a delivered module imports it, because that module moved.
    (package / "unreviewed.py").write_text("# Extra module.\n")
    assert rt.delivery_digest(package) == original
    assert "unreviewed.py" not in rt.delivered_sources(package)
    runtime = package / "runtime.py"
    reviewed = runtime.read_bytes()
    runtime.write_bytes(reviewed + b"\nfrom . import unreviewed  # noqa: F401\n")
    assert rt.delivery_digest(package) != original
    assert "unreviewed.py" in rt.delivered_sources(package)
    runtime.write_bytes(reviewed)
    # An undelivered module cannot change what the supervisor runs.
    analyze = package / "analyze.py"
    analyze.write_bytes(analyze.read_bytes() + b"\n# Changed after approval.\n")
    assert rt.delivery_digest(package) == original


@pytest.mark.parametrize("changed_data", [False, True])
def test_config_creation_intent_pins_data_without_copying_source(
    env, monkeypatch, changed_data
):
    runner = lifecycle(env, cli.runner_api.Runner)
    config = obj("ConfigMap", "source", rt.SYSTEM)
    config["data"] = {"runtime.py": "# Reviewed source.\n" * 10000}
    create = env[0].create

    def created(value, dry_run=False):
        result = create(value, dry_run)
        if changed_data:
            result["data"]["runtime.py"] = "# Different source.\n"
            env[0].put(result)
        raise rt.Failure("Lost config creation response")

    monkeypatch.setattr(env[0], "create", created)
    with pytest.raises(
        rt.Failure, match="persisted creation intent" if changed_data else "Lost config"
    ):
        runner.create_root("config", config)
    control = runner.env.refresh()
    assert len(rt.json_bytes(control.to_dict())) < 2048
    assert control.config_intent["data_sha256"] == rt.digest(config["data"])
    assert "data" not in control.config_intent
    assert ("config" in control.roots) == (not changed_data)


def test_cli_lock_release_preserves_a_replacement_owner(env, monkeypatch, tmp_path):
    from types import SimpleNamespace

    store = Store()
    owner = env[2]["lock_owner"] | {"kind": "plan"}
    monkeypatch.setattr(cli.wf, "execution", lambda _kind, _nonce: owner)
    args = SimpleNamespace(
        operation="acquire",
        kind="plan",
        target="tier3-bootstrap",
        file=tmp_path / "owner.json",
    )
    cli.lock(args, store)
    saved = json.loads(args.file.read_text())
    assert store.read(rt.ENVIRONMENT)[0] == saved
    args.operation = "release"
    cli.lock(args, store)
    assert store.read(rt.ENVIRONMENT)[0] is None
    replacement = saved | {"nonce": "replacement"}
    store.write(rt.ENVIRONMENT, replacement)
    cli.lock(args, store)
    assert store.read(rt.ENVIRONMENT)[0] == replacement


@pytest.mark.parametrize("has_approval", [False, True])
def test_cli_completed_recovery_reaches_idle_and_retains_lock_for_plans(
    env, monkeypatch, tmp_path, has_approval
):
    from types import SimpleNamespace

    monkeypatch.setenv("GITHUB_RUN_ID", "456")
    monkeypatch.setenv("GITHUB_RUN_ATTEMPT", "1")
    monkeypatch.setenv("GITHUB_OUTPUT", str(tmp_path / "output"))
    owner = env[2]["lock_owner"]
    if has_approval:
        env[1].write("runs/test-1310/approval.json", env[2])
        runner = lifecycle(env, cli.runner_api.Runner)
        runner.cleanup.scale_operator(1)
        runner.create_application(app(env))
        env[1].write("runs/test-1310/application.json", app(env))
    snapshots = []
    monkeypatch.setattr(cli.wf, "snapshot", lambda kube, *_a: snapshots.append(kube))
    monkeypatch.setattr(cli.wf, "external", lambda _path, **kwargs: env[0])
    monkeypatch.setattr(
        cli.wf,
        "github_run",
        lambda _id: {
            "repository": {"full_name": rt.REPOSITORY},
            "path": ".github/workflows/tier3-run.yaml",
            "event": "workflow_dispatch",
            "head_sha": owner["sha"],
            "run_attempt": 1,
            "head_branch": "main",
            "status": "completed",
        },
    )
    environment = rt.Environment
    monkeypatch.setattr(
        rt,
        "Environment",
        lambda kube, store, approval, **kwargs: environment(
            kube, store, approval, env[3], env[3].sleep, **kwargs
        ),
    )
    cli.recover(
        SimpleNamespace(
            source_id="123", directory=tmp_path, kubeconfig=tmp_path / "kubeconfig"
        ),
        env[1],
    )
    rt.verify_idle(environment(env[0], env[1], env[2]))
    assert (tmp_path / "recovered.json").exists()
    assert (tmp_path / "output").read_text() == "idle=true\n"
    assert env[1].read(rt.ENVIRONMENT)[0] == owner
    assert env[1].read("_control/recovery.json")[0]["github_run_id"] == "456"
    assert bool(snapshots) == (not has_approval)


@pytest.mark.parametrize("lost_response", [False, True])
def test_cli_start_settles_once_after_the_last_admission_call(
    env, monkeypatch, tmp_path, lost_response
):
    from types import SimpleNamespace

    _supervisor, _pod, runner = ready_admission(env, monkeypatch)
    env[1].data.clear()
    owner = env[2]["lock_owner"]
    for key, value in {
        "GITHUB_REF": "refs/heads/main",
        "GITHUB_SHA": owner["sha"],
        "GITHUB_ACTOR": "fixture",
        "GITHUB_OUTPUT": str(tmp_path / "output"),
    }.items():
        monkeypatch.setenv(key, value)
    new_uuid = cli.uuid.uuid4
    uuids = iter([cli.uuid.UUID(env[2]["nonce"])])
    monkeypatch.setattr(cli.uuid, "uuid4", lambda: next(uuids, None) or new_uuid())
    monkeypatch.setattr(cli.time, "time", env[3])
    monkeypatch.setattr(cli.wf, "execution", lambda _kind, _nonce: owner)
    monkeypatch.setattr(cli.wf, "external", lambda _path, **kwargs: env[0])
    monkeypatch.setattr(
        cli.bootstrap,
        "Cluster",
        lambda _path: SimpleNamespace(can_i=lambda *args: None),
    )
    monkeypatch.setattr(
        cli.wf,
        "snapshot",
        lambda _kube, *_a: (
            env[2]["namespaces"],
            env[2]["operator_uid"],
            env[2]["images"]["operator"],
            env[2]["baseline_uids"],
        ),
    )
    monkeypatch.setattr(rt, "GoogleToken", lambda: None)
    monkeypatch.setattr(rt, "authorized_session", lambda _token: None)
    monkeypatch.setattr(cli.wf, "image_receipts", lambda *_args: {})
    config, job = (
        obj("ConfigMap", "source", rt.SYSTEM),
        obj("Job", "supervisor", rt.SYSTEM),
    )
    job["spec"]["template"] = {
        "spec": {"containers": [{"image": env[2]["images"]["supervisor"]}]}
    }
    monkeypatch.setattr(
        cli.wf,
        "render",
        lambda *args, **kwargs: (
            {"config": config, "supervisor": job}
            if len(args) > 5 or kwargs.get("expression")
            else app(env)
        ),
    )
    environment = rt.Environment
    monkeypatch.setattr(
        rt,
        "Environment",
        lambda kube, store, approval, **kwargs: environment(
            kube, store, approval, env[3], env[3].sleep, **kwargs
        ),
    )
    create = env[0].create

    def created(value, dry_run=False):
        result = create(value, dry_run)
        if value["kind"] == "FlinkDeployment" and not dry_run:
            current = env[0].get("Job", rt.SYSTEM, "supervisor")
            current["status"]["succeeded"] = 1
            env[0].put(current)
            if lost_response:
                raise rt.Failure("Lost application response")
        return result

    monkeypatch.setattr(env[0], "create", created)
    args = SimpleNamespace(
        approve=cli.APPROVAL,
        sha=owner["sha"],
        run_id=env[2]["run_id"],
        expires_at=env[2]["expires_at"],
        directory=tmp_path,
        kubeconfig=tmp_path / "kubeconfig",
    )
    if lost_response:
        with pytest.raises(rt.Failure, match="Lost application response"):
            cli.start(args, env[1])
    else:
        cli.start(args, env[1])
    assert (tmp_path / "output").read_text() == "idle=true\n"
    assert sum(call[:2] == ("delete", "FlinkDeployment") for call in env[0].calls) == 1
    assert runner.env.refresh().idle
    assert env[1].read(rt.ENVIRONMENT)[0] is not None
    rt.verify_idle(runner.env)


def test_failed_uid_evidence_stops_runner_before_full_admission(env, monkeypatch):
    _supervisor, _pod, runner = ready_admission(env, monkeypatch)
    write = env[1].write
    failed = False

    def failed_observation(name, data, generation="0", bucket=rt.EVIDENCE):
        nonlocal failed
        if name == runner.env.records.path and data.get("observed") and not failed:
            failed = True
            raise rt.Failure("Transient UID evidence write failure")
        return write(name, data, generation, bucket)

    monkeypatch.setattr(env[1], "write", failed_observation)
    with pytest.raises(rt.Failure, match="admission has been stopped"):
        runner.start(
            obj("ConfigMap", "source", rt.SYSTEM),
            obj("Job", "supervisor", rt.SYSTEM),
            app(env),
        )
    assert failed and runner.env.evidence_failed
    assert ("scale", 1) not in env[0].calls
    assert not any(call[:2] == ("create", "FlinkDeployment") for call in env[0].calls)
    assert (
        env[0].get("ResourceQuota", rt.SYSTEM, "tier3-idle")["spec"]["hard"]["pods"]
        == "1"
    )
    runner.settle(request_stop=True)
    rt.verify_idle(runner.env)
    assert runner.env.refresh().evidence_failed


def test_persisted_evidence_failure_blocks_a_new_actor(env):
    seed_record(env, evidence_failed=True)
    runner = lifecycle(env, cli.runner_api.Runner)
    with pytest.raises(rt.Failure, match="admission has been stopped"):
        runner.start(
            obj("ConfigMap", "source", rt.SYSTEM),
            obj("Job", "supervisor", rt.SYSTEM),
            app(env),
        )
    assert not env[0].calls


def test_supervisor_uid_evidence_failure_never_publishes_ready_heartbeat(
    env, monkeypatch
):
    supervisor, pod = prepared_supervisor(env)
    supervisor.env.records.set_phase(rt.Phase.READY)

    def failed_observation(_refs):
        raise rt.Failure("Transient supervisor UID evidence failure")

    monkeypatch.setattr(supervisor.env.records, "observe", failed_observation)
    with pytest.raises(rt.Failure, match="Admission unfinished"):
        supervisor.supervise(pod["metadata"]["uid"])
    record = supervisor.env.refresh()
    assert record.stop_requested
    assert record.heartbeat is None
    assert record.evidence_failed
    assert not env[0].calls
    assert record.phase == rt.Phase.READY


def test_evidence_failure_wins_the_creation_intent_cas_race(env):
    runner = lifecycle(env, cli.runner_api.Runner)
    other = lifecycle(env)
    env[1].before_write = other.env.records.mark_evidence_failed
    with pytest.raises(rt.Failure, match="admission has been stopped"):
        runner.create_application(app(env))
    assert env[1].conflicts == 1
    assert runner.env.refresh().evidence_failed
    assert ("create", "FlinkDeployment", True) in env[0].calls
    assert ("create", "FlinkDeployment", False) not in env[0].calls


# The start arguments `tier3-run.yaml` builds for each scenario.
WORKFLOW_INPUTS = {
    "smoke": [],
    "generic-recovery": [],
    "cloudtasks": [
        "--session",
        "calibration-1246",
        "--flink-version",
        "2.2.1",
        "--application-digest",
        "sha256:" + "d" * 64,
    ],
    "bigquery-recovery": [
        "--trial",
        "alo-10",
        "--application-digest",
        "sha256:" + "d" * 64,
    ],
}


def start_argv(tmp_path, scenario, extra):
    return [
        "--kubeconfig",
        str(tmp_path / "kubeconfig"),
        "start",
        "--run-id",
        "cli-1481",
        "--sha",
        "b" * 40,
        "--expires-at",
        "2026-09-23T16:20:00Z",
        "--approve",
        "phrase",
        "--scenario",
        scenario,
        *extra,
    ]


def test_the_start_inputs_here_are_the_ones_the_workflow_passes():
    import re

    import yaml

    workflow = yaml.safe_load((ROOT / ".github/workflows/tier3-run.yaml").read_text())
    [step] = [
        s for s in workflow["jobs"]["lifecycle"]["steps"] if s.get("id") == "lifecycle"
    ]
    branches = dict(
        re.findall(
            r'"\$SCENARIO" = ([a-z-]+) \]; then\s+start\+=\(([^)]*)\)', step["run"]
        )
    )
    assert set(branches) == {"cloudtasks", "bigquery-recovery"}
    # Each flag with the workflow input it carries, not only its name.
    expected = {
        "cloudtasks": [
            ("--session", "SESSION"),
            ("--flink-version", "FLINK_VERSION"),
            ("--application-digest", "APPLICATION_DIGEST"),
        ],
        "bigquery-recovery": [
            ("--trial", "TRIAL"),
            ("--application-digest", "APPLICATION_DIGEST"),
        ],
    }
    for scenario, arguments in branches.items():
        passed = re.findall(r'(--[a-z-]+) "\$([A-Z_]+)"', arguments)
        assert passed == expected[scenario], scenario
        assert [flag for flag, _ in passed] == [
            a for a in WORKFLOW_INPUTS[scenario] if a.startswith("--")
        ]
        env = step["env"]
        for _, name in passed:
            assert env[name] == "${{ inputs." + name.lower() + " }}", name


@pytest.mark.parametrize("scenario", sorted(WORKFLOW_INPUTS))
def test_cli_start_accepts_the_inputs_the_workflow_passes(
    monkeypatch, tmp_path, scenario
):
    """The BigQuery pilot died here: its digest was refused as a session input."""
    import sys

    class Reached(Exception):
        pass

    def storage():
        raise Reached

    monkeypatch.setattr(rt, "Storage", storage)
    argv = start_argv(tmp_path, scenario, WORKFLOW_INPUTS[scenario])
    monkeypatch.setattr(sys, "argv", ["flink-tier3-lifecycle", *argv])
    with pytest.raises(Reached):
        cli.main()


@pytest.mark.parametrize(
    "scenario, extra",
    [
        ("bigquery-recovery", ["--trial", "alo-10"]),
        ("bigquery-recovery", ["--application-digest", "sha256:" + "d" * 64]),
        (
            "bigquery-recovery",
            [*WORKFLOW_INPUTS["bigquery-recovery"], "--session", "x"],
        ),
        ("cloudtasks", WORKFLOW_INPUTS["cloudtasks"][:4]),
        ("cloudtasks", [*WORKFLOW_INPUTS["cloudtasks"], "--trial", "alo-10"]),
        ("smoke", ["--application-digest", "sha256:" + "d" * 64]),
        ("generic-recovery", ["--trial", "alo-10"]),
        # Supplied but empty: still another scenario's input, or still missing.
        ("smoke", ["--session", ""]),
        (
            "bigquery-recovery",
            ["--trial", "", "--application-digest", "sha256:" + "d" * 64],
        ),
    ],
)
def test_cli_start_refuses_another_scenarios_inputs_before_cloud_access(
    monkeypatch, tmp_path, capsys, scenario, extra
):
    import sys

    def cloud_access():
        raise AssertionError("Argument rejection must precede cloud access")

    monkeypatch.setattr(rt, "Storage", cloud_access)
    monkeypatch.setattr(
        sys, "argv", ["flink-tier3-lifecycle", *start_argv(tmp_path, scenario, extra)]
    )
    with pytest.raises(SystemExit) as error:
        cli.main()
    assert error.value.code == 2
    assert f"{scenario} requires" in capsys.readouterr().err


def test_cli_lock_acquire_requires_kind_before_cloud_access(
    monkeypatch, tmp_path, capsys
):
    import sys

    def cloud_access():
        raise AssertionError("Argument rejection must precede cloud access")

    monkeypatch.setattr(rt, "Storage", cloud_access)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "tier3-lifecycle.py",
            "lock",
            "acquire",
            "--file",
            str(tmp_path / "owner.json"),
        ],
    )
    with pytest.raises(SystemExit) as error:
        cli.main()
    assert error.value.code == 2
    assert "lock acquire requires --kind" in capsys.readouterr().err


@pytest.mark.parametrize("kind", ["plan", "apply"])
def test_infrastructure_recovery_allows_idle_operator_image_drift(
    env, monkeypatch, tmp_path, kind
):
    from types import SimpleNamespace

    monkeypatch.setattr(cli.wf, "ROOT", ROOT)
    owner = env[2]["lock_owner"] | {"kind": kind}
    _, generation = env[1].read(rt.ENVIRONMENT)
    env[1].write(rt.ENVIRONMENT, owner, generation)
    operator = env[0].get("Deployment", rt.SYSTEM, rt.OPERATOR)
    operator["spec"]["template"] = {"spec": {"containers": [{"image": "previous-pin"}]}}
    env[0].put(operator)
    with pytest.raises(rt.Failure, match="image differs"):
        cli.wf.snapshot(env[0])
    monkeypatch.setenv("GITHUB_RUN_ID", "456")
    monkeypatch.setenv("GITHUB_RUN_ATTEMPT", "1")
    monkeypatch.setattr(
        cli.wf, "verify_source", lambda actual, completed: actual == owner and completed
    )
    monkeypatch.setattr(cli.wf, "github_run", lambda _id: {"status": "completed"})
    preflights = []

    class Cluster:
        def __init__(self, _path):
            pass

        def authenticate(self):
            pass

        def preflight(self):
            rt.verify_idle(rt.Environment(env[0], env[1], env[2], env[3], env[3].sleep))
            preflights.append(True)

        def endpoint(self):
            return "https://fixture.example"

    monkeypatch.setattr(cli.bootstrap, "Cluster", Cluster)
    monkeypatch.setattr(rt, "GoogleToken", lambda: None)
    monkeypatch.setattr(rt, "KubernetesTransport", lambda *_args: None)
    monkeypatch.setattr(rt, "Kubernetes", lambda *_args: env[0])
    args = SimpleNamespace(
        source_id="123", directory=tmp_path, kubeconfig=tmp_path / "kubeconfig"
    )
    cli.recover(args, env[1])
    assert preflights == [True]
    assert env[1].read(rt.ENVIRONMENT)[0] == owner
    cli.wf.save(
        tmp_path / "plans.json",
        {"nonce": owner["nonce"], "roots": cli.wf.ROOTS, "empty": False},
    )
    with pytest.raises(rt.Failure, match="restored to idle"):
        cli.finish(args, env[1])
    assert env[1].read(rt.ENVIRONMENT)[0] is None
    assert (
        env[0].get("Deployment", rt.SYSTEM, rt.OPERATOR)["spec"]["template"]["spec"][
            "containers"
        ][0]["image"]
        == "previous-pin"
    )


def test_recorded_supervisor_uid_allows_cleanup_after_server_template_rewrite(
    env, monkeypatch
):
    runner = lifecycle(env, cli.runner_api.Runner)
    runner.cleanup.scale_operator(1)
    runner.create_application(app(env))
    job = obj("Job", "supervisor", rt.SYSTEM)
    job["spec"]["template"] = {
        "spec": {"containers": [{"resources": {"requests": {"cpu": "250m"}}}]}
    }
    create = env[0].create

    def rewritten(manifest, dry_run=False):
        value = create(manifest, dry_run)
        value["spec"]["template"]["spec"]["containers"][0]["resources"]["requests"][
            "cpu"
        ] = "500m"
        value["status"]["succeeded"] = 1
        return env[0].put(value)

    monkeypatch.setattr(env[0], "create", rewritten)
    runner.create_root("supervisor", job)
    assert runner.env.refresh().roots["supervisor"]["uid"] == "supervisor-uid"
    runner.settle(request_stop=True)
    rt.verify_idle(runner.env)
    assert runner.env.refresh().idle
    assert not env[0].get("FlinkDeployment", rt.SMOKE, "test-1310")
    assert env[1].read(rt.ENVIRONMENT)[0] == env[2]["lock_owner"]


@pytest.mark.parametrize("exhaust", [False, True])
def test_plans_share_one_budget_across_all_six_commands(
    env, monkeypatch, tmp_path, exhaust
):
    from types import SimpleNamespace

    cli.wf.save(tmp_path / "owner.json", env[2]["lock_owner"])
    monkeypatch.setattr(cli.wf.time, "monotonic", env[3])
    durations = [110, 140, 20, 40, 20, 240 if exhaust else 40]
    calls = []

    def run(args, **kwargs):
        index = len(calls)
        remaining = rt.Schedule.plan_budget_seconds - sum(durations[:index])
        assert kwargs["timeout"] == remaining
        calls.append(args)
        duration = durations[index]
        if duration > remaining:
            env[3].now += remaining
            raise subprocess.TimeoutExpired(
                args, remaining, output="refresh incomplete"
            )
        env[3].now += duration
        return SimpleNamespace(returncode=0, stdout="No changes.\n", stderr="")

    class Cluster:
        root_path = tmp_path

        def __init__(self, *_args):
            pass

        def authenticate(self):
            pass

        def preflight(self):
            pass

    monkeypatch.setattr(subprocess, "run", run)
    monkeypatch.setattr(cli.bootstrap, "Cluster", Cluster)
    monkeypatch.setattr(cli.bootstrap, "prepare_operator_chart", lambda _path: None)
    monkeypatch.setattr(cli.bootstrap, "provider_environment", lambda _path: {})
    args = SimpleNamespace(directory=tmp_path, kubeconfig=tmp_path / "kubeconfig")
    if exhaust:
        with pytest.raises(rt.Failure, match="tier3-operator plan exhausted"):
            cli.wf.plans(args, env[1])
        assert not (tmp_path / "plans.json").exists()
    else:
        cli.wf.plans(args, env[1])
        assert json.loads((tmp_path / "plans.json").read_text())["empty"]
    assert len(calls) == 6
    assert env[1].read(rt.ENVIRONMENT)[0] == env[2]["lock_owner"]


def test_plans_keep_the_authenticated_kubeconfig_after_tofu_chdir(
    env, monkeypatch, tmp_path
):
    from types import SimpleNamespace

    caller = tmp_path / "caller"
    caller.mkdir()
    (caller / "config").write_text("authenticated caller config")
    checkout = tmp_path / "checkout"
    for root in cli.wf.ROOTS:
        directory = checkout / "opentofu" / root
        directory.mkdir(parents=True)
        (directory / "config").write_text("wrong root-local config")
    monkeypatch.chdir(caller)
    monkeypatch.setattr(cli.wf, "ROOT", checkout)
    monkeypatch.setattr(cli.bootstrap, "ROOT", checkout)
    authenticated = []
    monkeypatch.setattr(
        cli.bootstrap.Cluster,
        "authenticate",
        lambda cluster: authenticated.append(cluster.kubeconfig),
    )
    monkeypatch.setattr(cli.bootstrap.Cluster, "preflight", lambda _cluster: None)
    monkeypatch.setattr(cli.bootstrap, "prepare_operator_chart", lambda _path: None)
    commands = []

    def tofu(arguments, **kwargs):
        root = Path(arguments[1].removeprefix("-chdir="))
        selected = root / kwargs["env"]["KUBE_CONFIG_PATH"]
        assert selected.read_text() == "authenticated caller config"
        commands.append(arguments)
        return subprocess.CompletedProcess(arguments, 0, "empty", "")

    monkeypatch.setattr(cli.wf.subprocess, "run", tofu)
    directory = tmp_path / "evidence"
    cli.wf.save(directory / "owner.json", env[2]["lock_owner"])
    cli.wf.plans(
        SimpleNamespace(kubeconfig=Path("config"), directory=directory), env[1]
    )
    assert authenticated == [caller / "config", caller / "config"]
    assert len(commands) == 6
    assert json.loads((directory / "plans.json").read_text())["empty"]


@pytest.mark.parametrize(
    "diagnostic", ["invalid image digest", "x" * 3000 + "schema mismatch"]
)
def test_render_failure_exposes_bounded_cue_diagnostic(monkeypatch, diagnostic):
    def fail(command, **kwargs):
        assert kwargs["check"] is False
        return subprocess.CompletedProcess(command, 1, "", diagnostic)

    monkeypatch.setattr(cli.wf.subprocess, "run", fail)
    with pytest.raises(rt.Failure) as error:
        cli.wf.render("test-run", "a" * 32, "2026-09-15T01:00:00Z", 3300)
    assert str(error.value) == "CUE rendering failed (exit 1): " + diagnostic[-2000:]


@pytest.mark.parametrize(
    "state", [{"stage": "preparing"}, {}, False, {"stage": "cleaned"}]
)
def test_pending_pubsub_cleanup_blocks_operator_shutdown_and_finalization(env, state):
    runner = lifecycle(env, cli.runner_api.Runner)
    runner.cleanup.scale_operator(1)
    runner.env.records._change(lambda record: setattr(record, "pubsub", state))
    with pytest.raises(rt.Failure, match="Pub/Sub resource cleanup is incomplete"):
        runner.cleanup.scale_operator(0)
    assert ("scale", 0) not in env[0].calls
    with pytest.raises(rt.Failure, match="Pub/Sub resource cleanup is incomplete"):
        runner.finalize(
            {"nonce": env[2]["nonce"], "roots": cli.wf.ROOTS, "empty": True}
        )
    assert env[1].read(rt.ENVIRONMENT)[0] is not None
    assert env[1].read(runner.env.records.path)[0] is not None
    assert env[1].read("runs/test-1310/result.json")[0] is None


def test_cleaned_pubsub_record_allows_normal_settlement(env):
    runner = lifecycle(env, cli.runner_api.Runner)
    runner.cleanup.scale_operator(1)

    def cleaned(record):
        record.pubsub = {
            "stage": "cleaned",
            "resources": [
                {"name": "owned-topic", "labels": {"tier3-run": "test-1310"}}
            ],
            "policies": [{"name": "owned-topic", "policy": {"etag": "observed"}}],
        }
        record.stop_requested = True

    runner.env.records._change(cleaned)
    saved = runner.env.refresh().pubsub
    runner.cleanup.scale_operator(0)
    assert ("scale", 0) in env[0].calls
    runner.finalize({"nonce": env[2]["nonce"], "roots": cli.wf.ROOTS, "empty": True})
    assert env[1].read(rt.ENVIRONMENT)[0] is None
    assert env[1].read(runner.env.records.path)[0] is None

    assert env[1].read("runs/test-1310/result.json")[0]["pubsub"] == saved


@pytest.mark.parametrize("previous", [None, {"stage": "cleaned", "resources": []}])
def test_prior_receipt_without_matching_pubsub_observations_retains_control(
    env, previous
):
    runner = lifecycle(env, cli.runner_api.Runner)

    def cleaned(record):
        record.pubsub = {"stage": "cleaned", "resources": [{"name": "owned-topic"}]}
        record.stop_requested = True

    runner.env.records._change(cleaned)
    receipt = {
        "nonce": env[2]["nonce"],
        "sha": env[2]["sha"],
        "idle": True,
        "success": False,
        "plans": {"nonce": env[2]["nonce"], "roots": cli.wf.ROOTS, "empty": True},
    }
    if previous is not None:
        receipt["pubsub"] = previous
    path = "runs/test-1310/result.json"
    generation = env[1].write(path, receipt)
    plans = {"nonce": env[2]["nonce"], "roots": cli.wf.ROOTS, "empty": True}
    with pytest.raises(rt.Failure, match="Final receipt conflicts"):
        runner.finalize(plans)
    assert env[1].read(rt.ENVIRONMENT)[0] is not None
    assert env[1].read(runner.env.records.path)[0] is not None
    receipt["pubsub"] = runner.env.refresh().pubsub
    env[1].write(path, receipt, generation)
    assert runner.finalize(plans) is False
    assert env[1].read(rt.ENVIRONMENT)[0] is None
    assert env[1].read(runner.env.records.path)[0] is None
    assert env[1].read(path)[0] == receipt


def test_prior_pubsub_receipt_with_missing_control_snapshot_retains_lock(env):
    runner = lifecycle(env, cli.runner_api.Runner)
    env[1].write(
        "runs/test-1310/result.json",
        {
            "nonce": env[2]["nonce"],
            "idle": True,
            "success": False,
            "pubsub": {"stage": "cleaned", "resources": []},
            "sha": env[2]["sha"],
            "plans": {"nonce": env[2]["nonce"], "roots": cli.wf.ROOTS, "empty": True},
        },
    )
    with pytest.raises(rt.Failure, match="Final receipt conflicts"):
        runner.finalize(
            {"nonce": env[2]["nonce"], "roots": cli.wf.ROOTS, "empty": True}
        )
    assert env[1].read(rt.ENVIRONMENT)[0] is not None
    assert env[1].read(runner.env.records.path)[0] is not None


def test_pubsub_finalization_retains_control_changed_during_receipt_write(env):
    runner = lifecycle(env, cli.runner_api.Runner)

    def cleaned(record):
        record.pubsub = {"stage": "cleaned", "resources": []}
        record.stop_requested = True

    runner.env.records._change(cleaned)
    env[1].before_write = lambda: runner.env.records._change(
        lambda record: record.pubsub.update(stage="cleaning")
    )
    plans = {"nonce": env[2]["nonce"], "roots": cli.wf.ROOTS, "empty": True}
    with pytest.raises(
        rt.Failure, match="Run control changed during service finalization"
    ):
        runner.finalize(plans)
    assert runner.env.refresh().pubsub["stage"] == "cleaning"
    assert env[1].read(rt.ENVIRONMENT)[0] is not None
    runner.env.records._change(cleaned)
    assert runner.finalize(plans) is False
    assert env[1].read(rt.ENVIRONMENT)[0] is None


def test_pubsub_retry_refuses_success_receipt_invalidated_by_evidence_failure(env):
    runner = lifecycle(env, cli.runner_api.Runner)

    def cleaned(record):
        record.pubsub = {"stage": "cleaned", "resources": []}
        record.stop_requested = True
        record.success = True

    runner.env.records._change(cleaned)
    env[1].before_write = lambda: runner.env.records._change(
        lambda record: setattr(record, "evidence_failed", True)
    )
    plans = {"nonce": env[2]["nonce"], "roots": cli.wf.ROOTS, "empty": True}
    with pytest.raises(
        rt.Failure, match="Run control changed during service finalization"
    ):
        runner.finalize(plans)
    assert env[1].read("runs/test-1310/result.json")[0]["success"] is True
    assert runner.env.refresh().evidence_failed
    with pytest.raises(rt.Failure, match="Final receipt conflicts"):
        runner.finalize(plans)
    assert env[1].read(rt.ENVIRONMENT)[0] is not None
    assert runner.env.refresh().evidence_failed
