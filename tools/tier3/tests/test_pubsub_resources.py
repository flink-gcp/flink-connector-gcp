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
"""Exercise Pub/Sub ownership and partial failures without credentials or service calls."""

import copy
import json

import pytest
import requests
from flink_tier3.common import ApiError, Failure, TransportError
from flink_tier3.pubsub import (
    BASE,
    HTTP_TIMEOUT,
    ResourcePlan,
    Resources,
    validate_resource,
)


class FakeStore:
    def __init__(self):
        self.value = None
        self.calls = []

    def read(self, path):
        self.calls.append(("GET", path))
        return copy.deepcopy(self.value), "1" if self.value is not None else "0"

    def write(self, path, value, generation):
        self.calls.append(("PUT", path))
        assert generation == "0"
        if self.value is not None:
            raise ApiError(412, "PUT", path)
        self.value = copy.deepcopy(value)
        return "1"


class FakeHttp:
    def __init__(self):
        self.resources = {}
        self.calls = []
        self.before = lambda method, name: None
        self.after = lambda method, name: None

    def request(self, method, url, *, json, timeout, allow_redirects):
        assert url.startswith(BASE)
        assert timeout == HTTP_TIMEOUT
        assert allow_redirects is False
        name = url.removeprefix(BASE)
        self.calls.append((method, name, copy.deepcopy(json)))
        self.before(method, name)
        if method == "GET":
            response = reply(
                self.resources.get(name), 200 if name in self.resources else 404
            )
        elif method == "PUT":
            if name in self.resources:
                return reply({}, 409)
            if "/subscriptions/" in name:
                assert json["topic"] in self.resources
            value = {**copy.deepcopy(json), "state": "ACTIVE"}
            # The REST service may omit false scalar values and echo empty push config.
            if "/subscriptions/" in name:
                value = {k: v for k, v in value.items() if v is not False}
                value["pushConfig"] = {}
            else:
                value["messageStoragePolicy"].pop("enforceInTransit")
            self.resources[name] = value
            response = reply(value)
        elif method == "DELETE":
            assert json is None
            response = reply(
                {}, 200 if self.resources.pop(name, None) is not None else 404
            )
        else:
            raise AssertionError(method)
        self.after(method, name)
        return response


def reply(value, status=200):
    response = requests.Response()
    response.status_code = status
    response._content = json.dumps(value).encode()
    return response


@pytest.fixture
def setup():
    plan = ResourcePlan("probe", "a" * 32)
    http, store, guards = FakeHttp(), FakeStore(), []
    resources = Resources(http, store, plan, lambda *args: guards.append(args))
    return resources, http, store, guards


def test_plan_matches_the_relay_names_and_fixed_settings():
    plan = ResourcePlan("probe", "a" * 32)
    assert plan.manifest_path == "_control/pubsub/probe.json"
    assert [p["name"] for p in plan.topics()] == [
        f"projects/flink-gcp/topics/t3-probe-{suffix}"
        for suffix in ("in-0", "in-1", "out")
    ]
    for topic, sub in zip(plan.topics(), plan.subscriptions(), strict=True):
        assert sub["name"] == topic["name"].replace("/topics/", "/subscriptions/")
        assert sub["topic"] == topic["name"]
        assert (
            topic["labels"]
            == sub["labels"]
            == {
                "tier3-run": "probe",
                "tier3-nonce": "a" * 32,
            }
        )
        assert sub["ackDeadlineSeconds"] == 30
        assert sub["messageRetentionDuration"] == "86400s"
        assert sub["expirationPolicy"] == {"ttl": "86400s"}
        assert not any(
            sub[key]
            for key in (
                "enableExactlyOnceDelivery",
                "enableMessageOrdering",
                "retainAckedMessages",
            )
        )
        assert topic["messageStoragePolicy"] == {
            "allowedPersistenceRegions": ["us-central1"],
            "enforceInTransit": False,
        }
    mutated = plan.manifest()
    mutated["topics"][0]["labels"]["tier3-nonce"] = "b" * 32
    assert plan.manifest()["topics"][0]["labels"]["tier3-nonce"] == "a" * 32


@pytest.mark.parametrize(
    "run_id,nonce",
    [
        ("../outside", "a" * 32),
        ("x" * 41, "a" * 32),
        (None, "a" * 32),
        ("probe", ""),
        ("probe", "A" * 32),
        ("probe", True),
    ],
)
def test_invalid_plan_refuses_before_io(run_id, nonce):
    with pytest.raises(Failure):
        ResourcePlan(run_id, nonce)


def test_construction_has_no_io_and_requires_guard(setup):
    resources, http, store, guards = setup
    assert not http.calls and not store.calls and not guards
    with pytest.raises(Failure, match="guard"):
        Resources(http, store, resources.plan, None)


def test_create_inspect_and_repeated_cleanup(setup):
    resources, http, store, guards = setup
    observed = resources.provision()
    expected = resources.plan.topics() + resources.plan.subscriptions()
    assert [item["name"] for item in observed] == [p["name"] for p in expected]
    puts = [call for call in http.calls if call[0] == "PUT"]
    assert [(method, name, body) for method, name, body in puts] == [
        ("PUT", p["name"], p) for p in expected
    ]
    manifest = resources.plan.manifest()
    assert store.value == manifest
    assert resources.inspect() == observed
    assert len(guards) == len(http.calls) + len(store.calls)
    for call in http.calls:
        assert any(guard[1:] == call[:2] for guard in guards)
    absent = resources.cleanup()
    order = resources.plan.subscriptions() + resources.plan.topics()
    assert absent == [p["name"] for p in order]
    assert [c[1] for c in http.calls if c[0] == "DELETE"] == absent
    assert not http.resources
    assert resources.cleanup() == absent
    assert len([c for c in http.calls if c[0] == "DELETE"]) == 6
    assert store.value == manifest
    assert len(guards) == len(http.calls) + len(store.calls)


@pytest.mark.parametrize("index", range(6))
def test_collision_never_records_or_mutates_any_resource(setup, index):
    resources, http, store, _ = setup
    existing = (resources.plan.topics() + resources.plan.subscriptions())[index]
    http.resources[existing["name"]] = copy.deepcopy(existing)
    with pytest.raises(Failure, match="already exists"):
        resources.provision()
    assert store.value is None
    assert all(call[0] == "GET" for call in http.calls)
    assert http.resources == {existing["name"]: existing}
    with pytest.raises(Failure, match="manifest"):
        resources.cleanup()


def test_existing_manifest_cannot_restart_provisioning(setup):
    resources, http, store, _ = setup
    store.value = resources.plan.manifest()
    with pytest.raises(Failure, match="already has"):
        resources.provision()
    assert not http.calls


def test_lost_manifest_write_never_starts_service_creation(setup):
    resources, http, store, _ = setup
    original = store.write

    def lose(*args, **kwargs):
        original(*args, **kwargs)
        raise Failure("Manifest response lost")

    store.write = lose
    with pytest.raises(Failure, match="response lost"):
        resources.provision()
    assert all(c[0] == "GET" for c in http.calls)
    assert resources.cleanup()
    assert not http.resources


@pytest.mark.parametrize("index", range(6))
def test_ambiguous_create_is_not_retried_and_partial_work_is_cleanable(setup, index):
    resources, http, store, _ = setup
    names = [
        p["name"] for p in resources.plan.topics() + resources.plan.subscriptions()
    ]

    def lost(method, name):
        if method == "PUT" and name == names[index]:
            raise requests.Timeout("Response lost after creation")

    http.after = lost
    with pytest.raises(TransportError):
        resources.provision()
    assert [c[1] for c in http.calls if c[0] == "PUT"] == names[: index + 1]
    assert store.value == resources.plan.manifest()
    http.after = lambda *args: None
    resources.cleanup()
    assert not http.resources
    assert len([c for c in http.calls if c[0] == "DELETE"]) == index + 1


@pytest.mark.parametrize(
    "corruption", [None, {}, {"version": True}, {"nonce": "b" * 32}]
)
def test_cleanup_refuses_missing_or_changed_manifest(setup, corruption):
    resources, http, store, _ = setup
    resources.provision()
    store.value = corruption
    http.calls.clear()
    with pytest.raises(Failure, match="manifest"):
        resources.cleanup()
    assert not http.calls
    assert len(http.resources) == 6


def test_manifest_is_rechecked_before_each_service_write(setup):
    resources, http, store, _ = setup

    def replace_after_first(method, name):
        if method == "PUT":
            store.value["nonce"] = "b" * 32

    http.after = replace_after_first
    with pytest.raises(Failure, match="manifest"):
        resources.provision()
    assert len([c for c in http.calls if c[0] == "PUT"]) == 1


def test_foreign_subscription_prevents_its_topic_deletion(setup):
    resources, http, _, _ = setup
    resources.provision()
    sub = resources.plan.subscriptions()[0]["name"]
    http.resources[sub]["labels"]["tier3-nonce"] = "b" * 32
    with pytest.raises(Failure, match="ownership"):
        resources.cleanup()
    assert not any(c[0] == "DELETE" for c in http.calls)


def test_own_settings_drift_can_be_cleaned_but_not_admitted(setup):
    resources, http, _, _ = setup
    resources.provision()
    sub = resources.plan.subscriptions()[0]["name"]
    http.resources[sub]["enableExactlyOnceDelivery"] = True
    with pytest.raises(Failure, match="settings"):
        resources.inspect()
    resources.cleanup()
    assert not http.resources


def test_guard_denial_prevents_delete_even_after_ownership_read(setup):
    resources, http, _, _ = setup
    resources.provision()

    def reject(phase, method, name):
        if method == "DELETE":
            raise Failure("Run lost the lock or cleanup budget")

    resources.before_operation = reject
    with pytest.raises(Failure, match="lock"):
        resources.cleanup()
    assert not any(c[0] == "DELETE" for c in http.calls)


def test_delete_timeout_is_not_retried_or_followed_by_parent_deletion(setup):
    resources, http, _, _ = setup
    resources.provision()

    def lost(method, name):
        if method == "DELETE":
            raise requests.Timeout("Delete response lost")

    http.after = lost
    with pytest.raises(TransportError):
        resources.cleanup()
    assert len([c for c in http.calls if c[0] == "DELETE"]) == 1
    assert all(p["name"] in http.resources for p in resources.plan.topics())
    http.after = lambda *args: None
    resources.cleanup()
    assert not http.resources


@pytest.mark.parametrize(
    "change",
    [
        {"ackDeadlineSeconds": 31},
        {"ackDeadlineSeconds": "30"},
        {"messageRetentionDuration": "86401s"},
        {"expirationPolicy": {}},
        {"enableExactlyOnceDelivery": True},
        {"enableMessageOrdering": True},
        {"retainAckedMessages": True},
        {"detached": True},
        {"enableExactlyOnceDelivery": 0},
        {"pushConfig": {"pushEndpoint": "https://example.invalid"}},
        {"bigqueryConfig": {"table": "foreign"}},
        {"filter": "attributes.x = '1'"},
        {"retryPolicy": {}},
        {"deadLetterPolicy": {"deadLetterTopic": "foreign"}},
        {"messageTransforms": [{}]},
        {"state": "STATE_UNSPECIFIED"},
        {"topicMessageRetentionDuration": "600s"},
    ],
)
def test_subscription_settings_are_read_back(setup, change):
    resources, http, _, _ = setup
    resources.provision()
    name = resources.plan.subscriptions()[0]["name"]
    http.resources[name].update(change)
    with pytest.raises(Failure):
        resources.inspect()


@pytest.mark.parametrize(
    "change",
    [
        {"messageStoragePolicy": {"allowedPersistenceRegions": ["europe-west1"]}},
        {
            "messageStoragePolicy": {
                "allowedPersistenceRegions": ["us-central1"],
                "enforceInTransit": True,
            }
        },
        {"messageRetentionDuration": "600s"},
        {"schemaSettings": {"schema": "foreign"}},
        {"kmsKeyName": "foreign"},
        {"messageTransforms": [{}]},
        {"ingestionDataSourceSettings": {"cloudStorage": {}}},
    ],
)
def test_topic_settings_are_read_back(setup, change):
    resources, http, _, _ = setup
    resources.provision()
    http.resources[resources.plan.topics()[0]["name"]].update(change)
    with pytest.raises(Failure):
        resources.inspect()


def test_duration_fractional_zeros_and_omitted_false_values_are_accepted(setup):
    resources, http, _, _ = setup
    resources.provision()
    expected = resources.plan.subscriptions()[0]
    actual = http.resources[expected["name"]]
    actual["messageRetentionDuration"] = "86400.000000000s"
    actual["expirationPolicy"]["ttl"] = "86400.000s"
    validate_resource(actual, expected)


@pytest.mark.parametrize("status", [301, 401, 403, 409, 429, 500, 503])
def test_service_errors_propagate_without_retries(setup, status):
    resources, http, store, _ = setup
    calls = []

    def reject(*args, **kwargs):
        calls.append(args)
        return reply({}, status)

    http.request = reject
    with pytest.raises(ApiError) as error:
        resources.provision()
    assert error.value.status == status
    assert len(calls) == 1
    assert store.value is None


def test_malformed_response_fails_before_mutation(setup):
    resources, http, store, _ = setup
    response = reply({})
    response._content = b"not-json"
    http.request = lambda *args, **kwargs: response
    with pytest.raises(Failure, match="Malformed"):
        resources.provision()
    assert store.value is None


def test_collision_between_preflight_and_create_is_not_adopted(setup):
    resources, http, store, _ = setup
    name = resources.plan.topics()[0]["name"]
    foreign = {**resources.plan.topics()[0], "labels": {"owner": "another"}}

    def collide(method, resource_name):
        if method == "PUT" and resource_name == name:
            http.resources[name] = copy.deepcopy(foreign)

    http.before = collide
    with pytest.raises(ApiError) as error:
        resources.provision()
    assert error.value.status == 409
    assert store.value == resources.plan.manifest()
    with pytest.raises(Failure, match="ownership"):
        resources.cleanup()
    assert http.resources == {name: foreign}
    assert not any(c[0] == "DELETE" for c in http.calls)


def test_manifest_write_collision_does_not_overwrite_or_create(setup):
    resources, http, store, _ = setup
    foreign = ResourcePlan("probe", "b" * 32).manifest()
    original = store.write

    def collide(*args, **kwargs):
        store.value = foreign
        return original(*args, **kwargs)

    store.write = collide
    with pytest.raises(ApiError) as error:
        resources.provision()
    assert error.value.status == 412
    assert store.value == foreign
    assert not any(c[0] == "PUT" for c in http.calls)


def test_manifest_version_boolean_does_not_equal_version_one(setup):
    resources, http, store, _ = setup
    resources.provision()
    store.value["version"] = True
    http.calls.clear()
    with pytest.raises(Failure, match="manifest"):
        resources.cleanup()
    assert not http.calls


def test_resource_reappearing_after_delete_stops_cleanup(setup):
    resources, http, _, _ = setup
    resources.provision()
    name = resources.plan.subscriptions()[0]["name"]

    def recreate(method, resource_name):
        if method == "DELETE" and resource_name == name:
            http.resources[name] = {"name": name, "labels": {"owner": "another"}}

    http.after = recreate
    with pytest.raises(Failure, match="remains"):
        resources.cleanup()
    assert len([c for c in http.calls if c[0] == "DELETE"]) == 1
    assert all(p["name"] in http.resources for p in resources.plan.topics())


def test_delete_not_found_still_checks_absence(setup):
    resources, http, _, _ = setup
    resources.provision()

    def disappear(method, name):
        if method == "DELETE":
            http.resources.pop(name)

    http.before = disappear
    assert len(resources.cleanup()) == 6
    assert not http.resources
    assert http.calls[-1][0] == "GET"


def test_guard_budgets_composite_production_storage_read(setup):
    from flink_tier3.google import Storage
    from google.api_core.exceptions import PreconditionFailed

    resources, http, _, _ = setup
    plan = resources.plan
    requests_made = []

    class Blob:
        def __init__(self):
            self.generation = len(requests_made) // 2 + 1

        def reload(self, *, timeout, retry):
            assert (timeout, retry) == (HTTP_TIMEOUT, None)
            requests_made.append("metadata")

        def download_as_bytes(self, *, if_generation_match, timeout, retry):
            assert (if_generation_match, timeout, retry) == (
                self.generation,
                HTTP_TIMEOUT,
                None,
            )
            requests_made.append("download")
            if len(requests_made) < 10:
                raise PreconditionFailed("Generation changed")
            return json.dumps(plan.manifest()).encode()

    class Client:
        def bucket(self, bucket):
            return self

        def blob(self, name):
            assert name == plan.manifest_path
            return Blob()

    # One record read may consume ten data requests, plus six Pub/Sub GETs.
    remaining = 16
    guarded = []

    def reserve(phase, method, name):
        nonlocal remaining
        cost = 10 if name == plan.manifest_path else 1
        if remaining < cost:
            raise Failure("Insufficient adapter-call request budget")
        remaining -= cost
        guarded.append((phase, method, name))

    operations = Resources(http, Storage(Client()), plan, reserve)
    assert len(operations.cleanup()) == 6
    assert requests_made == ["metadata", "download"] * 5
    assert len(guarded) == 7
    assert remaining == 0
    with pytest.raises(Failure, match="budget"):
        operations.cleanup()
    assert len(requests_made) == 10
    assert len(http.calls) == 6


def test_retained_intent_does_not_block_the_next_environment_lock(setup):
    from flink_tier3.records import EnvironmentLock
    from test_tier3_lifecycle import Store

    resources, http, _, _ = setup
    store = Store()
    lock = EnvironmentLock(store)
    owner, next_owner = {"run_id": "probe"}, {"run_id": "next"}
    lock.acquire(owner)
    active = "_control/runs/probe.json"
    generation = store.write(active, owner)
    resources = Resources(
        http, store, resources.plan, lambda *args: lock.assert_owner(owner)
    )
    resources.provision()
    resources.cleanup()
    assert store.read(resources.plan.manifest_path)[0] == resources.plan.manifest()
    with pytest.raises(Failure, match="Unfinished run"):
        lock.acquire(next_owner)
    store.delete(active, generation)
    lock.release(owner)
    lock.acquire(next_owner)
    lock.assert_owner(next_owner)
    lock.release(next_owner)
    assert store.read(resources.plan.manifest_path)[0] == resources.plan.manifest()


def test_owned_subscription_with_deleted_topic_remains_cleanable(setup):
    resources, http, _, _ = setup
    resources.provision()
    topic = resources.plan.topics()[0]["name"]
    sub = resources.plan.subscriptions()[0]["name"]
    del http.resources[topic]
    http.resources[sub]["topic"] = "_deleted-topic_"
    with pytest.raises(Failure, match="topic changed"):
        validate_resource(http.resources[sub], resources.plan.subscriptions()[0])
    with pytest.raises(Failure):
        resources.inspect()
    assert len(resources.cleanup()) == 6
    assert not http.resources


def test_cleanup_still_refuses_another_live_topic_binding(setup):
    resources, http, _, _ = setup
    resources.provision()
    sub = resources.plan.subscriptions()[0]["name"]
    http.resources[sub]["topic"] = "projects/flink-gcp/topics/foreign"
    with pytest.raises(Failure, match="topic changed"):
        resources.cleanup()
    assert not any(c[0] == "DELETE" for c in http.calls)


def test_inspection_names_the_absent_resource(setup):
    resources, http, _, _ = setup
    resources.provision()
    sub = resources.plan.subscriptions()[1]["name"]
    del http.resources[sub]
    with pytest.raises(Failure, match="resource is absent") as error:
        resources.inspect()
    assert sub in str(error.value)
