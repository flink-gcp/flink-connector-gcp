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
"""Message evidence ordering and ambiguous outcomes without cloud credentials."""

import base64
import copy
import json

import pytest
import requests
from flink_tier3.common import ApiError, Failure, TransportError
from flink_tier3.pubsub import BASE, HTTP_TIMEOUT, ResourcePlan
from flink_tier3.pubsub_messages import MAX_RESPONSE_BYTES, Messages


class Store:
    def __init__(self, events):
        self.objects = {}
        self.events = events
        self.before = lambda path: None
        self.after = lambda path: None

    def write(self, path, value, generation):
        assert generation == "0"
        self.events.append(("store", path))
        self.before(path)
        if path in self.objects:
            raise ApiError(412, "POST", path)
        self.objects[path] = json.loads(json.dumps(value))
        self.after(path)
        return "1"


class Response:
    def __init__(self, value=None, *, status=200, raw=None):
        self.data = json.dumps(value).encode() if raw is None else raw
        self.status_code = status
        self.closed = False

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.closed = True

    def iter_content(self, chunk_size):
        for offset in range(0, len(self.data), chunk_size):
            yield self.data[offset : offset + chunk_size]


class Http:
    def __init__(self, events):
        self.events, self.responses, self.calls = events, [], []

    def request(self, method, url, *, json, timeout, allow_redirects, stream):
        assert method == "POST" and timeout == HTTP_TIMEOUT
        assert not allow_redirects and stream
        assert url.startswith(BASE)
        self.events.append(("http", url))
        self.calls.append((url.removeprefix(BASE), copy.deepcopy(json)))
        response = self.responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response


@pytest.fixture
def setup():
    events = []
    store, http = Store(events), Http(events)
    plan = ResourcePlan("messages", "a" * 32)

    def make(actor="runner", guard=None):
        return Messages(
            http,
            store,
            plan,
            actor=actor,
            records_per_subscription=1000,
            before_operation=guard or (lambda *args: events.append(("guard", args))),
        )

    return make, http, store, events


def output(message_id="output-1", payload=b"relay-output", ack_id="ack-1"):
    return {
        "ackId": ack_id,
        "message": {
            "messageId": message_id,
            "data": base64.b64encode(payload).decode(),
            "publishTime": "2026-09-20T00:00:00Z",
        },
    }


def fail(message="lost response"):
    raise Failure(message)


def test_publish_retains_exact_input_and_ordered_service_ids_before_return(setup):
    make, http, store, events = setup
    http.responses.append(Response({"messageIds": ["second", "first"]}))
    result = make().publish(1, 998, 2)
    path = result["path"]
    assert path.endswith("/input/1/998-2")
    intent = store.objects[path + "/intent.json"]
    assert intent["run_id"] == "messages" and intent["nonce"] == "a" * 32
    assert intent["records_per_subscription"] == 1000 and intent["actor"] == "runner"
    assert [base64.b64decode(m["data"]) for m in intent["request"]["messages"]] == [
        b"v1|messages|1|998",
        b"v1|messages|1|999",
    ]
    assert http.calls == [
        ("projects/flink-gcp/topics/t3-messages-in-1:publish", intent["request"])
    ]
    assert result["message_ids"] == ["second", "first"]
    assert store.objects[path + "/response.json"]["messageIds"] == result["message_ids"]
    assert [event[0] for event in events] == [
        "guard",
        "store",
        "guard",
        "http",
        "guard",
        "store",
    ]


def test_ambiguous_publish_is_not_repeated_after_restart(setup):
    make, http, store, _ = setup
    http.responses.append(requests.exceptions.Timeout())
    with pytest.raises(TransportError):
        make().publish(0, 0, 1)
    with pytest.raises(ApiError) as error:
        make().publish(0, 0, 1)
    assert error.value.status == 412
    assert len(http.calls) == 1
    assert (
        next(iter(store.objects)).endswith("/intent.json") and len(store.objects) == 1
    )


@pytest.mark.parametrize("accepted", [False, True])
def test_publication_intent_failure_sends_nothing(setup, accepted):
    make, http, store, _ = setup
    if accepted:
        store.after = lambda path: fail()
    else:
        store.before = lambda path: fail()
    with pytest.raises(Failure):
        make().publish(0, 0, 1)
    assert not http.calls
    assert bool(store.objects) is accepted
    if accepted:
        store.after = lambda path: None
        with pytest.raises(ApiError):
            make().publish(0, 0, 1)
        assert not http.calls


def test_missing_publication_receipt_is_not_proof_of_no_publication(setup):
    make, http, store, _ = setup
    http.responses.append(Response({"messageIds": ["accepted"]}))
    store.before = lambda path: fail() if path.endswith("/response.json") else None
    with pytest.raises(Failure):
        make().publish(0, 0, 1)
    with pytest.raises(ApiError):
        make().publish(0, 0, 1)
    assert len(http.calls) == 1 and len(store.objects) == 1


@pytest.mark.parametrize(
    "response",
    [
        {},
        {"messageIds": []},
        {"messageIds": "x"},
        {"messageIds": [""]},
        {"messageIds": [3]},
        {"messageIds": ["x" * 1025]},
    ],
)
def test_malformed_publish_receipt_is_retained_and_refuses_replay(setup, response):
    make, http, store, _ = setup
    http.responses.append(Response(response))
    with pytest.raises(Failure):
        make().publish(0, 0, 1)
    assert any(value == response for value in store.objects.values())
    with pytest.raises(ApiError):
        make().publish(0, 0, 1)
    assert len(http.calls) == 1


def test_duplicate_publish_ids_are_rejected(setup):
    make, http, _, _ = setup
    http.responses.append(Response({"messageIds": ["same", "same"]}))
    with pytest.raises(Failure, match="repeats"):
        make().publish(0, 0, 2)


def test_collect_keeps_every_delivery_before_ack_and_never_deduplicates(setup):
    make, http, store, events = setup
    response = {"receivedMessages": [output(), output(ack_id="ack-2")]}
    http.responses.extend([Response(response), Response({})])
    result = make("supervisor").collect("first", max_messages=2)
    path = result["path"]
    assert http.calls == [
        ("projects/flink-gcp/subscriptions/t3-messages-out:pull", {"maxMessages": 2}),
        (
            "projects/flink-gcp/subscriptions/t3-messages-out:acknowledge",
            {"ackIds": ["ack-1", "ack-2"]},
        ),
    ]
    assert store.objects[path + "/response.json"] == response
    lines = result["tsv"].splitlines()
    assert len(lines) == 2 and lines[0] == lines[1]
    assert lines[0] == "b3V0cHV0LTE\tcmVsYXktb3V0cHV0"
    assert store.objects[path + "/observations.json"]["tsv"] == result["tsv"]
    writes = [(i, event[1]) for i, event in enumerate(events) if event[0] == "store"]
    evidence_index = next(i for i, p in writes if p.endswith("/observations.json"))
    ack_index = next(
        i
        for i, e in enumerate(events)
        if e[0] == "http" and e[1].endswith(":acknowledge")
    )
    assert evidence_index < ack_index
    assert store.objects[path + "/acknowledged.json"] == {"version": 1, "count": 2}
    assert [(e[1][0], e[1][1]) for e in events if e[0] == "guard"] == [
        ("collect", "PUT"),
        ("collect", "POST"),
        ("collect", "PUT"),
        ("collect", "PUT"),
        ("acknowledge", "POST"),
        ("acknowledge", "PUT"),
    ]


@pytest.mark.parametrize("suffix", ["response.json", "observations.json"])
@pytest.mark.parametrize("accepted", [False, True])
def test_failed_output_evidence_never_acknowledges(setup, suffix, accepted):
    make, http, store, _ = setup
    http.responses.append(Response({"receivedMessages": [output()]}))
    callback = lambda path: fail() if path.endswith("/" + suffix) else None
    if accepted:
        store.after = callback
    else:
        store.before = callback
    with pytest.raises(Failure):
        make("supervisor").collect("first")
    assert len(http.calls) == 1
    assert any(path.endswith("/" + suffix) for path in store.objects) is accepted


def test_lost_ack_response_retains_evidence_and_new_batch_keeps_redelivery(setup):
    make, http, store, _ = setup
    http.responses.extend(
        [Response({"receivedMessages": [output()]}), requests.exceptions.Timeout()]
    )
    with pytest.raises(TransportError):
        make("supervisor").collect("first")
    first = next(
        value
        for path, value in store.objects.items()
        if path.endswith("/observations.json")
    )
    assert not any(path.endswith("/acknowledged.json") for path in store.objects)
    with pytest.raises(ApiError):
        make("supervisor").collect("first")
    http.responses.extend(
        [Response({"receivedMessages": [output(ack_id="new-ack")]}), Response({})]
    )
    second = make("supervisor").collect("second")
    assert first["tsv"] == second["tsv"] and len(http.calls) == 4


@pytest.mark.parametrize("response", [{}, {"receivedMessages": []}])
def test_empty_pull_is_retained_without_ack(setup, response):
    make, http, store, _ = setup
    http.responses.append(Response(response))
    result = make("supervisor").collect("empty")
    assert result["count"] == 0 and result["tsv"] == ""
    assert len(http.calls) == 1 and len(store.objects) == 3


@pytest.mark.parametrize(
    "item",
    [
        None,
        {},
        {"message": {}},
        output(ack_id=""),
        output(message_id=""),
        {"ackId": "a", "message": {"messageId": "id", "data": "%%%"}},
        output(payload=b"x" * 4097),
    ],
)
def test_malformed_output_is_retained_without_ack(setup, item):
    make, http, store, _ = setup
    response = {"receivedMessages": [item]}
    http.responses.append(Response(response))
    with pytest.raises(Failure):
        make("supervisor").collect("bad")
    assert len(http.calls) == 1
    assert any(value == response for value in store.objects.values())


def test_foreign_and_binary_payloads_are_preserved_for_the_offline_oracle(setup):
    make, http, _, _ = setup
    http.responses.extend(
        [
            Response({"receivedMessages": [output(payload=b"foreign\x00\xff")]}),
            Response({}),
        ]
    )
    result = make("supervisor").collect("foreign")
    encoded = result["tsv"].split("\t")[1].strip()
    assert (
        base64.urlsafe_b64decode(encoded + "=" * (-len(encoded) % 4))
        == b"foreign\x00\xff"
    )


@pytest.mark.parametrize(
    "response",
    [
        Response(raw=b"not-json"),
        Response([]),
        Response({"padding": "x" * MAX_RESPONSE_BYTES}),
        Response({}, status=403),
        Response({}, status=302),
    ],
)
def test_transport_refusals_close_response_and_do_not_ack(setup, response):
    make, http, _, _ = setup
    http.responses.append(response)
    with pytest.raises(Failure) as error:
        make("supervisor").collect("bad")
    if len(response.data) > MAX_RESPONSE_BYTES:
        assert "exceeds 1 MiB" in str(error.value)
    assert response.closed and len(http.calls) == 1


@pytest.mark.parametrize(
    "stage", ["intent", "pull", "response", "observations", "ack", "acknowledged"]
)
def test_each_output_operation_is_guarded(setup, stage):
    make, http, _, events = setup
    http.responses.extend([Response({"receivedMessages": [output()]}), Response({})])
    stop = ["intent", "pull", "response", "observations", "ack", "acknowledged"].index(
        stage
    )
    seen = []

    def guard(*args):
        seen.append(args)
        if len(seen) == stop + 1:
            fail("guard refused")

    with pytest.raises(Failure, match="guard refused"):
        make("supervisor", guard).collect("guarded")
    assert len(events) == stop
    assert len(seen) == stop + 1


@pytest.mark.parametrize(
    "args",
    [
        (True, 0, 1),
        (2, 0, 1),
        (0, -1, 1),
        (0, 1000, 1),
        (0, 0, 0),
        (0, 0, 101),
        (0, 999, 2),
        (0, 0, True),
    ],
)
def test_invalid_publish_domain_performs_no_io(setup, args):
    make, _, _, events = setup
    with pytest.raises(Failure):
        make().publish(*args)
    assert not events


def test_wrong_actor_cannot_use_the_other_data_role(setup):
    make, _, _, events = setup
    with pytest.raises(Failure, match="runner"):
        make("supervisor").publish(0, 0, 1)
    with pytest.raises(Failure, match="supervisor"):
        make().collect("one")
    assert not events


@pytest.mark.parametrize(
    "batch,count",
    [("../escape", 1), ("", 1), ("a" * 41, 1), ("ok", 0), ("ok", 101), ("ok", True)],
)
def test_invalid_collection_performs_no_io(setup, batch, count):
    make, _, _, events = setup
    with pytest.raises(Failure):
        make("supervisor").collect(batch, max_messages=count)
    assert not events


@pytest.mark.parametrize("received", [None, {}, [output(), output()]])
def test_invalid_received_population_is_not_acknowledged(setup, received):
    make, http, store, _ = setup
    http.responses.append(Response({"receivedMessages": received}))
    with pytest.raises(Failure):
        make("supervisor").collect("one", max_messages=1)
    assert len(http.calls) == 1 and len(store.objects) == 2


def test_unicode_surrogate_id_is_a_controlled_failure(setup):
    make, http, store, _ = setup
    http.responses.append(Response({"receivedMessages": [output(message_id="\ud800")]}))
    with pytest.raises(Failure, match="output message ID"):
        make("supervisor").collect("one")
    assert len(http.calls) == 1 and len(store.objects) == 2


def test_competing_publisher_cannot_send_the_same_interval(setup):
    make, http, store, _ = setup
    http.responses.append(Response({"messageIds": ["winner"]}))

    def compete(path):
        store.before = lambda path: None
        make().publish(0, 0, 1)

    store.before = compete
    with pytest.raises(ApiError):
        make().publish(0, 0, 1)
    assert len(http.calls) == 1


def test_ack_receipt_failure_retains_output_and_refuses_same_batch(setup):
    make, http, store, _ = setup
    http.responses.extend([Response({"receivedMessages": [output()]}), Response({})])
    store.before = lambda path: fail() if path.endswith("/acknowledged.json") else None
    with pytest.raises(Failure):
        make("supervisor").collect("first")
    assert len(http.calls) == 2 and len(store.objects) == 3
    with pytest.raises(ApiError):
        make("supervisor").collect("first")
    assert len(http.calls) == 2


def test_shared_storage_adapter_uses_create_only_uploads_and_no_retries(setup):
    from flink_tier3.google import Storage
    from google.api_core.exceptions import PreconditionFailed

    _, http, _, events = setup
    saved = {}

    class Blob:
        generation = 1

        def __init__(self, path):
            self.path = path

        def upload_from_string(self, data, **options):
            assert options == {
                "content_type": "application/json",
                "if_generation_match": 0,
                "timeout": HTTP_TIMEOUT,
                "retry": None,
                "checksum": None,
            }
            events.append(("upload", self.path))
            if self.path in saved:
                raise PreconditionFailed("Already exists")
            saved[self.path] = json.loads(data)

    class Client:
        def bucket(self, name):
            assert name == "flink-gcp-tier3-evidence"
            return self

        def blob(self, name):
            return Blob(name)

    def make():
        return Messages(
            http,
            Storage(Client()),
            ResourcePlan("messages", "a" * 32),
            actor="supervisor",
            records_per_subscription=1000,
            before_operation=lambda *args: events.append(("guard", args)),
        )

    http.responses.extend([Response({"receivedMessages": [output()]}), Response({})])
    result = make().collect("first")
    assert saved[result["path"] + "/observations.json"]["tsv"] == result["tsv"]
    assert len(saved) == 4
    with pytest.raises(ApiError) as error:
        make().collect("first")
    assert error.value.status == 412 and len(http.calls) == 2


def test_exact_response_byte_limit_is_accepted(setup):
    make, http, store, _ = setup
    overhead = len(json.dumps({"padding": ""}).encode())
    response = Response({"padding": "x" * (MAX_RESPONSE_BYTES - overhead)})
    assert len(response.data) == MAX_RESPONSE_BYTES
    http.responses.append(response)
    result = make("supervisor").collect("boundary")
    assert result["count"] == 0 and response.closed
    assert len(store.objects) == 3


@pytest.mark.parametrize("data", ["+/8=", "+/8", "-_8=", "-_8"])
def test_protojson_base64_variants_preserve_identical_output_bytes(setup, data):
    make, http, store, _ = setup
    item = output()
    item["message"]["data"] = data
    http.responses.extend([Response({"receivedMessages": [item]}), Response({})])
    result = make("supervisor").collect("encoding")
    assert result["tsv"] == "b3V0cHV0LTE\t-_8\n"
    assert (
        store.objects[result["path"] + "/response.json"]["receivedMessages"][0][
            "message"
        ]["data"]
        == data
    )
    assert len(http.calls) == 2


@pytest.mark.parametrize(
    "overrides",
    [
        {"plan": None},
        {"before_operation": None},
        {"actor": "workload"},
        {"records_per_subscription": 0},
        {"records_per_subscription": 10001},
        {"records_per_subscription": True},
    ],
)
def test_invalid_constructor_cannot_start_io(setup, overrides):
    _, http, store, events = setup
    options = {
        "plan": ResourcePlan("messages", "a" * 32),
        "actor": "runner",
        "records_per_subscription": 1000,
        "before_operation": lambda *args: None,
    }
    options.update(overrides)
    with pytest.raises(Failure):
        Messages(http, store, **options)
    assert not events


def test_nonempty_ack_response_leaves_evidence_without_success_receipt(setup):
    make, http, store, _ = setup
    http.responses.extend(
        [Response({"receivedMessages": [output()]}), Response({"unexpected": True})]
    )
    with pytest.raises(Failure, match="acknowledgement response"):
        make("supervisor").collect("ack-error")
    assert len(http.calls) == 2
    assert any(path.endswith("/observations.json") for path in store.objects)
    assert not any(path.endswith("/acknowledged.json") for path in store.objects)


@pytest.mark.parametrize("field,limit", [("messageId", 1024), ("ackId", 4096)])
@pytest.mark.parametrize("extra", [0, 1])
def test_collection_id_caps_count_utf8_bytes(setup, field, limit, extra):
    make, http, store, _ = setup
    item = output()
    target = item["message"] if field == "messageId" else item
    target[field] = "é" * (limit // 2) + "x" * extra
    http.responses.extend([Response({"receivedMessages": [item]}), Response({})])
    if extra:
        with pytest.raises(Failure, match="ID"):
            make("supervisor").collect("id-limit")
        assert len(http.calls) == 1 and len(store.objects) == 2
    else:
        result = make("supervisor").collect("id-limit")
        assert result["count"] == 1 and len(http.calls) == 2
