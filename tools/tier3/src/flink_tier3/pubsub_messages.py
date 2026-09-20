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
"""Single-attempt Pub/Sub data operations with durable evidence before ACK."""

import base64
import binascii
import json
import re

import google.auth.exceptions
import requests

from .common import ApiError, Failure, TransportError
from .policy import HTTP_TIMEOUT
from .pubsub import BASE, ResourcePlan

MAX_BATCH = 100
MAX_RESPONSE_BYTES = 1024 * 1024
MAX_PAYLOAD_BYTES = 4096
MAX_ID_BYTES = 1024
MAX_ACK_ID_BYTES = 4096


def _integer(value, low, high, name):
    if type(value) is not int or not low <= value <= high:
        raise Failure("Invalid Pub/Sub " + name)


def _identifier(value, limit, name):
    if not isinstance(value, str) or not value:
        raise Failure("Invalid Pub/Sub " + name)
    try:
        encoded = value.encode("utf-8")
    except UnicodeError as error:
        raise Failure("Invalid Pub/Sub " + name) from error
    if len(encoded) > limit:
        raise Failure("Invalid Pub/Sub " + name)
    return value


def _encode(value):
    return base64.urlsafe_b64encode(value).decode("ascii").rstrip("=")


class Messages:
    """Internal helper; construction does not authenticate or admit a run.

    The caller authenticates ``actor``, freezes the logical input domain, and
    proves ownership, prepared resources, exclusive actor authority, and budgets
    through ``before_operation(phase, method, name)`` before every operation.
    The guard must reserve each complete call, including credential and guard I/O.
    Use the shared authorized HTTP session without retries and the GCS adapter.
    Evidence objects are create-only and must survive until final assessment.
    """

    def __init__(
        self, http, store, plan, *, actor, records_per_subscription, before_operation
    ):
        if not isinstance(plan, ResourcePlan) or not callable(before_operation):
            raise Failure(
                "Pub/Sub messages require a resource plan and lifecycle guard"
            )
        if actor not in ("runner", "supervisor"):
            raise Failure("Pub/Sub messages require a runner or supervisor actor")
        _integer(records_per_subscription, 1, 10000, "logical input count")
        self.http, self.store, self.plan = http, store, plan
        self.actor, self.records = actor, records_per_subscription
        self.before_operation = before_operation
        self.prefix = f"runs/{plan.run_id}/pubsub/messages/{plan.nonce}"

    def _role(self, expected):
        if self.actor != expected:
            raise Failure("Pub/Sub operation requires the " + expected + " actor")

    def _save(self, phase, path, value):
        self.before_operation(phase, "PUT", path)
        self.store.write(path, value, generation="0")

    def _post(self, phase, name, body):
        self.before_operation(phase, "POST", name)
        try:
            with self.http.request(
                "POST",
                BASE + name,
                json=body,
                timeout=HTTP_TIMEOUT,
                allow_redirects=False,
                stream=True,
            ) as response:
                if not 200 <= response.status_code < 300:
                    raise ApiError(response.status_code, "POST", name)
                data = bytearray()
                for chunk in response.iter_content(chunk_size=8192):
                    data.extend(chunk)
                    if len(data) > MAX_RESPONSE_BYTES:
                        raise Failure("Pub/Sub response exceeds 1 MiB")
                try:
                    value = json.loads(data) if data else {}
                except (ValueError, UnicodeError) as error:
                    raise Failure("Malformed Pub/Sub response") from error
                if not isinstance(value, dict):
                    raise Failure("Malformed Pub/Sub response")
                return value
        except (
            requests.exceptions.RequestException,
            google.auth.exceptions.GoogleAuthError,
        ) as error:
            raise TransportError(
                "Pub/Sub data request failed: " + type(error).__name__
            ) from error

    def _intent(self, kind, resource, request):
        return {
            "version": 1,
            "kind": kind,
            "run_id": self.plan.run_id,
            "nonce": self.plan.nonce,
            "records_per_subscription": self.records,
            "actor": self.actor,
            "resource": resource,
            "request": request,
        }

    def publish(self, input_index, start, count):
        """Publish one logical interval once; an existing intent refuses replay.

        Different overlapping intervals can publish duplicate logical inputs.
        The caller freezes disjoint cohorts or explicitly budgets such repeats.
        A missing response receipt is an unknown outcome, never proof of absence.
        """
        self._role("runner")
        _integer(input_index, 0, 1, "input index")
        _integer(start, 0, self.records - 1, "sequence start")
        _integer(count, 1, min(MAX_BATCH, self.records - start), "publish count")
        topic = self.plan.topics()[input_index]["name"]
        request = {
            "messages": [
                {
                    "data": base64.b64encode(
                        f"v1|{self.plan.run_id}|{input_index}|{sequence}".encode()
                    ).decode("ascii")
                }
                for sequence in range(start, start + count)
            ]
        }
        path = f"{self.prefix}/input/{input_index}/{start}-{count}"
        self._save(
            "publish", path + "/intent.json", self._intent("publish", topic, request)
        )
        response = self._post("publish", topic + ":publish", request)
        self._save("publish", path + "/response.json", response)
        ids = response.get("messageIds")
        if not isinstance(ids, list) or len(ids) != count:
            raise Failure("Pub/Sub publish response does not cover the input batch")
        for value in ids:
            _identifier(value, MAX_ID_BYTES, "input message ID")
        if len(set(ids)) != len(ids):
            raise Failure("Pub/Sub publish response repeats a message ID")
        return {"path": path, "message_ids": ids}

    def collect(self, batch_id, *, max_messages=MAX_BATCH):
        """Pull once and retain all output identities before acknowledging.

        A retry uses a new batch ID and may observe redelivery. Never deduplicate
        across batches; an empty pull does not establish an empty subscription.
        """
        self._role("supervisor")
        if not isinstance(batch_id, str) or not re.fullmatch(
            r"[a-z0-9][a-z0-9-]{0,39}", batch_id
        ):
            raise Failure("Invalid Pub/Sub collection batch ID")
        _integer(max_messages, 1, MAX_BATCH, "pull count")
        subscription = self.plan.subscriptions()[2]["name"]
        request = {"maxMessages": max_messages}
        path = f"{self.prefix}/output/{batch_id}"
        self._save(
            "collect",
            path + "/intent.json",
            self._intent("pull", subscription, request),
        )
        response = self._post("collect", subscription + ":pull", request)
        self._save("collect", path + "/response.json", response)
        received = response.get("receivedMessages", [])
        if not isinstance(received, list) or len(received) > max_messages:
            raise Failure("Pub/Sub pull response exceeds the requested batch")
        ack_ids, lines = [], []
        for item in received:
            if not isinstance(item, dict) or not isinstance(item.get("message"), dict):
                raise Failure("Malformed Pub/Sub received message")
            ack_ids.append(
                _identifier(item.get("ackId"), MAX_ACK_ID_BYTES, "acknowledgement ID")
            )
            message = item["message"]
            message_id = _identifier(
                message.get("messageId"), MAX_ID_BYTES, "output message ID"
            )
            data = message.get("data", "")
            if not isinstance(data, str) or len(data) > 4 * (
                (MAX_PAYLOAD_BYTES + 2) // 3
            ):
                raise Failure("Pub/Sub output payload exceeds the collector limit")
            try:
                payload = base64.b64decode(
                    data + "=" * (-len(data) % 4), altchars=b"-_", validate=True
                )
            except (ValueError, binascii.Error) as error:
                raise Failure("Malformed Pub/Sub output encoding") from error
            if len(payload) > MAX_PAYLOAD_BYTES:
                raise Failure("Pub/Sub output payload exceeds the collector limit")
            lines.append(
                _encode(message_id.encode("utf-8")) + "\t" + _encode(payload) + "\n"
            )
        evidence = {
            "version": 1,
            "subscription": subscription,
            "count": len(received),
            "tsv": "".join(lines),
        }
        self._save("collect", path + "/observations.json", evidence)
        if ack_ids:
            acknowledged = self._post(
                "acknowledge", subscription + ":acknowledge", {"ackIds": ack_ids}
            )
            if acknowledged != {}:
                raise Failure("Malformed Pub/Sub acknowledgement response")
            self._save(
                "acknowledge",
                path + "/acknowledged.json",
                {"version": 1, "count": len(ack_ids)},
            )
        return {"path": path, **evidence}
