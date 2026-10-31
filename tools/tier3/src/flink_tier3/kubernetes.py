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
"""Tier-3 lifecycle kubernetes."""

from __future__ import annotations

import json
import ssl
import time
import urllib.parse
from functools import partial

import urllib3

from kubernetes import client as kubernetes_client

from .common import (
    ApiError,
    ConnectionDropped,
    Failure,
    ReadCeilingExceeded,
    TransportError,
    encoded,
)
from .policy import HTTP_TIMEOUT, MIB, NAMESPACES, NONCE, READ_RETRY_DELAYS

COLLECTIONS = {
    "Pod": ("/api/v1", "pods"),
    "Service": ("/api/v1", "services"),
    "ConfigMap": ("/api/v1", "configmaps"),
    "PersistentVolumeClaim": ("/api/v1", "persistentvolumeclaims"),
    "ResourceQuota": ("/api/v1", "resourcequotas"),
    "Event": ("/api/v1", "events"),
    "Deployment": ("/apis/apps/v1", "deployments"),
    "ReplicaSet": ("/apis/apps/v1", "replicasets"),
    "StatefulSet": ("/apis/apps/v1", "statefulsets"),
    "Job": ("/apis/batch/v1", "jobs"),
    "CronJob": ("/apis/batch/v1", "cronjobs"),
    "FlinkDeployment": ("/apis/flink.apache.org/v1beta1", "flinkdeployments"),
    "FlinkSessionJob": ("/apis/flink.apache.org/v1beta1", "flinksessionjobs"),
    "FlinkStateSnapshot": ("/apis/flink.apache.org/v1beta1", "flinkstatesnapshots"),
    "FlinkBlueGreenDeployment": (
        "/apis/flink.apache.org/v1beta1",
        "flinkbluegreendeployments",
    ),
}


# A mutation answered with one of these may still have been applied.
UNCERTAIN_STATUSES = (500, 502, 503, 504)
# A control-plane blip answers a read with one of these, or with the 429 of
# API Priority and Fairness, which refuses a request before the API server
# processes it; pilot bq1312-alo-10-a6 lost a run to a single 503 after both
# recoveries passed, and ct1778-rescale-out-a6 to a single 429 during its
# JobManager failover.
TRANSIENT_READ_STATUSES = (429, *UNCERTAIN_STATUSES)


# What a mutation's settlement returns when the read-back shows the earlier
# attempt did not land, so the same request is sent again.
RESEND = object()


INVENTORY = tuple(k for k in COLLECTIONS if k not in ("ResourceQuota", "Event"))


def retry_after(headers):
    """The seconds a `Retry-After` header names, or None.

    The API server sends whole seconds; an HTTP date, a value too long for
    `int` to convert, or anything else falls back to the scheduled delay.
    """
    value = (headers or {}).get("Retry-After", "").strip()
    if not value.isdecimal():
        return None
    try:
        return int(value)
    except ValueError:
        return None


def status(failure):
    return failure.status if isinstance(failure, ApiError) else None


def repeats_read(failure):
    """Whether a read is sent again: a transient status, or a dropped connection.

    A connection dropped before an answer is repeated over a new one, though
    the drop can come as late as a timeout would have. Any other transport
    failure is not repeated: a timed-out read has already spent its whole
    timeout, and repeating it would multiply that against the caller's
    deadline.
    """
    return isinstance(failure, ConnectionDropped) or (
        status(failure) in TRANSIENT_READ_STATUSES
    )


def throttled(failure):
    """Whether a mutation is sent again as it is: only a 429 was surely refused."""
    return status(failure) == 429


def may_have_landed(failure):
    """Whether a mutation that met `failure` may still have been applied.

    Any transport failure may have reached the API server, and a 500, 502,
    503 or 504 may have been answered after it applied the change.
    """
    return isinstance(failure, TransportError) or status(failure) in UNCERTAIN_STATUSES


def dropped(error):
    """Whether urllib3 lost the connection before an answer, rather than timing out.

    A reset or aborted connection surfaces as a `ProtocolError`, and a TLS
    connection closed during its handshake as an `SSLError` wrapping
    `ssl.SSLEOFError`, each bare or as the reason of a `MaxRetryError`. A
    request body whose upload timed out also surfaces as a `ProtocolError`,
    wrapping the socket's `TimeoutError`; it has spent its timeout, so it is
    not a dropped connection.
    """
    reason = getattr(error, "reason", None) or error
    if isinstance(reason, urllib3.exceptions.SSLError):
        return bool(reason.args) and isinstance(reason.args[0], ssl.SSLEOFError)
    return isinstance(reason, urllib3.exceptions.ProtocolError) and not any(
        isinstance(cause, TimeoutError) for cause in reason.args
    )


def pointer(document, path):
    """Whether a JSON Pointer through objects resolves in `document`, and to what."""
    value = document
    for token in path.split("/")[1:]:
        if not isinstance(value, dict) or token not in value:
            return False, None
        value = value[token]
    return True, value


def reflected(document, operation):
    """Whether `document` holds the value a JSON Patch operation set.

    The rig's patches carry only `add` and `replace` operations after their
    tests, and either leaves its value at its path, unless the API server
    omits the field when it is empty, as for a scale's zero replicas or an
    empty finalizer list; such a landed patch reads as not reflected.
    """
    found, value = pointer(document, operation["path"])
    return found and value == operation["value"]


def created_by(stored, sent):
    """Whether `stored`, read under the name a create sent, is the object it sent.

    Every label and annotation the create set must read back unchanged. Only
    an object carrying the run's approval nonce can be told apart from one
    another run left under the same name, so a create without it is never
    claimed.
    """
    meta, read = sent["metadata"], stored.get("metadata", {})
    return NONCE in meta.get("annotations", {}) and all(
        read.get(field, {}).get(key) == value
        for field in ("labels", "annotations")
        for key, value in meta.get(field, {}).items()
    )


class KubernetesTransport:
    def __init__(self, endpoint, token, ca=None):
        configuration = kubernetes_client.Configuration(host=endpoint)
        configuration.proxy = None
        configuration.ssl_ca_cert = ca
        configuration.retries = urllib3.Retry(total=0, redirect=0)
        configuration.api_key["BearerToken"] = ""
        configuration.api_key_prefix["BearerToken"] = "Bearer"
        configuration.refresh_api_key_hook = lambda config: config.api_key.update(
            BearerToken=token()
        )
        self.client = kubernetes_client.ApiClient(configuration)

    def request(
        self, method, url, body=None, content_type="application/json", limit=None
    ):
        parts = urllib.parse.urlsplit(url)
        try:
            response = self.client.call_api(
                parts.path,
                method,
                query_params=urllib.parse.parse_qsl(parts.query),
                header_params={"Content-Type": content_type},
                body=body,
                auth_settings=["BearerToken"],
                _preload_content=False,
                _request_timeout=(HTTP_TIMEOUT, HTTP_TIMEOUT),
            )
            try:
                data = response.read() if limit is None else response.read(limit + 1)
                if limit is not None and len(data) > limit:
                    raise ReadCeilingExceeded("Response exceeds its read ceiling")
                return data
            finally:
                response.close()
                response.release_conn()
        except kubernetes_client.exceptions.ApiException as error:
            raise ApiError(
                error.status, method, parts.path, retry_after(error.headers)
            ) from error
        except urllib3.exceptions.HTTPError as error:
            failure = ConnectionDropped if dropped(error) else TransportError
            raise failure(
                f"{method} Kubernetes request failed: {type(error).__name__}"
            ) from error

    def json(self, method, url, body=None, **kwargs):
        data = self.request(method, url, body, **kwargs)
        return json.loads(data) if data else {}


class Kubernetes:
    """Every Kubernetes API call of the runner and supervisor, on one retry schedule."""

    def __init__(self, endpoint, http, sleep=time.sleep):
        self.endpoint, self.http, self.sleep = endpoint.rstrip("/"), http, sleep

    @staticmethod
    def backoff(delay, failure):
        """The scheduled delay, or a longer `Retry-After`, up to the longest one."""
        if isinstance(failure, ApiError) and failure.retry_after is not None:
            return min(max(delay, failure.retry_after), READ_RETRY_DELAYS[-1])
        return delay

    def send(
        self, call, repeated, settle=None, answers_if_landed=(), before_repeat=None
    ):
        """Run `call`, repeating it on `READ_RETRY_DELAYS` while that can help.

        A failure `repeated` names is sent again after the next delay, or a
        longer `Retry-After` up to the longest one. With `settle`, a failure
        after which the mutation `may_have_landed` is settled instead: after
        the next delay `settle(failure)` reads the object back and returns the
        call's result, raises what the API server would have answered, or
        returns `RESEND` to send the call again; a read-back that fails is
        raised chained to the failure it settles. After a timeout `RESEND`
        raises the timeout rather than sending the call again, since it has
        already spent its time, and after any other transport failure but a
        dropped connection, since a repeat would meet the same failure. After
        such a failure, a repeat answered with a status in `answers_if_landed`
        may have met the earlier attempt's doing, so that answer is settled
        the same way. When the schedule is spent, or a later answer is neither
        repeated nor settled, the latest failure after which the mutation may
        have landed is raised, chained to that answer, so a caller's own
        read-back still applies; if there was none, the failure itself is.
        `before_repeat` runs before every repeat, not before the first attempt
        or a read-back, so a caller that checked admission or ownership once
        before the call is not sent again after it closed. Its failure is
        raised chained to the latest failure after which the mutation may have
        landed, or else to the one the repeat would have followed; a failure
        that has a cause of its own keeps it, since the rig's transient-failure
        classifiers follow causes, and names that Kubernetes failure in a note
        instead.
        """
        delays, doubtful, failure = iter(READ_RETRY_DELAYS), None, None
        while True:
            if failure is not None and before_repeat is not None:
                try:
                    before_repeat()
                except Failure as error:
                    if error.__cause__ is None:
                        raise error from (doubtful or failure)
                    error.add_note(
                        f"Raised before repeating after: {doubtful or failure}"
                    )
                    raise
            try:
                return call()
            except (ApiError, TransportError) as error:
                failure = error
            if settle is not None and may_have_landed(failure):
                doubtful = failure
            settling = doubtful is failure or (
                doubtful is not None and status(failure) in answers_if_landed
            )
            delay = next(delays, None) if settling or repeated(failure) else None
            if delay is None:
                if doubtful is None or doubtful is failure:
                    raise failure
                raise doubtful from failure
            self.sleep(self.backoff(delay, failure))
            if settling:
                try:
                    result = settle(failure)
                except Failure as error:
                    raise error from failure
                if result is not RESEND:
                    return result
                if isinstance(failure, TransportError) and not isinstance(
                    failure, ConnectionDropped
                ):
                    raise failure

    def path(self, kind, namespace, name=""):
        if namespace not in NAMESPACES:
            raise Failure("Namespace is outside Tier-3")
        group, resource = COLLECTIONS[kind]
        path = f"{group}/namespaces/{namespace}/{resource}"
        return path + "/" + encoded(name) if name else path

    def caller(self, method, path, body=None, **kwargs):
        return partial(self.http.json, method, self.endpoint + path, body, **kwargs)

    def request(self, method, path, body=None, **kwargs):
        """Send a proxied call once, a GET as a read, and any other repeated on 429."""
        call = self.caller(method, path, body, **kwargs)
        # A proxied Flink REST answer is the job's, not the control plane's:
        # its 503 during a recovery is an observation the caller classifies.
        if "/proxy/" in path:
            return call()
        return self.send(call, repeats_read if method == "GET" else throttled)

    def object_at(self, path):
        """The object at `path`, read on the read rule, or None when absent."""
        try:
            return self.request("GET", path)
        except ApiError as error:
            if error.status == 404:
                return None
            raise

    def created(self, path, body, failure):
        """Settle a create whose outcome is unknown, or whose repeat met 409.

        An object absent from `path` was not created, so the create is sent
        again. The object this create sent is returned as created; one it did
        not send holds the name, and that is refused.
        """
        stored = self.object_at(path)
        if stored is None:
            return RESEND
        if not created_by(stored, body):
            raise Failure(
                f"Create outcome is uncertain and {body['kind']} "
                f"{body['metadata']['name']} is not the object it sent"
            ) from failure
        return stored

    def patched(self, path, body, failure):
        """Settle a guarded patch whose outcome is unknown, or a repeat's 409 or 422.

        An unchanged resourceVersion means the patch did not land, so it is
        sent again. A changed one returns the read object when it reflects
        every change. Otherwise, or when the uid differs, this raises the 422
        the API server answers a failed `test` operation with, and for a
        target that is gone the 404 it answers a patch of one, so the
        callers' conflict handling applies unchanged.
        """
        stored = self.object_at(path)
        if stored is None:
            raise ApiError(404, "PATCH", path) from failure
        meta = stored.get("metadata", {})
        if meta.get("uid") == body[0]["value"]:
            if meta.get("resourceVersion") == body[1]["value"]:
                return RESEND
            if all(reflected(stored, operation) for operation in body[2:]):
                return stored
        raise ApiError(422, "PATCH", path) from failure

    def deleted(self, path, uid):
        """Settle a delete whose outcome is unknown, or whose repeat met 404 or 409.

        An object still holding `uid` without a deletion mark was not
        deleted, so the delete is sent again under its uid precondition. One
        that is gone, marked for deletion, or replaced under its name is what
        the delete asked for, and the earlier attempt may have done it.
        """
        meta = (self.object_at(path) or {}).get("metadata", {})
        if meta.get("uid") == uid and not meta.get("deletionTimestamp"):
            return RESEND
        return {}

    def review(self, path, body):
        """Send an access or identity review, which persists nothing, as a read."""
        return self.send(self.caller("POST", path, body), repeats_read)

    def namespace(self, name):
        if name not in NAMESPACES:
            raise Failure("Unexpected namespace")
        return self.request("GET", "/api/v1/namespaces/" + name)

    def get(self, kind, namespace, name):
        return self.object_at(self.path(kind, namespace, name))

    def node_events(self, reason):
        """The events of one reason on Nodes, which Kubernetes records in `default`.

        A Spot reclamation shows on the node, not the Pods: the kubelet records
        `Shutdown` there before it stops them, and the cluster autoscaler records
        `ScaleDown` before it removes a node. Only the one reason is read,
        through the field selector, so the read stays small however many other
        events the cluster's nodes record. The namespace is outside Tier-3's on
        purpose, and the read is all its Role allows there.
        """
        query = urllib.parse.urlencode(
            {"fieldSelector": "involvedObject.kind=Node,reason=" + reason}
        )
        page = self.request("GET", "/api/v1/namespaces/default/events?" + query)
        return [
            {**item, "kind": "Event", "apiVersion": "v1"}
            for item in page.get("items", [])
        ]

    def items(self, kind, namespace):
        result, token = [], None
        while True:
            query = {"limit": 500}
            if token:
                query["continue"] = token
            page = self.request(
                "GET", self.path(kind, namespace) + "?" + urllib.parse.urlencode(query)
            )
            api_version = (
                COLLECTIONS[kind][0].removeprefix("/apis/").removeprefix("/api/")
            )
            for item in page.get("items", []):
                # Native List encodings omit TypeMeta on their elements.
                result.append({**item, "kind": kind, "apiVersion": api_version})
            if len(result) > 1000:
                raise Failure("Kubernetes inventory exceeds its count ceiling")
            token = page.get("metadata", {}).get("continue")
            if not token:
                return result

    def inventory(self):
        return [
            item
            for namespace in NAMESPACES
            for kind in INVENTORY
            for item in self.items(kind, namespace)
        ]

    def create(self, obj, dry_run=False, before_repeat=None):
        """Create `obj`; a dry run, which persists nothing, is never read back.

        A dry run is repeated only on 429 and runs no `before_repeat`, since a
        repeat after a stop still persists nothing.
        """
        meta = obj["metadata"]
        path = self.path(obj["kind"], meta["namespace"])
        if dry_run:
            return self.request("POST", path + "?dryRun=All", obj)
        return self.send(
            self.caller("POST", path, obj),
            throttled,
            partial(
                self.created,
                self.path(obj["kind"], meta["namespace"], meta["name"]),
                obj,
            ),
            (409,),
            before_repeat,
        )

    def patch(self, obj, changes, subresource="", before_repeat=None):
        """Apply `changes` if `obj` still has its uid and resourceVersion."""
        meta = obj["metadata"]
        path = self.path(obj["kind"], meta["namespace"], meta["name"]) + subresource
        body = [
            {"op": "test", "path": "/metadata/uid", "value": meta["uid"]},
            {
                "op": "test",
                "path": "/metadata/resourceVersion",
                "value": meta["resourceVersion"],
            },
            *changes,
        ]
        return self.send(
            self.caller(
                "PATCH", path, body, content_type="application/json-patch+json"
            ),
            throttled,
            partial(self.patched, path, body),
            (409, 422),
            before_repeat,
        )

    def delete(self, obj, force=False):
        """Delete `obj` if it still has its uid; False when it was already gone."""
        meta = obj["metadata"]
        path = self.path(obj["kind"], meta["namespace"], meta["name"])
        options = {
            "apiVersion": "v1",
            "kind": "DeleteOptions",
            "preconditions": {"uid": meta["uid"]},
            "propagationPolicy": "Background",
        }
        if force:
            options["gracePeriodSeconds"] = 0
        try:
            self.send(
                self.caller("DELETE", path, options),
                throttled,
                lambda _failure: self.deleted(path, meta["uid"]),
                (404, 409),
            )
        except ApiError as error:
            if error.status != 404:
                raise
            return False
        return True

    def logs(self, pod, since=None):
        # One limit for every read: a first read can land before a container
        # has printed its startup output, which then arrives in a later one.
        limit = MIB
        query = {
            "timestamps": "true",
            "limitBytes": limit,
            "container": pod["spec"]["containers"][0]["name"],
        }
        if since:
            query["sinceTime"] = since
        path = (
            self.path("Pod", pod["metadata"]["namespace"], pod["metadata"]["name"])
            + "/log?"
            + urllib.parse.urlencode(query)
        )
        return self.send(
            partial(self.http.request, "GET", self.endpoint + path, limit=limit),
            repeats_read,
        )
