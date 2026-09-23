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
import urllib.parse

import urllib3

from kubernetes import client as kubernetes_client

from .common import ApiError, Failure, TransportError, encoded
from .policy import HTTP_TIMEOUT, MIB, NAMESPACES, SMOKE, inventory_namespaces

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


INVENTORY = tuple(k for k in COLLECTIONS if k not in ("ResourceQuota", "Event"))


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
                    raise Failure("Response exceeds its read ceiling")
                return data
            finally:
                response.close()
                response.release_conn()
        except kubernetes_client.exceptions.ApiException as error:
            raise ApiError(error.status, method, parts.path) from error
        except urllib3.exceptions.HTTPError as error:
            raise TransportError(
                f"{method} Kubernetes request failed: {type(error).__name__}"
            ) from error

    def json(self, method, url, body=None, **kwargs):
        data = self.request(method, url, body, **kwargs)
        return json.loads(data) if data else {}


class Kubernetes:
    def __init__(self, endpoint, http):
        self.endpoint, self.http = endpoint.rstrip("/"), http

    def path(self, kind, namespace, name=""):
        if namespace not in NAMESPACES:
            raise Failure("Namespace is outside Tier-3")
        group, resource = COLLECTIONS[kind]
        path = f"{group}/namespaces/{namespace}/{resource}"
        return path + "/" + encoded(name) if name else path

    def request(self, method, path, body=None, **kwargs):
        return self.http.json(method, self.endpoint + path, body, **kwargs)

    def nodes(self):
        """Every node, which the cluster-scoped read alone may list."""
        return self.request("GET", "/api/v1/nodes").get("items", [])

    def namespace(self, name):
        if name not in NAMESPACES:
            raise Failure("Unexpected namespace")
        return self.request("GET", "/api/v1/namespaces/" + name)

    def get(self, kind, namespace, name):
        try:
            return self.request("GET", self.path(kind, namespace, name))
        except ApiError as error:
            if error.status == 404:
                return None
            raise

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

    def inventory(self, application=SMOKE):
        return [
            item
            for namespace in inventory_namespaces(application)
            for kind in INVENTORY
            for item in self.items(kind, namespace)
        ]

    def create(self, obj, dry_run=False):
        meta = obj["metadata"]
        path = self.path(obj["kind"], meta["namespace"])
        return self.request("POST", path + ("?dryRun=All" if dry_run else ""), obj)

    def patch(self, obj, changes, subresource=""):
        meta = obj["metadata"]
        path = self.path(obj["kind"], meta["namespace"], meta["name"]) + subresource
        checks = [
            {"op": "test", "path": "/metadata/uid", "value": meta["uid"]},
            {
                "op": "test",
                "path": "/metadata/resourceVersion",
                "value": meta["resourceVersion"],
            },
        ]
        return self.request(
            "PATCH", path, checks + changes, content_type="application/json-patch+json"
        )

    def delete(self, obj, force=False):
        meta = obj["metadata"]
        options = {
            "apiVersion": "v1",
            "kind": "DeleteOptions",
            "preconditions": {"uid": meta["uid"]},
            "propagationPolicy": "Background",
        }
        if force:
            options["gracePeriodSeconds"] = 0
        try:
            self.request(
                "DELETE",
                self.path(obj["kind"], meta["namespace"], meta["name"]),
                options,
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
        return self.http.request("GET", self.endpoint + path, limit=limit)
