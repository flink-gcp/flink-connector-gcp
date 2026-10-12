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
"""Synthetic Cloud Tasks REST contracts for the run's queue."""

import pytest
import requests
from flink_tier3.cloudtasks.queue import (
    BASE,
    PAGE_SIZE,
    QUEUE_CONFIGURATION,
    Queue,
    configured,
)
from flink_tier3.cloudtasks.trial import Trial
from flink_tier3.common import ApiError, Failure, TransportError

from ..bigquery.test_resources import NOW, Http, Response

TRIAL = Trial("ct-queue", 100, 10)
NAME = TRIAL.queue_path


def queue(*responses, deadline=NOW + 120, clock=lambda: NOW):
    http = Http(*responses)
    return Queue(http, TRIAL, deadline, clock), http


def paused(**changes):
    return {
        "name": NAME,
        "state": "PAUSED",
        **QUEUE_CONFIGURATION,
        "rateLimits": {**QUEUE_CONFIGURATION["rateLimits"], "maxBurstSize": 1},
        **changes,
    }


def test_create_posts_the_approved_configuration_under_the_run_name():
    api, http = queue({"name": NAME, "state": "RUNNING"})
    api.create()
    [(method, url, kwargs)] = http.calls
    assert (method, url) == (
        "POST",
        BASE + "projects/flink-gcp/locations/us-central1/queues",
    )
    assert kwargs["json"] == {"name": NAME, **QUEUE_CONFIGURATION}
    assert kwargs["allow_redirects"] is False


def test_a_created_queue_under_another_name_is_refused():
    api, _ = queue({"name": NAME + "-other"})
    with pytest.raises(Failure, match="name differs"):
        api.create()


def test_get_and_delete_treat_absence_as_an_answer():
    api, http = queue(Response({}, 404), Response({}, 404))
    assert api.get() is None
    assert api.delete() is None
    assert [call[:2] for call in http.calls] == [
        ("GET", BASE + NAME),
        ("DELETE", BASE + NAME),
    ]


def test_pause_posts_to_the_queue_action():
    api, http = queue({"name": NAME, "state": "PAUSED"})
    api.pause()
    assert http.calls[0][:2] == ("POST", BASE + NAME + ":pause")


def test_tasks_reads_basic_pages_and_their_token():
    api, http = queue(
        {"tasks": [{"name": NAME + "/tasks/a"}], "nextPageToken": "next"},
        {"tasks": [{"name": NAME + "/tasks/b"}]},
        {},
    )
    assert api.tasks() == ([{"name": NAME + "/tasks/a"}], "next")
    assert api.tasks("next") == ([{"name": NAME + "/tasks/b"}], None)
    assert api.tasks() == ([], None)
    assert http.calls[0][2]["params"] == {
        "responseView": "BASIC",
        "pageSize": PAGE_SIZE,
    }
    assert http.calls[1][2]["params"]["pageToken"] == "next"


@pytest.mark.parametrize(
    "page",
    [{"tasks": {"name": "x"}}, {"tasks": [], "nextPageToken": 3}],
)
def test_a_malformed_listing_page_is_refused(page):
    api, _ = queue(page)
    with pytest.raises(Failure, match="malformed"):
        api.tasks()


def test_errors_keep_their_status_and_transport_failures_their_kind():
    api, _ = queue(
        Response({}, 409),
        requests.exceptions.ConnectionError("reset"),
        Response(b"[1]"),
    )
    with pytest.raises(ApiError) as error:
        api.create()
    assert error.value.status == 409
    with pytest.raises(TransportError):
        api.get()
    with pytest.raises(Failure, match="object"):
        api.get()


def test_the_deadline_refuses_a_call_and_narrows_but_never_extends():
    times = iter([NOW + 121])
    api, http = queue({}, clock=lambda: next(times))
    with pytest.raises(Failure, match="deadline"):
        api.get()
    assert http.calls == []
    base, _ = queue()
    assert base.with_deadline(NOW + 60).deadline == NOW + 60
    assert base.with_deadline(NOW + 600).deadline == NOW + 120


def test_configured_requires_every_approved_setting():
    assert configured(paused())
    assert not configured(paused(tombstoneTtl="600s"))
    assert not configured(paused(retryConfig={"maxAttempts": 5}))
    assert not configured(None)
