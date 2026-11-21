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
"""Map a paused queue's listed tasks back to the run's input sequences.

The supervisor exports each listing page as it reads it, in the shape
``page`` returns, and decides the report from those pages; the offline
analyzer recomputes the same report from the exported pages.
"""

import base64
import hashlib
import re

TASK_ID = re.compile(r"[0-9a-f]{64}")


def _body_is_own_key(task, task_id):
    """Whether the task's body is the key its id is the SHA-256 of.

    The application's body repeats the task key ``<run-id>/<sequence>``, and
    the sink names the task with the key's digest, so a task whose body is
    another record's, or is missing or undecodable, carries a wrong value.
    """
    request = task.get("httpRequest")
    body = request.get("body") if isinstance(request, dict) else None
    if not isinstance(body, str):
        return False
    try:
        decoded = base64.b64decode(body, validate=True)
    except ValueError:
        return False
    return hashlib.sha256(decoded).hexdigest() == task_id


def page(trial, tasks):
    """The compact evidence of one listing page.

    `ids` holds the id of every task named under the run's queue, in listing
    order; `foreign` every other name whole; `dispatched` the ids of tasks the
    service reports a dispatch attempt for, which a paused queue never makes;
    `mismatched` the ids of tasks whose body is not the key the id names.
    """
    prefix = trial.queue_path + "/tasks/"
    ids, foreign, dispatched, mismatched = [], [], [], []
    for task in tasks:
        name = task.get("name") if isinstance(task, dict) else None
        task_id = (
            name.removeprefix(prefix)
            if isinstance(name, str) and name.startswith(prefix)
            else None
        )
        if task_id is None or not TASK_ID.fullmatch(task_id):
            foreign.append(name if isinstance(name, str) else repr(name))
            continue
        ids.append(task_id)
        if task.get("dispatchCount") not in (None, 0, "0"):
            dispatched.append(task_id)
        if not _body_is_own_key(task, task_id):
            mismatched.append(task_id)
    return {
        "ids": ids,
        "foreign": foreign,
        "dispatched": dispatched,
        "mismatched": mismatched,
    }


def assess(trial, pages):
    """Every input sequence exactly once, and no task outside the input.

    `pages` are ``page`` results in listing order. The verdict is `pass` only
    when every sequence's task is listed once with its own body, nothing else
    is listed, and no task was dispatched.
    """
    expected = trial.task_ids()
    listed, matched, foreign, dispatched = 0, set(), 0, 0
    duplicates, mismatched = 0, 0
    for item in pages:
        foreign += len(item["foreign"])
        dispatched += len(item["dispatched"])
        mismatched += len(item["mismatched"])
        for task_id in item["ids"]:
            listed += 1
            if task_id not in expected:
                foreign += 1
            elif task_id in matched:
                duplicates += 1
            else:
                matched.add(task_id)
    missing = trial.records - len(matched)
    report = {
        "records": trial.records,
        "listed": listed + sum(len(item["foreign"]) for item in pages),
        "matched": len(matched),
        "missing": missing,
        "foreign": foreign,
        "duplicates": duplicates,
        "dispatched": dispatched,
        "mismatched": mismatched,
    }
    report["verdict"] = (
        "pass"
        if not (missing or foreign or duplicates or dispatched or mismatched)
        else "fail"
    )
    return report
