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

import re

TASK_ID = re.compile(r"[0-9a-f]{64}")


def page(trial, tasks):
    """The compact evidence of one listing page.

    `ids` holds the id of every task named under the run's queue, in listing
    order; `foreign` every other name whole; `dispatched` the ids of tasks the
    service reports a dispatch attempt for, which a paused queue never makes.
    """
    prefix = trial.queue_path + "/tasks/"
    ids, foreign, dispatched = [], [], []
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
    return {"ids": ids, "foreign": foreign, "dispatched": dispatched}


def assess(trial, pages):
    """Every input sequence exactly once, and no task outside the input.

    `pages` are ``page`` results in listing order. The verdict is `pass` only
    when every sequence's task is listed once, nothing else is listed, and no
    task was dispatched.
    """
    expected = trial.task_ids()
    listed, matched, foreign, dispatched = 0, set(), 0, 0
    duplicates = 0
    for item in pages:
        foreign += len(item["foreign"])
        dispatched += len(item["dispatched"])
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
    }
    report["verdict"] = (
        "pass" if not (missing or foreign or duplicates or dispatched) else "fail"
    )
    return report
