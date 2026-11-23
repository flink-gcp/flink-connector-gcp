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
"""Why a Pub/Sub run's output withholds `usable` (ADR-0204), and its Pod budget."""

from ..exercise import SEQUENCE_KIND, SEQUENCE_PARALLELISM


def application_pods(kind):
    """The Pods admission lets the application of a trial of `kind` hold at once.

    The fault sequence is the only kind a trial runs (ADR-0204). Its
    application holds the JobManager, the most TaskManagers it runs, and one
    replacement: a terminating Pod holds its quota until it is gone, so a
    replaced Pod and its replacement overlap. Both manifests run at two, and
    the exercise patches its rescales to one and back from the upgrade.
    """
    if kind != SEQUENCE_KIND:
        raise ValueError("Unknown Pub/Sub trial kind")
    return 2 + max(SEQUENCE_PARALLELISM.values())


def oracle_reasons(oracle):
    """Why the oracle's account does not carry the claim, if it does not."""
    if not isinstance(oracle, dict):
        return ["oracle-missing"]
    # An absent refusal is not an acceptance, and `False == 0` in Python, so
    # the count is compared with its type.
    if "rejected" not in oracle or oracle["rejected"] is not None:
        return ["oracle-rejected"]
    missing = oracle.get("missing_inputs")
    if type(missing) is not int or missing != 0:
        return ["oracle-incomplete"]
    return []


def input_reasons(inputs):
    """Why the outputs' input message IDs do not carry the claim, if they do not.

    The relay copies each input's message ID into its output, and the
    runner's publication receipts bind every ID the service returned to its
    input, so an output that names its input under an ID no receipt of that
    input holds carries a wrong value, and one whose input has no receipt
    cannot be checked.
    """
    if not isinstance(inputs, dict):
        return ["inputs-missing"]
    reasons = []
    for key, reason in (
        ("unpublished", "input-ids-unpublished"),
        ("unreceipted", "input-receipts-missing"),
    ):
        count = inputs.get(key)
        if type(count) is not int or count != 0:
            reasons.append(reason)
    return reasons


def data_reasons(record):
    """Why the run's output does not carry the claim, if it does not.

    It carries it when the oracle accepted every collected line and found
    every logical input of both subscriptions, and every output named its
    input under an ID the input was published with. Duplicates are reported
    beside it.
    """
    return [*oracle_reasons(record.get("oracle")), *input_reasons(record.get("inputs"))]
