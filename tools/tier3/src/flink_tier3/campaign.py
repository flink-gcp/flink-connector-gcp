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
"""Choose the next attempt of a campaign that one dispatch carries.

A campaign is the kinds a dispatch still has to run after its own, and the
repeats it may still spend. After each attempt has finished and released the
environment, its receipt decides the next one: the next kind once a verdict is
`usable`, the same kind again, while repeats remain, after a stop the platform
caused, and nothing after any other result, which is the owner's to read.
"""

import re

from .common import Failure
from .exercise import FAULTS, PLATFORM_STOPS

# Each kind's part of a run ID, `<prefix>-<abbreviation>-a<attempt>`.
ABBREVIATIONS = {
    "jm-replacement": "jm",
    "tm-replacement": "tm",
    "rescale-out": "ro",
    "rescale-in": "ri",
}
RUN = re.compile(
    "(?P<prefix>.+)-(?P<abbreviation>"
    + "|".join(ABBREVIATIONS.values())
    + ")-a(?P<attempt>[1-9][0-9]*)"
)
MAX_REPEATS = 9


def requested(campaign, repeats):
    """Whether a dispatch names a campaign at all."""
    return bool(campaign) or (repeats or "0") != "0"


def parse(run_id, kind, campaign, repeats):
    """The remaining kinds, repeats and run ID parts, or Failure before anything runs."""
    kinds = [item for item in (campaign or "").split(",") if item]
    if any(item not in FAULTS for item in kinds):
        raise Failure("A campaign names only the kinds " + ", ".join(FAULTS))
    repeats = repeats or "0"
    if not repeats.isdigit() or int(repeats) > MAX_REPEATS:
        raise Failure(f"A campaign repeats 0 to {MAX_REPEATS} attempts")
    match = RUN.fullmatch(run_id or "")
    if requested(campaign, repeats) and (
        match is None or match["abbreviation"] != ABBREVIATIONS.get(kind)
    ):
        raise Failure(
            "A campaign's run ID is <prefix>-<kind>-a<attempt>, with the kind as "
            + ", ".join(f"{a} for {k}" for k, a in ABBREVIATIONS.items())
        )
    return kinds, int(repeats), match


def platform_stop(receipt):
    """Whether the run stopped for a cause that repeating it may avoid.

    A run whose application was never created ran no connector code, so
    whatever ended it, a supervisor a system Pod preempted or a service call
    that never answered, cannot hide a connector defect; a cause that recurs
    spends the repeat budget, which bounds it. After that only a Pod the
    platform preempted, or lost with its node, counts.
    """
    return (
        receipt.get("application_created") is False
        or receipt.get("reason") in PLATFORM_STOPS
    )


def next_attempt(receipt, run_id, kind, campaign, repeats):
    """The next attempt's run ID, kind, campaign and repeats, or None to stop."""
    kinds, repeats, match = parse(run_id, kind, campaign, repeats)
    if receipt.get("success") is True:
        if not kinds:
            return None
        following = kinds[0]
        return {
            "run_id": f"{match['prefix']}-{ABBREVIATIONS[following]}-a1",
            "kind": following,
            "campaign": ",".join(kinds[1:]),
            "repeats": str(repeats),
        }
    if repeats and platform_stop(receipt):
        attempt = int(match["attempt"]) + 1
        return {
            "run_id": f"{match['prefix']}-{match['abbreviation']}-a{attempt}",
            "kind": kind,
            "campaign": ",".join(kinds),
            "repeats": str(repeats - 1),
        }
    return None
