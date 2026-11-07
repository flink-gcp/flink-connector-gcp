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

"""Download the checksum-pinned Operator Helm chart from the pin's mirrors.

Each pin lists its download URLs in the order to try them. The pinned SHA-512
alone decides which response is accepted, so the host that answers cannot
change the bytes that are used, and adding a mirror cannot weaken the pin.
"""

import hashlib
import http.client
import urllib.request

MAX_CHART_BYTES = 2 * 1024 * 1024


def pinned_urls(pin, pin_name):
    """Return the pin's download URLs in the order they are tried."""
    urls = pin.get("urls")
    if (
        not isinstance(urls, list)
        or not urls
        or not all(isinstance(url, str) for url in urls)
    ):
        raise ValueError(
            f"{pin_name} must list the chart's download URLs "
            "as a non-empty `urls` array of strings"
        )
    return urls


def check_bytes(data, sha512, pin_name):
    """Reject an oversized or altered chart before anything parses it."""
    if len(data) > MAX_CHART_BYTES:
        raise ValueError(f"Operator chart exceeds the {MAX_CHART_BYTES}-byte limit")
    if hashlib.sha512(data).hexdigest() != sha512:
        raise ValueError(f"Operator chart SHA-512 does not match {pin_name}")


def download(urls, sha512, pin_name):
    """Return the first URL's bytes that match the pin.

    A host that is unreachable, answers with an error, or serves other bytes
    leaves the next URL to try. Only when every URL fails is the download an
    error, and the error lists each URL's failure.
    """
    failures = []
    for candidate in urls:
        try:
            with urllib.request.urlopen(candidate, timeout=60) as response:
                data = response.read(MAX_CHART_BYTES + 1)
            check_bytes(data, sha512, pin_name)
        except (OSError, http.client.HTTPException, ValueError) as error:
            failures.append(f"{candidate}: {error}")
            continue
        return data
    raise ValueError(
        "No source served the pinned Operator chart:\n" + "\n".join(failures)
    )
