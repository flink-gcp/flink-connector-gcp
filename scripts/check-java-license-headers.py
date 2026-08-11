#!/usr/bin/env python3
# Copyright 2026 laughingman7743
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
"""Require every Java source to start with a complete approved header.

Apache RAT identifies a licence family from a distinctive substring.
That answers whether a file names an approved licence, but it cannot prove that
the surrounding notice is complete.
This check holds Java sources to either this project's canonical Apache-2.0
header or the canonical ASF header retained by copied Apache sources.
"""

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

COPYRIGHT_HEADER = """/*
 * Copyright 2026 laughingman7743
"""

APACHE_HEADER_SUFFIX = """ *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
"""

PROJECT_HEADER = COPYRIGHT_HEADER + APACHE_HEADER_SUFFIX

ASF_HEADER = """/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
"""


def java_sources(root: Path) -> list[Path]:
    return sorted(
        path
        for path in root.rglob("*.java")
        if "target" not in path.relative_to(root).parts
        and not any(part.startswith(".") for part in path.relative_to(root).parts)
    )


def invalid_headers(root: Path) -> list[Path]:
    invalid = []
    for path in java_sources(root):
        contents = path.read_text(encoding="utf-8")
        copyright_header = re.match(r"/\*\n \* Copyright [^\n]+\n", contents)
        complete_copyright_header = bool(
            copyright_header
            and contents.startswith(APACHE_HEADER_SUFFIX, copyright_header.end())
        )
        if not (complete_copyright_header or contents.startswith(ASF_HEADER)):
            invalid.append(path.relative_to(root))
    return invalid


def main() -> int:
    invalid = invalid_headers(ROOT)
    if not invalid:
        return 0
    print(
        "Java files must start with a complete copyright-bearing or ASF "
        "Apache-2.0 header:"
    )
    for path in invalid:
        print(f"  {path}")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
