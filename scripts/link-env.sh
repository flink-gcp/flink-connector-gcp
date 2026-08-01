#!/usr/bin/env bash
#
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
#
# Makes the main checkout's uncommitted .env reachable from a git worktree by
# symlinking it into the current directory (issue #156). mise resolves
# `env._.file = ".env"` against config_root — the worktree root — and .env is
# gitignored, so a fresh worktree silently loads no variables and every gated
# real-GCP ITCase skips (or, since `just e2e`, fails its pre-flight). The
# symlink is covered by the same .gitignore pattern as the file it points to.
#
# The main-checkout root is derived from git itself rather than configured:
# a worktree's --git-common-dir is the main checkout's .git directory, so its
# parent is where the real .env lives. No personal path enters the repository.

set -euo pipefail

main_root=$(dirname "$(git rev-parse --path-format=absolute --git-common-dir)")

# Never clobber a real file: in the main checkout this is the .env itself
# (and linking there would replace it with a self-referential symlink), and in
# a worktree it is a hand-made copy someone chose to have.
if [ -f .env ] && [ ! -L .env ]; then
    echo ".env is already a regular file here; nothing to link."
    exit 0
fi

if [ ! -f "$main_root/.env" ]; then
    echo "::error::$main_root/.env does not exist. Create it in the main checkout first" \
        '(see "Local use" in opentofu/README.md for the variables it needs).' >&2
    exit 1
fi

# -sfn refreshes a stale or dangling symlink from an earlier link.
ln -sfn "$main_root/.env" .env
echo "Linked .env -> $main_root/.env"
