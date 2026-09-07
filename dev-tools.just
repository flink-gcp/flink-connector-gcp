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

# Copy this recipe into the consumer's justfile, or import this file from it.
# Keep the selected full commit SHA in the consumer's tracked configuration.

[positional-arguments]
install-skills revision:
    #!/usr/bin/env bash
    set -euo pipefail
    revision="$1"
    [[ "$revision" =~ ^[0-9a-f]{40}$ ]] || { echo 'Use a full commit SHA.' >&2; exit 1; }
    prefix="$(git rev-parse --show-prefix)"
    [[ -z "$prefix" ]] || { echo 'Run from the repository root.' >&2; exit 1; }
    [[ ! -L .agents && ! -L .agents/skills ]] || { echo 'The project skill directory must not be a symlink.' >&2; exit 1; }
    skills=(push-pr-branch self-review self-review-round-two independent-review)
    staging="$(mktemp -d)"
    trap 'rm -rf "$staging"' EXIT
    gh api "repos/flink-gcp/flink-gcp-dev-tools/tarball/$revision" > "$staging/source.tar.gz"
    mkdir "$staging/source"
    tar -xzf "$staging/source.tar.gz" --strip-components=1 -C "$staging/source"
    changed=()
    for skill in "${skills[@]}"; do
      source="$staging/source/skills/$skill"
      target=".agents/skills/$skill"
      [[ -f "$source/SKILL.md" && ! -L "$source" && ! -L "$target" ]] || { echo "Invalid skill directory: $skill" >&2; exit 1; }
      if [[ -d "$target" ]]; then
        if git diff --no-index --no-ext-diff --no-textconv --quiet -- "$source" "$target"; then
          continue
        else
          comparison_status=$?
          [[ "$comparison_status" -eq 1 ]] || exit "$comparison_status"
        fi
      fi
      status="$(git status --porcelain --untracked-files=all --ignored -- "$target")"
      [[ -z "$status" ]] || { echo "Commit or preserve local changes in $target before installing." >&2; exit 1; }
      changed+=("$skill")
    done
    mkdir -p .agents/skills
    if ((${#changed[@]})); then
      for skill in "${changed[@]}"; do
        target=".agents/skills/$skill"
        if [[ -e "$target" ]]; then mv "$target" "$staging/previous-$skill"; fi
        if ! mv "$staging/source/skills/$skill" "$target"; then
          if [[ -e "$staging/previous-$skill" ]]; then mv "$staging/previous-$skill" "$target"; fi
          exit 1
        fi
      done
    fi
    printf 'Installed workflow skills from %s. Review and commit the skill files and revision pin.\n' "$revision"
