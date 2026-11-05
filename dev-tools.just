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

# The archive is downloaded once, and every item is checked before any is replaced.
# If a replacement fails or is interrupted by a signal the shell can trap, every path already replaced is restored.
[doc("Install the workflow skills and the shared tools from one revision.")]
[positional-arguments]
install-dev-tools revision:
    #!/usr/bin/env bash
    set -euo pipefail
    revision="$1"
    skills=(push-pr-branch self-review self-review-round-two independent-review)
    scripts=(central-portal.py)
    [[ "$revision" =~ ^[0-9a-f]{40}$ ]] || { echo 'Use a full commit SHA.' >&2; exit 1; }
    prefix="$(git rev-parse --show-prefix)"
    [[ -z "$prefix" ]] || { echo 'Run from the repository root.' >&2; exit 1; }
    [[ ! -L .agents && ! -L .agents/skills ]] || { echo 'The project skill directory must not be a symlink.' >&2; exit 1; }
    [[ ! -L scripts ]] || { echo 'The project scripts directory must not be a symlink.' >&2; exit 1; }
    # An explicit template makes BSD mktemp honor TMPDIR as GNU mktemp does.
    staging="$(mktemp -d "${TMPDIR:-/tmp}/dev-tools.XXXXXX")"
    sources=()
    targets=()
    # The replacement takes two steps per target: progress is 2i+1 once target i starts moving aside, and 2i+2 once
    # its new copy starts moving in. One assignment per step keeps the state consistent whenever a signal arrives.
    # It returns to zero once every target is in place, so the exit trap restores only an unfinished replacement;
    # the trap cannot use the exit status instead, because that reads zero there after a signal.
    progress=0
    finish() {
      local index failed=0 started=$(((progress + 1) / 2)) moving_in=$((progress > 0 && progress % 2 == 0))
      for ((index = started - 1; index >= 0; index--)); do
        if ((index < started - 1 || moving_in)) && ! rm -rf "${targets[index]}"; then
          echo "Could not restore ${targets[index]}: its partial or new copy could not be removed." >&2
          failed=1
        elif [[ -e "$staging/previous/$index" ]]; then
          # A target that still exists beside its previous copy was interrupted while being moved aside.
          if [[ -e "${targets[index]}" ]]; then
            echo "Could not restore ${targets[index]}: it was interrupted while being moved aside." >&2
            failed=1
          elif ! mv "$staging/previous/$index" "${targets[index]}"; then
            echo "Could not restore ${targets[index]}: its previous copy could not be moved back." >&2
            failed=1
          fi
        fi
        if [[ -e "$staging/previous/$index" ]]; then
          echo "The previous copy of ${targets[index]} remains at $staging/previous/$index." >&2
        fi
      done
      if ((failed)); then
        echo "Some replaced paths could not be restored; compare them with Git and the previous copies above." >&2
      else
        rm -rf "$staging" || echo "Could not remove the temporary directory $staging." >&2
      fi
    }
    trap finish EXIT
    # Queues an item unless its installed copy is identical, and refuses local changes at its path.
    # The third argument is the test that finds an installed copy: -d for a skill directory, -f for a tool file.
    queue() {
      local source="$1" target="$2" installed_test="$3" comparison_status status
      if test "$installed_test" "$target"; then
        if git diff --no-index --no-ext-diff --no-textconv --quiet -- "$source" "$target"; then
          return 0
        else
          comparison_status=$?
          [[ "$comparison_status" -eq 1 ]] || exit "$comparison_status"
        fi
      fi
      status="$(git status --porcelain --untracked-files=all --ignored -- "$target")"
      [[ -z "$status" ]] || { echo "Commit or preserve local changes in $target before installing." >&2; exit 1; }
      sources+=("$source")
      targets+=("$target")
    }
    gh api "repos/flink-gcp/flink-gcp-dev-tools/tarball/$revision" > "$staging/source.tar.gz"
    mkdir "$staging/source" "$staging/previous"
    tar -xzf "$staging/source.tar.gz" --strip-components=1 -C "$staging/source"
    for skill in "${skills[@]}"; do
      source="$staging/source/skills/$skill"
      target=".agents/skills/$skill"
      [[ -f "$source/SKILL.md" && ! -L "$source" && ! -L "$target" ]] || { echo "Invalid skill directory: $skill" >&2; exit 1; }
      queue "$source" "$target" -d
    done
    for script in "${scripts[@]}"; do
      source="$staging/source/scripts/$script"
      target="scripts/$script"
      [[ -f "$source" && ! -L "$source" && ! -L "$target" && ! -d "$target" ]] || { echo "Invalid script: $script" >&2; exit 1; }
      queue "$source" "$target" -f
    done
    mkdir -p .agents/skills scripts
    for ((index = 0; index < ${#targets[@]}; index++)); do
      progress=$((2 * index + 1))
      if [[ -e "${targets[index]}" ]]; then mv "${targets[index]}" "$staging/previous/$index"; fi
      progress=$((2 * index + 2))
      mv "${sources[index]}" "${targets[index]}"
    done
    progress=0
    printf 'Installed workflow skills and shared tools from %s. Review and commit them with the revision pin.\n' "$revision"
