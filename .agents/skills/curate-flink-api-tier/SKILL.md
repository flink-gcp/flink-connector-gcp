---
name: curate-flink-api-tier
description: Decide how to respond when `just check-flink-api-tiers` / `scripts/check-flink-api-tiers.py` fails. Use on "is Internal/Experimental/unannotated but has no entry", "entry … is stale", "owns no imported type", "resolves to no .java entry", or when moving the supported Flink range re-tiers a type. Covers the judgment ladder (stable alternative → inline → allowlist with reason → project discussion) and what must go to the user.
---

# Curate a Flink API tier allowlist decision

The tier audit is automated end to end except for the step that is judgment:
whether a dependency on an unstable Flink type is worth carrying. This skill
is the procedure for that step. Every outcome is either a reviewable allowlist
entry whose reason survives being read in a year, or a question to the user —
never a reflexive entry that makes the failure go away. The check's value is
noticing the next unstable dependency; an allowlist that grows by default
spends that value.

## Failure: "<type> is Internal/unannotated but has no […] entry"

Work down this ladder and stop at the first rung that holds.

1. **A stable alternative.** Search the same package family for a
   `@Public`/`@PublicEvolving` type that does the job. Check the javadoc of
   the type you reached for — Flink often documents the supported entry point
   on the internal type itself.
2. **Inline it.** For utility classes the cost is usually a few lines
   (`Preconditions.checkArgument` → an `if`+`throw`; `IOUtils.closeAll` → a
   small loop). Prefer this when the usage count is small: measure it
   (`grep -rn` the call sites) rather than assuming — the count decides, and
   past decisions here have hinged on a mismeasured cost.
3. **Allowlist it.** Only when the type is structurally hard to avoid (the
   `SplitsRemoval` case: naming the `@Internal` subtype is how the
   `SplitReader` SPI distinguishes removal from addition). The entry's reason
   must say *why it is unavoidable* and *what the fallback is* if Flink moves
   it — the existing entries in `scripts/flink-api-tiers.toml` are the
   template. Name call sites, not vibes.

## Failure: a new `@Experimental` type

Different from `@Internal`: an `@Experimental` dependency is usually a *design
decision* (a whole topology or feature resting on an unguaranteed surface),
not a utility choice. The two existing exposures — the FILE_LOADS pre-commit
topology (issue #14) and the buffered-stream exactly-once sink's identity
pre-commit topology used as a validation hook (issue #30) — were each decided
and documented deliberately. A new one gets the same treatment: **take it to
the user as a design discussion first**, and only then record the entries,
each reason pointing at the issue where it was decided. Do not allowlist your
way past this rung.

## Failure: "entry … is stale: the main sources no longer import it"

The import is gone (or the type changed tier — the message covers both).
Delete the entry, or re-file it under the tier it moved to. Nothing to
discuss: the list is an exact record of the present, never a superset.

## Failure: "resolves to no .java entry" / "owns no imported type"

Mechanical config maintenance, not judgment: extend or trim the `artifacts`
list in `scripts/flink-api-tiers.toml`. Find the owning artifact by searching
Maven Central for the sources jar that contains the type's path, and note in
the TOML comment when the owner is non-obvious (the existing comments about
flink-runtime owning `streaming.api.*` and flink-core-api owning
`util.function.*` are the precedent). If no artifact ships the type at the
pinned `flink.version`, the import itself is wrong for the supported range.

## When the supported range moves

Re-tiering surfaces here: a floor bump reclassifies every import at the new
version, so a type Flink promoted or demoted shows up as unlisted or stale in
the range-move PR. Handle each through the ladders above in that same PR —
the range move is not complete while this check is red, same rule as
re-running `just binary-compat` against the new ceiling.
