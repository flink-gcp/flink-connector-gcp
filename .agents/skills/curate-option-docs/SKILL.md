---
name: curate-option-docs
description: Decide how to respond when `just check-option-docs` / `scripts/check-option-docs.py` fails. Use on "is a builder option but no `Option`-headed table names it", "the option table names X, which no builder declares", "declares options but no [[builders]] entry maps it", "declares public builder setters but nothing maps it", "is named under X's sources but", "matches *Options.java but declares no builder setter", or when adding a connector, an options class, a public builder, or a Table API option. Covers where a row goes, what its Default column may say, the opposite directions [exempt] and [extra] point in, and when a builder is reached by `sources` rather than by a glob.
---

# Curate a configuration-reference decision

`scripts/check-option-docs.py` holds `docs/content/docs/reference/` to the options the connectors
actually take. It is mechanical in both directions, so almost every failure has one correct
response and no judgment in it — **write the row**. The two allowlists exist for the cases that
genuinely are not rows, and reaching for one when a row was called for is how a reference stops
being complete.

Run `just check-option-docs` after any change to a builder, an options class, a `ConfigOption`, or
a reference page. It is offline and takes under a second.

## The config file, in one paragraph

`scripts/config/option-docs.toml` has four tables and they do different jobs:

| Table | Direction | Answers |
|---|---|---|
| `[[builders]]` | module → page | Which reference page must document this module's builder setters |
| its `sources` | source file → that page | A public builder the globs cannot see, named one per line |
| `[[config_options]]` | source file → page | Which page must document this `ConfigOption` class's keys |
| `[exempt]` | **source side** | A setter that exists and deliberately has *no* row. Keyed `Class.setter` |
| `[extra]` | **page side** | A row that exists and has *no* setter or key behind it. Keyed by the name as the table writes it |

**`[exempt]` and `[extra]` point in opposite directions**, which is the thing to get right: one
forgives a source the docs do not mention, the other forgives a doc entry the source does not
declare. If you are unsure which you need, read the failure message — coverage failures name a
`Class.setter` and want `[exempt]`; staleness failures name a page and line and want `[extra]`, or
more likely a corrected row.

Both are dictionaries of `key = "reason"`. The reason is read by the next person deciding whether
the entry still applies, so it says *why this is not a row*, not *what the option is*.

**An entry that never fires is itself a failure**, the rule `check-flink-api-tiers.toml` applies to
its allowlist. So an entry is never a safe way to quieten something: if the check passes without
it, it has to go.

## Failure: "`Class.setter` is a builder option but no `Option`-headed table names it"

A knob was added or renamed and the reference did not follow. **Write the row** — this is the
common case and the one the check exists for.

1. **Find the page** from the `[[builders]]` entry for the module.
2. **Find the section.** One `##` per builder or options class, named for the class exactly
   (`## \`DefaultStreamOptions\``). If the class is new, add a section in the order the sink uses
   it: the builder first, then the options objects it takes.
3. **Write `| \`name\` | default | one line |`.** The Default column takes one of exactly three
   forms, and picking the wrong one is the mistake worth avoiding:

   | Form | When | Example |
   |---|---|---|
   | A value | The builder's own field initializer or `DEFAULT_*` constant supplies it | `1000`, `500 ms`, `false` |
   | **required** | `build()` rejects the object without it | `stagingPath` |
   | *unset ⇒ …* | The connector sets nothing and the client library or service decides | *unset ⇒ SDK default (100)* |

   **Read the default off the source, never off the prose.** `DEFAULT_*` constants and field
   initializers are the truth; a javadoc "Defaults to …" sentence can lag. For the third form, the
   concrete value is documentation of someone else's default — say where it comes from, and expect
   it to move under a dependency bump.
4. **Keep the *why* on the connector page.** The reference says what the option is and what it
   defaults to; the reasoning stays under the connector's own section and the row links to it. A
   row that starts explaining itself belongs there instead.

Grouping is allowed: one row may name several options (`` `subscription` / `subscriptions` ``), and
the check only asks that the name appears in a first cell. Group when the knobs are one decision;
do not group to save typing.

### When `[exempt]` is right instead

Only when a reader looking up the setter would find nothing new — and check first that a row is not
simply the better answer. **`[exempt]` is empty today**, and the way it got that way is the lesson:
the four bulk `Collection<String>` overloads listed there at first were all documentable in the same
row as their singular (`` `jsonFieldPath` / `jsonFieldPaths` ``), so the exemptions forgave nothing.
An entry that never fires now fails the check, so a dead one cannot accumulate — but the cheaper
habit is to try the row first.

## Failure: "the option table names `x`, which no builder in `<module>` declares"

The docs are ahead of, or behind, the source. Decide which:

- **The setter was renamed** → correct the row. The same run also reports the new name as
  undocumented, so both halves of a rename appear together; fix them together.
- **The setter was removed** → remove the row, and check the connector page's prose for sentences
  that referred to it.
- **It is real but declared elsewhere** → `[extra]`, with a reason naming *who* declares it. The
  three entries today are Flink's own `FactoryUtil` keys (`format`, `scan.parallelism`,
  `sink.parallelism`), which the SQL page carries because a reader writing DDL needs every key the
  connector accepts, not only the ones this project defines.

`[extra]` is the last resort of the three. A row nothing declares is usually a typo or a leftover.

## Failure: "`<module>` declares options but no `[[builders]]` entry maps it"

A connector module grew options and nothing is checking them — the case Bigtable (issue #33) and
Spanner (issue #36) will hit. This is not an allowlist decision:

1. Create `docs/content/docs/reference/<connector>.md`, front matter `title` / `type: docs` /
   `weight` matching the connector's weight under `connectors/datastream/`, plus the plain
   Apache-2.0 header every page carries.
2. Add the `[[builders]]` entry.
3. Add the page to the table in `docs/content/docs/reference/_index.md`, and the reference link to
   the module README.
4. Run the check and write rows until it passes.

## Failure: "declares public builder setters (…) but nothing maps it"

A public builder the globs cannot see — the gap that left `PubSubDeadLetterQueue.Builder`'s five
knobs unchecked in both directions from the day the class landed (issue #328). Neither direction says anything
about a class it never reads, so this guard reports one before the silence starts. It is not an
allowlist decision, and there are exactly two answers:

- **It is a user-facing option surface** → add its path to that module's `sources` list in
  `[[builders]]`, then write rows until the check passes. The class is then parsed, keyed and
  checked exactly as a glob-matched one, so `[exempt]` keys read `Class.setter` as usual. If the
  module has no `[[builders]]` entry at all, it needs one first — that is the section above.
- **It is not** → annotate the class `@Internal`, which is the honest fix when the builder is
  assembled by a factory rather than by a user (`BigQueryDynamicSink`, `SubscriptionInfo`). Only
  the file's **first top-level type**'s annotation is read, so a nested `@Internal` will not do it.

An unannotated class is reported on purpose: `check-flink-api-tiers.py` treats an unannotated type
as needing a decision rather than as absent, and so does this. Do not answer the guard by widening
`SOURCE_GLOBS` — see the last section.

Its exit-2 siblings, all config-authoring errors in a `sources` entry: the file does not exist, it
lives outside the module's main source roots, it already matches `SOURCE_GLOBS`, or it declares no
setter this script recognises. Each message names its own remedy and two of them are *delete the
entry* — a mapping onto a class the globs already scan, and one onto a class with nothing to check,
are both the dead claim a never-firing allowlist entry is.

## Failure: "matches `*Options.java` but declares no builder setter this script recognises"

Exit code 2 — infrastructure, not policy. Two causes, and they need opposite fixes:

- **The file is not an options class** (a constant holder that happens to end in `Options`).
  Narrow `SOURCE_GLOBS`, or rename the class.
- **The builder shape changed** — a setter returning something other than `Builder` or
  `<Name>Builder<T>`. Then `SETTER` is under-matching *everywhere*, and every other class's result
  is untrustworthy too. Fix the pattern, do not exclude the file.

Never make this one go away by deleting the file from the scan without establishing which it is.

## What goes to the user

- **Removing a `[[builders]]` or `[[config_options]]` mapping**, which silently retires the check
  for a whole surface.
- **A default that the source and the documentation disagree about** — that is either a docs bug or
  a behaviour change, and which one it is decides whether it is a docs commit.
- **Widening `SOURCE_GLOBS`.** The boundary is deliberate: serialization schemas' `with*` methods
  configure a record rather than the sink, and the script has no way to judge which `with*` on
  which fluent type is an option. Widening it to reach a class the guard reported was measured on
  #328 and declined — `*Queue.java` alone breaks the check two different ways, and a wider pattern
  pulls in 19 `@Internal` setters that would each demand a row. The reasoning is in the comment
  above `SOURCE_GLOBS`; changing it is a design decision, not maintenance.

## What this check does not do

It compares the *set* of options, not their values. A default changed in the source with the table
left saying the old number passes — so when you change a default, change the row in the same
commit; nothing will catch it for you. Checking values would mean running the JVM to read them,
which was weighed and left out.
