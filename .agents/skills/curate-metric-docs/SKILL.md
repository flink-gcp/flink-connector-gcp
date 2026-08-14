---
name: curate-metric-docs
description: Decide how to respond when `just check-metric-docs` / `scripts/check-metric-docs.py` fails. Use on "but no `Metric`-headed table names it", "names X, which MODULE does not register", "documented as a counter/gauge but ... registers it as a", "marked (Flink standard) but ... registers it", "registers a counter/gauge by a name outside the module's *MetricNames.java inventory", "takes Flink's `num` prefix", "uses CLASS but no `Metric`-headed table documents", "registers metrics but no [[connectors]] entry maps it", or when adding a connector, a metric, or a metrics table. Covers where a row goes, what its Type and Meaning columns may say, and the opposite directions [exempt] and [extra] point in.
---

# Curate a metrics-table decision

`scripts/check-metric-docs.py` holds the metrics tables on the DataStream pages to what the
connectors actually register. It is mechanical in both directions, so almost every failure has one
correct response and no judgment in it — **write the row**. The allowlists exist for the cases that
genuinely are not rows, and reaching for one when a row was called for is how the tables stop being
complete.

Run `just check-metric-docs` after any change to a `*MetricNames` inventory, a `*Metrics` class, a
registration site, or a DataStream page's metrics section. It is offline and takes under a second.

## The config file, in one paragraph

`scripts/config/metric-docs.toml` has four tables and they do different jobs:

| Table | Direction | Answers |
|---|---|---|
| `[[connectors]]` | module → page | Which DataStream page must document this module's metrics |
| `[[subgroups]]` | source file | Which `base.metrics` sources register templated subgroup leaves (`errorClass.CODE.errors`) on a connector's behalf |
| `[exempt]` | **source side** | A registered name that deliberately has *no* row. Keyed `Class.name` (`BigQueryMetricNames.inFlightAppends`, `ErrorClassCounters.errors`) |
| `[extra]` | **page side** | A row that exists and has *no* registration behind it. Keyed by the name as the table writes it |

**`[exempt]` and `[extra]` point in opposite directions**: one forgives a source the docs do not
mention, the other a doc entry the source does not back. Read the failure message — coverage
failures name a `Class.name` and want `[exempt]`; staleness failures name a page and line and want
`[extra]`, or more likely a corrected row. Both are `key = "reason"`, the reason saying *why this
is not a row*. **An entry that never fires is itself a failure**, so an entry is never a safe way
to quieten something.

The third escape, which the option-docs checker does not have: a name Flink itself provides
(`numRecordsSend`, `numRecordsInErrors`) is not registered in this tree at all, and its row says so
by carrying **`(Flink standard)` in the Type cell**. (Flink's committer metrics stay in prose on
the BigQuery page, which the check does not read — they need no marker because they have no row.) That marker is
load-bearing — it exempts the row from the registration requirement — and it is guarded: a marked
row whose name the module *does* register fails, so it cannot hide a stale row.

## Failure: "registers `x` (counter) but no `Metric`-headed table names it"

A metric was added or renamed and the page did not follow. **Write the row** — the common case and
the one the check exists for.

1. **Find the page** from the `[[connectors]]` entry; the metrics tables live on the DataStream
   design-record page, under its `## Metrics` section.
2. **Find the table.** BigQuery has one per write method plus the committer's; a metric shared by
   several write methods gets a row in each table that reports it (coverage asks for at least one,
   but a table describing a write method should be complete for it). Pub/Sub splits sink and
   source.
3. **Write `| \`name\` | kind | one line |`.** The Type cell leads with `counter` or `gauge`,
   lowercase, exactly as the source registers it — the check compares them. A parenthetical after
   the kind is free text (`gauge (enumerator)`), except `(Flink standard)`, which has the meaning
   above and may only mark names Flink provides.
4. **The Meaning cell is one line of *what*, not *why*.** The reasoning — counting rules, what a
   number does and does not include — goes in the prose paragraphs under the table, where the
   existing pages put it (`numRecordsSend` counts records, not attempts…).

One cell may name a pair that shares a meaning (`` `destination.TABLE.recordsSend` ``,
`` `destination.TABLE.sendErrors` ``); group when the names are one decision.

### When `[exempt]` is right instead

Only when a reader looking the name up would find nothing new. It is empty today, and the bar is
high on purpose: a registered metric a user cannot look up is the drift this check was filed
against (issue #296 — three renames swept across five pages by hand, verified only by grep).

## Failure: "names `x`, which `<module>` does not register"

The page is ahead of, or behind, the source. Decide which:

- **The metric was renamed** → correct the row. The same run reports the new name as undocumented,
  so both halves appear together; fix them together, and sweep the page's prose for the old name —
  the check reads only the tables, so prose mentions rot silently.
- **The metric was removed** → remove the row, and the prose that explained it.
- **Flink provides it** → mark the Type cell `(Flink standard)` instead of listing it in
  `[extra]`. The marker keeps the fact on the page, where a reader sees it.
- **Something else genuinely registers it** → `[extra]`, with a reason naming who. Last resort;
  it is empty today.

## Failure: "documented as a `gauge` but … registers it as a `counter`" (or vice versa)

The source is the truth — the registration call is right there. Fix the Type cell, unless the
registration itself is the bug, in which case fixing it is a behavior change that belongs to its
own commit, not to a docs sweep.

## Failure: "registers a counter by a name outside the module's `*MetricNames.java` inventory"

The #280 rule: the inventory class *is* the connector's inventory, so every
`metricGroup.counter(...)` / `.gauge(...)` names one of its constants. Declare the constant (with
the comment saying which class registers it, as the existing inventories do) and register through
it. Never inline the string to make the failure go away — that is precisely the bypass the check
exists to catch.

## Failure: "`x` takes Flink's `num` prefix"

Rename the metric. The convention (`flink-connector-gcp-base/CLAUDE.md`, #280): a counter names
the event (`tablesCreated`), a gauge names the state (`openDestinations`), and the `num` prefix
belongs to Flink's standard names alone. Only the prefix is checked mechanically — whether the
name is an event or a state needs English morphology and stays with review.

## Failure: "uses `ErrorClassCounters` but no `Metric`-headed table documents `errorClass.….errors`"

The module wires a `base.metrics` subgroup registrar, so its page owes the templated row: the
group segment, an all-caps placeholder for the runtime value, and the leaf —
`` `errorClass.CODE.errors` ``, `` `destination.TABLE.recordsSend` ``. Pick the placeholder for
what the value is (`CODE`, `TABLE`, `TOPIC`, `QUEUE`) and say in the Meaning cell what it stands
for, as the existing rows do. The reverse failure ("does not use `<Class>`") means the row
documents a metric that never appears — remove it, or the module stopped using the class and the
row outlived it.

## Failure: "registers metrics but no `[[connectors]]` entry maps it"

A module grew metrics and nothing checks them — the case the next connector (Spanner, issue #36)
walks into. Not an allowlist decision:

1. Give the connector's DataStream page a `## Metrics` section with a `| Metric | Type | Meaning |`
   table, in the register the existing pages set.
2. Add the `[[connectors]]` entry.
3. Run the check and write rows until it passes.

## Exit code 2 — infrastructure, not policy

"declares no `static final String`", "found no registration through it at all", "could not read
one addGroup segment": the shape the patterns match changed, and every other module's result is
untrustworthy until the pattern is fixed. Fix the script's patterns or the source's shape; never
exclude the file to make it pass.

## What goes to the user

- **Removing a `[[connectors]]` or `[[subgroups]]` mapping**, which silently retires the check for
  a whole surface.
- **A metric whose registration kind looks wrong** — changing it is a behavior change, not
  documentation.
- **Renaming a metric that has shipped in a tagged release** — the name is the user interface of a
  dashboard; a rename breaks queries and belongs to a discussed change, not a checker appeasement.

## What this check does not do

It compares names and kinds, not meanings: a Meaning cell describing the wrong thing passes, and
prose outside the tables is invisible to it — sweep both by hand when renaming. It also does not
judge event-versus-state morphology (#280's other half); review does.
