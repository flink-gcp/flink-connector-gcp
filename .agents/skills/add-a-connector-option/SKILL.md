---
name: add-a-connector-option
description: Add a configuration option to a connector — a `ConfigOption` on a Table API factory, a public builder setter, or both. Use when adding, renaming or removing a connector option or knob, when `just check-option-docs` fails with "is a builder option but no `Option`-headed table names it", and when deciding what a rejection message may name, whether a value earns a character check, or how a `Duration` is bounded. Covers the five things an option owes, which of them a checker already holds, and the test that holds the one no checker does.
---

# Add a connector option

An option is cheap to add and has five obligations, four of which are easy to forget because
nothing fails until much later — or, for one of them, until a user reads a confusing message in
production. This is the list, in the order the work happens.

A checker for the third obligation was built and withdrawn — it could not see a newly added option
at all, which is exactly this moment. ADR-0127 records why.

`just check-option-docs` **always** fails when an option is added, so its failure is the reliable
way back here.

## 1. Declare it

In the module's `*ConnectorOptions.java`, for a Table API key:

```java
public static final ConfigOption<String> SINK_LOCATION =
        ConfigOptions.key("sink.location")
                .stringType()
                .noDefaultValue()
                .withDescription("The BigQuery location of the destination table, …");
```

- **The key is the user's vocabulary**, dotted and kebab-cased: `sink.file-loads.temp-dataset`,
  `scan.startup.mode`. Match a sibling connector's spelling for the same concept — that consistency
  is what [#782](https://github.com/flink-gcp/flink-connector-gcp/issues/782) reviews.
- **The description never restates a default** (ADR-0139) — not a declared one, not a derived one,
  not the value absence selects ("uses ADC when unset"). Those belong in the reference row; the
  module's `*ConnectorOptionsTest.noDescriptionRestatesADefault` rejects the known restatement
  phrases, and a mapped option is `noDefaultValue()` unless that test's recorded exceptions say
  otherwise.
- **Register it** in the factory's `optionalOptions()` or `requiredOptions()`. An unregistered key
  makes `FactoryUtil` reject the whole `WITH` clause as unknown.
- **A `Duration` that becomes nanoseconds is bounded** at `Duration.ofNanos(Long.MAX_VALUE)` through
  `OptionChecks.checkExpressibleInNanos` (ADR-0068). Whether the bound is re-checked where a
  deserialized options instance is used is a per-connector decision that ADR recorded — read it
  rather than copying the nearest connector.

## 2. Decide whether the value earns a character check

Most do not. ADR-0127 is the rule and it is narrow: presence and blankness are ours, and the rest of
a name's validity is the service's. A check is earned only by one of three shapes:

1. **A component of a resource path this project composes** — `ResourceNames.checkComponent`,
   because a `/` inside it silently addresses a *different* resource.
2. **A value this connector parses, splits, quotes or emits** — checked against the grammar that
   will read it, so the failure names the option rather than the grammar.
3. **A value whose service-side failure would be silent, per-record, or would not name the option.**

Anything else gets `ResourceNames.checkNotBlank` and no more. **"The service might reject it" is not
one of the three.** A proposal to add a character check names which shape it is.

## 3. Name the rejection after what the caller typed

**This is the obligation that has been missed most** — [#235], [#895], [#984]/[#920]/[#976],
[#1009]/[#1013], [#1019], [#1030] — several of them after ADR-0127 wrote the rule down. A builder
setter's message names the setter, which is the right name for a DataStream caller and appears
nowhere in a `WITH` clause.

**Apply the value through the module's `table.OptionSetters`, not through the setter directly.**
ADR-0133 made this the rule for every Table mapper: the setter's `IllegalArgumentException` is
rethrown as a `ValidationException` naming the option key first, with the builder's own sentence
kept as the detail.

```java
OptionSetters.apply(
        config,
        PubSubConnectorOptions.SINK_BATCHING_ELEMENT_COUNT_THRESHOLD,
        builder::batchElementCountThreshold);
```

A bad value then reads
`Option 'sink.buffer-flush.max-cells' is invalid: maxBatchCells must be positive` instead of
`maxBatchCells must be positive`. Two sibling forms exist and both are the seam, so reach for them
rather than calling the target directly:

- `accept(key, value, setter)` for a value that no longer sits in a `ReadableConfig` — a
  plan-to-runtime translation point holding a raw field.
- `convert(key, value, converter)` when the target is a *factory method* rather than a setter
  (`GcRule::maxAge`, `TopicDestination.of`). Calling the converter directly leaks its own parameter
  name into the message, which is the defect in miniature.

**The bound stays in the builder.** Nothing is restated, re-implemented or moved: the failure is
renamed at the seam. Do not add a second copy of a check in the table layer to get the name right.

Four cases the seam does not cover, each settled rather than open:

- **Cross-field `build()` checks are not renamed** (ADR-0133). A message naming two knobs cannot be
  attributed to one key. Restate those by hand where the vocabulary needs it — the precedent is
  `PublisherOptionsMapper.rejectBoundedRetriesWithMessageOrdering` — and leave them where it does
  not.
- **A value fed by either of two options is renamed where the supplier is resolved.** BigQuery's
  `parentProject` comes from `scan.parent-project` or `project`; the factory's
  `parentProject(ReadableConfig)` applies the builder check through `OptionSetters` under the
  supplying key before that origin is erased from plan state.
- **A value this connector *parses* is checked in the factory, not only at the setter** — ADR-0127's
  shape 2. `EmulatorEndpoint.parse(value, EMULATOR_ENDPOINT.key())` runs at
  `createDynamicTable{Source,Sink}` so a malformed endpoint fails at `CREATE TABLE` rather than on a
  TaskManager. Place it **behind the checks the factory makes before assembly begins**, never in
  front of one: a DDL being told to remove an option is not helped by an answer about that option's
  shape. Assembly itself refuses options too — `HttpTargetSpec.from`, the create-options mappers —
  and those run later, so a DDL that trips one of them *and* carries a malformed endpoint reads the
  endpoint's message first. That is an accepted limit, not an ordering to fix.

Two shapes stay outside all of this: a check reached only by a DataStream caller, and a value no
`WITH` clause can reach. Say which, in the pull request, rather than leaving it implied.

## 4. Document it

Add the row to the module's reference or connector page. `just check-option-docs` fails until you
do, and `$curate-option-docs` is the procedure for where the row goes and what its Default column
may say. If the option registers a metric, `$curate-metric-docs` is its counterpart.

## 5. Write the test that holds obligation 3

No checker holds this one — that was measured on [#1028] and recorded in ADR-0127. A unit test does,
in about ten lines, by driving the mapper or the factory and pinning both halves of the sentence:

```java
@Test
void namesTheOptionKeyWhenAValueIsRejected() {
    Map<String, String> options = new HashMap<>();
    options.put("sink.in-flight.max-messages", "0");

    assertThatThrownBy(() -> PublisherOptionsMapper.map(Configuration.fromMap(options)))
            .isInstanceOf(ValidationException.class)
            .hasMessageContaining("Option 'sink.in-flight.max-messages' is invalid")
            .hasMessageContaining("maxInFlightMessages must be positive");
}
```

Four things this shape is deliberate about:

- **Assert both halves.** The key alone passes if the rename is there but the builder's bound is
  gone; the builder's sentence alone passes if the bound is there but the rename is not.
- **Through a factory, match the stack rather than the root cause.** `FactoryUtil` wraps whatever
  the factory throws, and `OptionSetters` puts the key on an *intermediate* `ValidationException`
  whose own cause is the builder's `IllegalArgumentException` — so `.rootCause()` sees only the
  setter's sentence. `hasStackTraceContaining` for both halves is what the factory tests use;
  `hasMessageContaining` works where the mapper is called directly.
- **Reach the arm.** A check inside an options mapper runs only when that part is assembled — a
  Pub/Sub create-options test needs a create disposition that allows creation, or the mapper never
  runs at all.
- **Make it able to fail.** Break the check locally and confirm the test goes red first; the
  failure then points at the builder's setter, which is the defect.

## What goes to the user

- **Adding a character check that names none of ADR-0127's three shapes**, which is a change to the
  rule rather than an application of it.
- **A value you conclude no SQL caller can reach.** Measure it by driving the factory rather than by
  reading — [#1027] excluded two candidates that way and included five a reading pass had missed.
- **A key whose spelling differs from the sibling connectors' word for the same concept.**

[#235]: https://github.com/flink-gcp/flink-connector-gcp/issues/235
[#895]: https://github.com/flink-gcp/flink-connector-gcp/issues/895
[#920]: https://github.com/flink-gcp/flink-connector-gcp/issues/920
[#976]: https://github.com/flink-gcp/flink-connector-gcp/issues/976
[#984]: https://github.com/flink-gcp/flink-connector-gcp/issues/984
[#1009]: https://github.com/flink-gcp/flink-connector-gcp/issues/1009
[#1013]: https://github.com/flink-gcp/flink-connector-gcp/issues/1013
[#1019]: https://github.com/flink-gcp/flink-connector-gcp/issues/1019
[#1027]: https://github.com/flink-gcp/flink-connector-gcp/issues/1027
[#1030]: https://github.com/flink-gcp/flink-connector-gcp/issues/1030
[#1028]: https://github.com/flink-gcp/flink-connector-gcp/issues/1028
