---
name: curate-option-message-names
description: Decide how to respond when `just check-option-message-names` / `scripts/check-option-message-names.py` fails. Use on "a check that names a setting, with no verdict", "classified `same`, but X is not a key", "classified `restated` under X, but nothing in Y's table package checks", "classified `restated` under X, which Y's *ConnectorOptions.java does not declare", "never fires", "names its setting with X, which is neither a string literal nor a `.key()` expression", "sits in no method body", or when adding a check that names a configured value, a table factory, an options mapper, or a `ConfigOption`. Covers which of the three verdicts an entry takes, what a restatement has to look like to count, and which failures are a decision for the user.
---

# Curate a rejection's name

`scripts/check-option-message-names.py` holds [ADR-0127][adr]'s rule that **a rejection a user can
reach names what that user typed, in the vocabulary they typed it in**. The rule was written down
and then broken three more times — #1009/#1013, #1019, #1027 — because nothing enforced it and the
only detector was someone grepping the call sites by hand.

Most failures have one correct response and no judgment in them: **restate the check in the table
layer under the option key**. The verdicts exist because two shapes genuinely are not that, and
reaching for one when a restatement was called for is how the class comes back.

Run `just check-option-message-names` after touching a builder setter that rejects a configured
value, a table factory, an options mapper, or a `ConfigOption`. It is offline and takes under a
second.

[adr]: ../../../docs/adr/0127-a-configured-name-is-checked-for-what-this-project-will-do-with-it.md

## The config file, in one paragraph

`scripts/config/option-message-names.toml` is one `[[sites]]` entry per call to
`ResourceNames.checkComponent`, `ResourceNames.checkNotBlank` or `EmulatorEndpoint.parse` whose name
argument is a string literal. `module`, `class`, `member` and `literal` identify the call site;
`verdict` is one of three:

| Verdict | Says | Held against the sources by |
|---|---|---|
| `same` | the literal **is** the DDL key, so the message already names what a SQL caller typed | comparing them, and requiring the key in that module's `*ConnectorOptions.java` |
| `restated` | the table layer checks the value again under the key(s) in `keys` | looking for each key's `ConfigOption` at a `.key()`-named validator call under `**/table/**` |
| `unreachable` | no SQL caller can reach this check; `reason` is required | **nothing** — this is the one claim the script takes on the entry's word |

So `same` and `restated` are checked, and `unreachable` is argued. That asymmetry is the whole
shape of the file: prefer a verdict the sources settle, and write a reason only when neither of the
other two is true.

**An entry that never fires is itself a failure**, the rule `check-flink-api-tiers.toml` applies to
its allowlist. An entry is never a safe way to quieten something: if the check passes without it,
it has to go.

## Failure: "a check that names a setting, with no verdict in option-message-names.toml"

A new check that names a configured value. Decide which of the three it is, in this order — the
first that is true is the answer:

1. **Is the literal already the DDL key?** Compare it against the module's `*ConnectorOptions.java`.
   Every `*Destination.of` / `SpannerDatabase.of` component is here: `project`, `dataset`, `table`,
   `instance`, `location`, `queue`, `topic`, `subscription`, `database`. Verdict `same`, no other
   fields. Nothing to write in the sources.
2. **Can a SQL caller reach it?** If a `WITH` clause value can arrive at this setter, the setter
   keeps its literal — a DataStream caller wrote that setter, and there the name is right — and the
   **table layer restates the check under the DDL key**. Verdict `restated`, with `keys`. The next
   section is how to write the restatement.
3. **Only then, `unreachable`**, with a reason saying what was measured. The four entries today are
   `AvroSchemaOptions` and `ProtoSchemaOptions`' `"path"` literals, measured on #1027 by driving the
   factory from a `WITH` clause and finding that a blank element reaches no check at all.

**Do not guess at step 3.** "I do not think SQL reaches it" is not a measurement; drive the factory
from a `WITH` clause and see what is thrown. #1027 excluded two candidates that way and included
five that a reading pass had missed.

### Writing the restatement

The shape #1014, #1019 and #1027 settled, in the factory or in the `*OptionsMapper` that reads the
value:

```java
private static String queryResultDataset(ReadableConfig config) {
    return config.getOptional(BigQueryConnectorOptions.SOURCE_QUERY_RESULT_DATASET)
            .map(
                    value ->
                            ResourceNames.checkComponent(
                                    value,
                                    BigQueryConnectorOptions.SOURCE_QUERY_RESULT_DATASET.key()))
            .orElse(null);
}
```

Four things it has to get right, none of which this script checks:

- **The builder keeps its own check.** Both run; each names the caller it answers.
- **Place the call behind every check that refuses an option outright, never in front of one.** A
  DDL being told to remove an option is not helped by an answer about that option's shape. The ADR
  carries the ordering rule and the reason each connector's placement differs.
- **A value with two possible sources reports the key that supplied it.** `parentProject` falls back
  from `source.parent-project` to `project`, and answering the fallback under the first key names an
  option the DDL does not contain — which is precisely why the fallback ran. List both in `keys` and
  check under whichever arm supplied the value.
- **Add a test that asserts the message.** The script holds that *a* check exists under the key; only
  a test holds that the sentence a user reads is the right one.

## Failure: "classified `same`, but `x` is not a key … *ConnectorOptions.java declares"

The literal is not a DDL key, so `same` is false. Either the option was renamed and the literal
should follow, or the verdict is wrong — `restated` if the table layer checks it under its own key,
`unreachable` with a reason if no SQL caller reaches this check. This message is what a rename on
either side produces, and correcting the row is usually the whole fix.

## Failure: "classified `restated` under `k`, but nothing in `<module>`'s table package checks"

**This is the defect itself, not a config problem.** A SQL caller who wrote `k` is being answered
under the setter's name. Write the restatement, as above.

Reach for a different verdict only if the claim was wrong to begin with — the key does not in fact
reach this setter, or nothing from SQL does. Changing the verdict to make the message go away is
how #1019's three connectors stayed wrong after #1014 fixed the other two.

Its neighbour, "`k`, which `<module>`'s `*ConnectorOptions.java` does not declare", is the typo
case: the key in `keys` is misspelled, or the option was renamed.

## Failure: "`[[sites]]` entry … never fires"

The config classifies a call site that is not there. The check no longer exists, or its class,
member or literal was renamed. Delete the entry, or correct it to the call site's current identity —
a run that reports both halves of a rename together is the usual way this appears.

## Exit code 2 — infrastructure, not policy

These say the script cannot trust its own reading, so none of them is answered by an entry:

- **"names its setting with `…`, which is neither a string literal nor an expression reaching a
  `ConfigOption`'s key: `<reason>`."** A shape the script has not been taught. It already reads
  four: the key directly, a local the same method assigned from one (`String key = …REGIONS.key();`),
  a local bound by a for-each header, and a `ConfigOption` parameter. **Read the reason** — it
  separates "no loop binds this" from "two loops do" and from "the local is assigned twice", which
  want different repairs. If yours reaches a key by a fifth route, teach it — `ALIAS`, `KEY_ARGUMENT`
  or the for-header pattern, with a test. If it names something else entirely, that is a design
  question for the user, not a parser change.
- **"`<name>` names the setting of a check under one of its own parameters, and this file declares N
  methods with that name."** Calls to a helper are matched by name and arity, and two overloads share
  both — so the keys it checks would be whichever overload's arguments happened to match. Rename one
  of them. This one is a *source* condition with a source repair, unlike its neighbours below.
- **"sits in no method body this script recognises"** and **"has no closing parenthesis this script
  can find."** `METHOD` or the argument walk is under-matching, which makes *every* call site's
  reading untrustworthy, not only this one. Fix the parser; never exclude the file.
- **"matches `*ConnectorOptions.java` but declares no ConfigOption this script recognises."** Either
  it is not an options class, or `CONFIG_OPTION` no longer matches the declaration shape — and the
  second would make every key lookup wrong.
- **"two checks with one identity."** Two *declarations* — normally overloads — reject under the
  same literal, and an entry is keyed by (module, class, member, literal), so one verdict would
  answer both. Rename one. Two calls in a single body are deliberately not this: one setting, one
  verdict.
- The config-authoring errors — an unknown top-level table, a missing field, an unknown verdict,
  `restated` with no `keys`, `unreachable` with no `reason`, a field a verdict does not read, two
  entries for one call site. Each names its own remedy.

## What goes to the user

- **An `unreachable` verdict that has not been measured.** It is the only claim in the file the
  sources do not settle, and the measurement costs one `WITH` clause.
- **Deleting a `restated` entry's key, or the restatement it points at.** That retires the check
  for a value a SQL caller configures, which is the defect this exists to stop.
- **Widening `VALIDATORS`.** The population is the three shared validators on purpose; see the next
  section for what that leaves out and why.

## What this check does not do

[ADR-0127][adr] records the boundary and the measurements behind it. Four consequences are worth
knowing before you trust a green run:

- **It reads the three validators in `VALIDATORS`, not every rejection that names something.** A
  check written as a bare `Preconditions.checkArgument(…, "kmsKeyName must not be blank")` is
  invisible here — one of #1027's own six sites. The repair for that shape is to route it through
  `ResourceNames`, which ADR-0127 already asks for.
- **`same` compares against the module's whole key set**, not against the key that reaches this
  setter. A module that grows a second, differently-scoped key spelled like an existing one
  satisfies `same` wrongly, and only a test would catch it.
- **`restated` finds the check, not the path to it.** A restatement left behind but no longer
  reached still counts, so reverting a call without deleting the helper it called reads clean.
- **The population is call sites, not options.** A *new* `ConfigOption` routed into an
  already-classified setter does not fail here.
